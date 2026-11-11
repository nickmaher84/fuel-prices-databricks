"""Load new MIMIT station registry or price files from a volume into a bronze table.

Auto Loader finds files in the landing folder that it hasn't processed before.
Each line becomes one row of string columns, keyed by a hash of the raw line.
Typing and cleaning happen downstream in dbt. Lines that can't be split into
columns go to a quarantine table instead of failing the load. Each file also gets
a row in a files table.

Processed files are moved from the landing folder to the archive once they're
older than the retention period. Rows record the archive path, so they stay valid
after the move.

The parsing rules are ported from the original fuel-prices backend
(app/scraper/base.py), which has handled the source files back to 2015.
"""

import logging
import re
import sys
from dataclasses import dataclass
from datetime import date
from hashlib import md5
from html import unescape

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileType:
    column_mapping: dict[str, str]
    # Station files can carry a stray delimiter in the address; merge the field
    # after it back in. None for files without that problem.
    overflow_index: int | None = None

    @property
    def columns(self) -> list[str]:
        return list(self.column_mapping.values())

    @property
    def bronze_schema(self) -> str:
        return (
            "line_hash STRING, "
            + ", ".join(f"{column} STRING" for column in self.columns)
            + ", extraction_date DATE, source_file STRING, line_number INT"
        )


FILE_TYPES = {
    "stations": FileType(
        column_mapping={
            "idimpianto": "station_id",
            "nomeimpianto": "station_name",
            "tipoimpianto": "station_type",
            "gestore": "operator_name",
            "bandiera": "brand_name",
            "indirizzo": "address",
            "comune": "comune",
            "provincia": "province_code",
            "latitudine": "latitude",
            "longitudine": "longitude",
        },
        overflow_index=5,
    ),
    "prices": FileType(
        column_mapping={
            "idimpianto": "station_id",
            "desccarburante": "fuel_description",
            "prezzo": "price",
            "isself": "self_service",
            "dtcomu": "entry_date",
        },
    ),
}

QUARANTINE_SCHEMA = (
    "source_file STRING, line_number INT, raw_line STRING, reason STRING"
)
FILES_SCHEMA = (
    "file_type STRING, source_file STRING, size_bytes BIGINT, uploaded_at TIMESTAMP, "
    "content_md5 STRING, extraction_date DATE, rows_loaded INT, rows_rejected INT"
)

# How long processed files stay in the landing folder before moving to the archive.
# Files only move during a run that has new files to process.
RETENTION = "7 days"

TAR_BLOCK = 512


def scrub_tar_debris(raw: bytes) -> bytes:
    """Strip slices of an uncompressed tar that disk corruption on the
    publisher's side has physically spliced into a source CSV. Exactly one file
    has ever been hit (2022-12-27), and only a file carrying NUL bytes can be
    affected, so this is a no-op for everything else.

    A tar header sits on a 512-byte boundary with the `ustar` magic at offset
    257 and is always followed by at least one record of member data. Drop every
    such header block and the block after it, then strip the NUL padding the
    splice left behind. Rows torn across a dropped boundary are left broken and
    end up in quarantine."""
    if b"\x00" not in raw:
        return raw

    kept, drop_next = [], False
    for start in range(0, len(raw), TAR_BLOCK):
        block = raw[start:start + TAR_BLOCK]
        if drop_next:
            drop_next = False
            continue
        if block[257:262] == b"ustar":
            drop_next = True
            continue
        kept.append(block)

    scrubbed = b"".join(kept).replace(b"\x00", b"")
    logger.warning(
        f"Scrubbed {len(raw) - len(scrubbed)} bytes of tar debris / NUL "
        f"padding from a corrupt source file"
    )
    return scrubbed


def preprocess_line(values: list[str], delimiter: str, column_count: int, overflow_index: int | None) -> list[str]:
    values = [value.strip().strip('"') for value in values]

    for i in ["gestori.prezzibenzina.it", "BENZINA.IT", ""]:
        while i in values and len(values) > column_count:
            n = values.index(i)
            values[n-1] += f" {delimiter} " + values[n]
            del values[n]

    if overflow_index is not None and len(values) > column_count:
        values[overflow_index] += values[overflow_index + 1]
        del values[overflow_index + 1]

    return values


def normalise_column(name: str) -> str:
    return name.strip().strip('"').replace(" ", "").lower()


def date_from_text(text: str) -> date | None:
    """Find a date written as YYYY-MM-DD, YY-MM-DD, DD/MM/YYYY or YYYYMMDD."""
    patterns = (
        (r"(\d{4})-(\d{2})-(\d{2})", "ymd"),
        (r"(\d{2})/(\d{2})/(\d{4})", "dmy"),
        (r"(?<!\d)(\d{2})-(\d{2})-(\d{2})(?!\d)", "ymd2"),
        (r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)", "ymd"),
    )
    for pattern, order in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        a, b, c = (int(part) for part in match.groups())
        try:
            if order == "dmy":
                return date(c, b, a)
            if order == "ymd2":
                return date(2000 + a, b, c)
            return date(a, b, c)
        except ValueError:
            continue
    return None


def parse_file(raw: bytes, source_file: str, file_type: FileType):
    """Return (bronze rows, quarantine rows) for one source file."""
    rejects = []

    def reject(line_number, line, reason):
        rejects.append((source_file, line_number, line, reason))

    try:
        text = scrub_tar_debris(raw).decode("utf-8")
    except UnicodeDecodeError as error:
        reject(None, None, f"Not UTF-8: {error}")
        return [], rejects

    extraction_date = None
    header = None
    delimiter = ";"
    columns = []
    rows = []

    for line_number, line in enumerate(text.split("\n"), 1):
        if line.startswith("Estrazione del"):
            extraction_date = date_from_text(line)

        elif line.strip() and header is None:
            header = line
            delimiter = "|" if header.count("|") > header.count(";") else ";"
            names = header.split(delimiter)
            columns = [file_type.column_mapping.get(normalise_column(name)) for name in names]
            unknown = [name for name, column in zip(names, columns) if column is None]
            if unknown:
                logger.warning(f"Ignoring unknown columns {unknown} in {source_file}")

        elif line.strip():
            fields = preprocess_line(
                unescape(line).split(delimiter), delimiter, len(columns), file_type.overflow_index
            )
            if len(fields) != len(columns):
                reject(line_number, line, f"Expected {len(columns)} fields, got {len(fields)}")
                continue

            record = {column: field for column, field in zip(columns, fields) if column}
            if not (record.get("station_id") or "").isdigit():
                reject(line_number, line, "Missing or non-numeric station_id")
                continue

            line_hash = md5(line.encode("utf-8")).hexdigest()
            rows.append((line_hash, *(record.get(column) for column in file_type.columns), line_number))

    if header is None:
        reject(None, None, "No header row found")
        return [], rejects

    extraction_date = extraction_date or date_from_text(source_file)
    if extraction_date is None:
        reject(None, None, "No extraction date in the file or its path")
        return [], rejects

    rows = [(*row[:-1], extraction_date, source_file, row[-1]) for row in rows]
    return rows, rejects


def strip_scheme(path: str) -> str:
    return path.removeprefix("dbfs:")


def archive_path(path: str, landing_path: str, archive: str) -> str:
    """Where a file in the landing folder ends up once it's moved to the archive."""
    path, landing_path = strip_scheme(path), strip_scheme(landing_path).rstrip("/") + "/"
    if not path.startswith(landing_path):
        raise ValueError(f"{path} isn't in the landing folder {landing_path}")
    return strip_scheme(archive).rstrip("/") + "/" + path.removeprefix(landing_path)


def load_batch(batch, batch_id, file_type_name, landing_path, archive, bronze_table, quarantine_table, files_table,
               checkpoint_path):
    file_type = FILE_TYPES[file_type_name]
    rows, rejects, files = [], [], []
    for file in batch.select("path", "modificationTime", "length", "content").toLocalIterator():
        raw = bytes(file.content)
        source_file = archive_path(file.path, landing_path, archive)
        file_rows, file_rejects = parse_file(raw, source_file, file_type)
        rows += file_rows
        rejects += file_rejects
        extraction_date = file_rows[0][len(file_type.columns) + 1] if file_rows else None
        files.append((
            file_type_name, source_file, file.length, file.modificationTime, md5(raw).hexdigest(),
            extraction_date, len(file_rows), len(file_rejects),
        ))
        print(f"{source_file}: {len(file_rows)} rows, {len(file_rejects)} rejected")

    from pyspark.sql.functions import current_timestamp

    spark = batch.sparkSession
    for table, data, schema in (
        (bronze_table, rows, file_type.bronze_schema),
        (quarantine_table, rejects, QUARANTINE_SCHEMA),
        (files_table, files, FILES_SCHEMA),
    ):
        # txnAppId/txnVersion make a retried batch a no-op rather than a duplicate.
        # Batch ids restart when a checkpoint is reset, so the app id includes the
        # checkpoint path; otherwise Delta would skip the new batches as already written.
        app_id = f"{table}:{checkpoint_path}"
        (
            spark.createDataFrame(data, schema)
            .withColumn("loaded_at", current_timestamp())
            .write.mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", batch_id)
            .saveAsTable(table)
        )


def main(file_type_name, landing_path, archive, bronze_table, quarantine_table, files_table, checkpoint_path):
    from pyspark.sql import SparkSession

    FILE_TYPES[file_type_name]  # Fail fast on an unknown file type.
    spark = SparkSession.builder.getOrCreate()

    # Both load tasks write to the files table and may start at the same moment, so
    # create it up front and tolerate the other task getting there first.
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {files_table} ({FILES_SCHEMA}, loaded_at TIMESTAMP)")
    except Exception as error:
        if "TABLE_OR_VIEW_ALREADY_EXISTS" not in str(error):
            raise

    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.csv")
        .option("cloudFiles.maxFilesPerTrigger", 10)
        .option("cloudFiles.cleanSource", "MOVE")
        .option("cloudFiles.cleanSource.moveDestination", archive)
        .option("cloudFiles.cleanSource.retentionDuration", RETENTION)
        .option("cloudFiles.cleanSource.waitForCompletion", "true")
        .load(landing_path)
        .writeStream.foreachBatch(
            lambda batch, batch_id: load_batch(
                batch, batch_id, file_type_name, landing_path, archive,
                bronze_table, quarantine_table, files_table, checkpoint_path,
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main(*sys.argv[1:8])
