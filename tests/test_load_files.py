import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from load_files import FILE_TYPES, archive_path, date_from_text, load_batch, parse_file, scrub_tar_debris

STATIONS = FILE_TYPES["stations"]
PRICES = FILE_TYPES["prices"]

PATH = "/Volumes/workspace/fuel_prices/raw/stations/anagrafica_impianti_attivi_2026-11-06.csv"
HEADER = (
    "idImpianto|Gestore|Bandiera|Tipo Impianto|Nome Impianto"
    "|Indirizzo|Comune|Provincia|Latitudine|Longitudine"
)
ROW = "59183|ENIMOOV S.P.A.|Agip Eni|Stradale|19829 AGRIGENTO|SS.189|AGRIGENTO|AG|37.33|13.59"


def parse(text, path=PATH, file_type=STATIONS):
    return parse_file(text.encode("utf-8"), path, file_type)


def as_dict(row, file_type=STATIONS):
    return dict(zip(["line_hash", *file_type.columns, "extraction_date", "source_file", "line_number"], row))


def test_parses_current_format():
    rows, rejects = parse(f"Estrazione del 2026-11-06\n{HEADER}\n{ROW}\n")
    assert rejects == []
    row = as_dict(rows[0])
    assert row["station_id"] == "59183"
    assert row["station_name"] == "19829 AGRIGENTO"
    assert row["operator_name"] == "ENIMOOV S.P.A."
    assert row["latitude"] == "37.33"
    assert row["extraction_date"] == date(2026, 11, 6)
    assert row["source_file"] == PATH
    assert row["line_number"] == 3


def test_parses_2015_format_with_date_from_filename():
    header = "Id impianto;Gestore;Bandiera;Tipo Impianto;Nome Impianto;Indirizzo;Comune;Provincia;Latitudine;Longitudine"
    rows, rejects = parse(
        f"{header}\n1;OP;BR;Stradale;NAME;ADDR;TOWN;RM;41.9;12.5\n",
        path="ftproot/osservaprezzi/copied/anagrafica_impianti_attivi-20150101.csv",
    )
    assert rejects == []
    assert as_dict(rows[0])["extraction_date"] == date(2015, 1, 1)
    assert as_dict(rows[0])["province_code"] == "RM"


def test_keeps_attribution_joined_to_its_field():
    rows, _ = parse(
        f"Estrazione del 2026-11-06\n{HEADER}\n"
        "54386|PRADELLI | gestori.prezzibenzina.it|Pompe Bianche|Stradale"
        "|PRADELLI | gestori.prezzibenzina.it|Via dei Martiri|ZOCCA|MO|44.3|11.0\n"
    )
    row = as_dict(rows[0])
    assert row["operator_name"] == "PRADELLI | gestori.prezzibenzina.it"
    assert row["station_name"] == "PRADELLI | gestori.prezzibenzina.it"
    assert row["address"] == "Via dei Martiri"


def test_unescapes_and_strips_quotes():
    rows, _ = parse(f'Estrazione del 2026-11-06\n{HEADER}\n1|"A &amp; B"|BR|Stradale|N|A|T|RM|1|2\n')
    assert as_dict(rows[0])["operator_name"] == "A & B"


def test_hashes_raw_line():
    rows, _ = parse(f"Estrazione del 2026-11-06\n{HEADER}\n{ROW}\n{ROW}\n")
    assert rows[0][0] == rows[1][0]
    assert len(rows[0][0]) == 32


def test_quarantines_bad_lines_and_keeps_good_ones():
    rows, rejects = parse(f"Estrazione del 2026-11-06\n{HEADER}\n1|too|few\nX|O|B|S|N|A|T|P|1|2\n{ROW}\n")
    assert len(rows) == 1
    assert [(r[1], r[3]) for r in rejects] == [
        (3, "Expected 10 fields, got 3"),
        (4, "Missing or non-numeric station_id"),
    ]


def test_quarantines_file_without_header():
    rows, rejects = parse("")
    assert rows == []
    assert rejects[0][3] == "No header row found"


def test_quarantines_file_without_date():
    rows, rejects = parse(f"{HEADER}\n{ROW}\n", path="/Volumes/raw/stations/undated.csv")
    assert rows == []
    assert rejects[0][3] == "No extraction date in the file or its path"


@pytest.mark.parametrize("text, expected", [
    ("Estrazione del 2026-11-06", date(2026, 11, 6)),
    ("Estrazione del 26-11-06", date(2026, 11, 6)),
    ("Estrazione del 06/11/2026", date(2026, 11, 6)),
    ("anagrafica_impianti_attivi-20150101.csv", date(2015, 1, 1)),
    ("no date here", None),
])
def test_date_from_text(text, expected):
    assert date_from_text(text) == expected


def test_scrub_tar_debris_removes_spliced_header_and_padding():
    before = b"a" * 512
    header = b"\x00" * 257 + b"ustar" + b"\x00" * 250
    member = b"m" * 512
    after = b"b" * 100 + b"\x00" * 412
    assert scrub_tar_debris(before + header + member + after) == b"a" * 512 + b"b" * 100


def test_scrub_tar_debris_leaves_clean_files_alone():
    assert scrub_tar_debris(b"clean") == b"clean"


PRICES_PATH = "/Volumes/workspace/fuel_prices/raw/prices/2026/2026-11-06/prezzo_alle_8.csv"
PRICES_HEADER = "idImpianto|descCarburante|prezzo|isSelf|dtComu"


def test_parses_prices():
    rows, rejects = parse(
        f"Estrazione del 2026-11-06\n{PRICES_HEADER}\n3464|Benzina|2.349|0|04/11/2026 19:30:08\n",
        path=PRICES_PATH, file_type=PRICES,
    )
    assert rejects == []
    row = as_dict(rows[0], PRICES)
    assert row["station_id"] == "3464"
    assert row["fuel_description"] == "Benzina"
    assert row["price"] == "2.349"
    assert row["self_service"] == "0"
    assert row["entry_date"] == "04/11/2026 19:30:08"
    assert row["extraction_date"] == date(2026, 11, 6)


def test_quarantines_price_row_with_one_extra_field():
    rows, rejects = parse(
        f"Estrazione del 2026-11-06\n{PRICES_HEADER}\n1|Benzina|2.349|0|04/11/2026 19:30:08|x\n",
        path=PRICES_PATH, file_type=PRICES,
    )
    assert rows == []
    assert rejects[0][3] == "Expected 5 fields, got 6"


def test_merges_station_address_overflow():
    rows, _ = parse(f"Estrazione del 2026-11-06\n{HEADER}\n1|O|B|S|N|VIA X|12|TOWN|RM|1|2\n")
    assert as_dict(rows[0])["address"] == "VIA X12"
    assert as_dict(rows[0])["comune"] == "TOWN"


LANDING = "/Volumes/workspace/fuel_prices/raw/landing/stations/"
ARCHIVE = "/Volumes/workspace/fuel_prices/raw/stations/"


@pytest.mark.parametrize("path", [
    "dbfs:/Volumes/workspace/fuel_prices/raw/landing/stations/anagrafica_impianti_attivi_2026-11-06.csv",
    "/Volumes/workspace/fuel_prices/raw/landing/stations/anagrafica_impianti_attivi_2026-11-06.csv",
])
def test_archive_path(path):
    assert archive_path(path, LANDING, ARCHIVE) == PATH


def test_archive_path_rejects_files_outside_landing():
    with pytest.raises(ValueError, match="isn't in the landing folder"):
        archive_path("/Volumes/workspace/fuel_prices/raw/stations/x.csv", LANDING, ARCHIVE)


class FakeFile:
    def __init__(self, path, content, modified):
        self.path, self.content, self.length, self.modificationTime = path, content, len(content), modified


class FakeFrame:
    def __init__(self, data, schema, writes):
        self.data, self.schema, self.writes, self.options = data, schema, writes, {}

    def withColumn(self, *args):
        return self

    @property
    def write(self):
        return self

    def mode(self, mode):
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def saveAsTable(self, table):
        self.writes[table] = self


class FakeBatch:
    def __init__(self, files):
        self.files, self.writes = files, {}
        self.sparkSession = self

    def select(self, *columns):
        return self

    def toLocalIterator(self):
        return iter(self.files)

    def createDataFrame(self, data, schema):
        return FakeFrame(data, schema, self.writes)


def test_load_batch_writes_bronze_quarantine_and_files_rows(monkeypatch):
    import types
    from datetime import datetime
    # Stand in for Spark's current_timestamp(), which needs a running Spark session.
    functions = types.ModuleType("pyspark.sql.functions")
    functions.current_timestamp = lambda: None
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    for name in ("pyspark", "pyspark.sql"):
        if name not in sys.modules:
            monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    content = f"Estrazione del 2026-11-06\n{HEADER}\n{ROW}\n1|too|few\n".encode()
    modified = datetime(2026, 11, 7, 9, 15)
    batch = FakeBatch([FakeFile(LANDING + "anagrafica_impianti_attivi_2026-11-06.csv", content, modified)])

    load_batch(batch, 3, "stations", LANDING, ARCHIVE, "bronze_stations", "quarantine_stations", "bronze_files", "/ckpt/a")

    assert [as_dict(r)["source_file"] for r in batch.writes["bronze_stations"].data] == [PATH]
    assert [r[0] for r in batch.writes["quarantine_stations"].data] == [PATH]
    (files_row,) = batch.writes["bronze_files"].data
    assert files_row[:4] == ("stations", PATH, len(content), modified)
    assert len(files_row[4]) == 32
    assert files_row[5:] == (date(2026, 11, 6), 1, 1)
    assert batch.writes["bronze_files"].options == {"txnAppId": "bronze_files:/ckpt/a", "txnVersion": 3}
    assert batch.writes["bronze_stations"].options == {"txnAppId": "bronze_stations:/ckpt/a", "txnVersion": 3}
