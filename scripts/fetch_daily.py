"""Download MIMIT station registry and price files and upload them to the landing folder.

By default it fetches today's files. With --quarter, it fetches every daily file
from the quarterly archives instead, to fill gaps or load history.

Uses only the Python standard library, so it runs anywhere Python 3.9+ does,
including Pythonista on iOS. The source only accepts connections from Italian
addresses, so it has to run from an Italian connection.

Each file is uploaded as <name>_<date>.csv, using the date on its 'Estrazione del'
first line, or the date in its archive filename for older files without one. Files
already in the landing folder or the archive are skipped, so running it more than
once is harmless. The Databricks job starts on its own when new files arrive.

    DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... python fetch_daily.py [--dry-run]
    python fetch_daily.py --profile fuel-prices    # host and token from ~/.databrickscfg
    DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... python fetch_daily.py --quarter 2022Q4 2023Q1
    DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... python fetch_daily.py --quarter 2026Q3 --type prices
"""

import argparse
import configparser
import http.client
import os
import re
import sys
import tarfile
import tempfile
import time
from datetime import date, datetime, timedelta
import urllib.error
import urllib.parse
import urllib.request

SOURCE_URL = "https://www.mimit.gov.it/images/exportCSV/{name}.csv"
ARCHIVE_URL = "https://opendatacarburanti.mise.gov.it/categorized/{name}/{year}/{year}_{quarter}_tr.tar.gz"
FILES = {
    "stations": "anagrafica_impianti_attivi",
    "prices": "prezzo_alle_8",
}
DEFAULT_VOLUME = "/Volumes/workspace/fuel_prices/raw"
# Partly downloaded archives are kept here, so an interrupted run can resume.
ARCHIVE_DIR = os.path.join(tempfile.gettempdir(), "fuel-prices-archives")
CHUNK = 1024 * 1024


def log(message, error=False):
    """Print a line with the local time, straight away rather than buffered."""
    print(f"{datetime.now():%H:%M:%S} {message}", file=sys.stderr if error else sys.stdout, flush=True)


def download(url, attempts=3, timeout=60):
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.read()
        except (urllib.error.URLError, OSError) as error:
            if attempt == attempts:
                raise
            log(f"download failed ({error}), retrying")
            time.sleep(5 * attempt)


def extraction_date(content, file_name=None):
    """The date on the file's 'Estrazione del YYYY-MM-DD' first line, or failing
    that, the YYYYMMDD date in its archive filename."""
    first_line = content.split(b"\n", 1)[0].decode("utf-8", "replace")
    match = re.search(r"\d{4}-\d{2}-\d{2}", first_line)
    if match:
        return match.group(0)
    match = re.search(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)", file_name or "")
    if match:
        return "-".join(match.groups())
    raise ValueError(f"No extraction date on the first line: {first_line[:80]!r}")


def open_url(request):
    return urllib.request.urlopen(request, timeout=60)


def download_archive(url, path, attempts=20):
    """Download url to path, resuming with Range requests when the connection drops
    or stalls. The archive server is slow and often cuts long downloads short. A
    partial file from an earlier run is resumed too, as long as the archive's ETag
    hasn't changed."""
    etag_path = path + ".etag"
    etag = open(etag_path).read() if os.path.exists(etag_path) else None
    if etag is None and os.path.exists(path):
        os.remove(path)  # Can't tell whether a partial file is still current.
    failures, total, next_log = 0, None, 0.1

    while True:
        have = os.path.getsize(path) if os.path.exists(path) else 0
        request = urllib.request.Request(url)
        if have and etag:
            request.add_header("Range", f"bytes={have}-")
            request.add_header("If-Range", etag)
        try:
            with open_url(request) as response:
                if response.status == 206:
                    total = int(response.headers["Content-Range"].rsplit("/", 1)[1])
                    mode = "ab"
                    log(f"resuming download at {have / CHUNK:.0f} of {total / CHUNK:.0f} MB")
                else:
                    # A full response: a fresh download, or the archive has changed.
                    total = int(response.headers["Content-Length"])
                    etag, have, mode = response.headers.get("ETag"), 0, "wb"
                    with open(etag_path, "w") as f:
                        f.write(etag or "")
                with open(path, mode) as f:
                    while True:
                        chunk = response.read(CHUNK)
                        if not chunk:
                            break
                        f.write(chunk)
                        have += len(chunk)
                        if have / total >= next_log:
                            log(f"downloaded {have / CHUNK:.0f} of {total / CHUNK:.0f} MB")
                            next_log = (int(have / total * 10) + 1) / 10
            if have >= total:
                return path
            raise http.client.IncompleteRead(b"", total - have)
        except urllib.error.HTTPError as error:
            if error.code == 416 and have:  # Already complete from an earlier run.
                return path
            if error.code < 500:
                raise
            failures += 1
            if failures > attempts:
                raise
            log(f"download interrupted ({error}), resuming")
            time.sleep(min(60, 5 * failures))
        except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
            failures += 1
            if failures > attempts:
                raise
            log(f"download interrupted ({error}), resuming")
            time.sleep(min(60, 5 * failures))


def archive_members(url):
    """Yield (name, content) for each file in a quarterly archive. The archive is
    downloaded first, and deleted once every file has been read."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    name, year, file_name = url.rsplit("/", 3)[1:]
    path = download_archive(url, os.path.join(ARCHIVE_DIR, f"{name}_{year}_{file_name}"))
    with tarfile.open(path, mode="r:gz") as archive:
        for member in archive:
            if member.isfile() and member.size:
                yield member.name, archive.extractfile(member).read()
    for leftover in (path, path + ".etag"):
        os.remove(leftover)


class Databricks:
    """The two Files API calls this script needs."""

    def __init__(self, host, token):
        if not host.startswith("https://"):
            host = "https://" + host
        self.host = host.rstrip("/")
        self.token = token

    def _request(self, method, path, data=None, query=""):
        url = f"{self.host}/api/2.0/fs/files{urllib.parse.quote(path)}{query}"
        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Authorization", f"Bearer {self.token}")
        if data is not None:
            request.add_header("Content-Type", "application/octet-stream")
        return urllib.request.urlopen(request, timeout=120)

    def exists(self, path):
        try:
            with self._request("HEAD", path):
                return True
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return False
            raise

    def upload(self, path, content):
        with self._request("PUT", path, data=content, query="?overwrite=false"):
            pass


def fetch(databricks, volume, kind, name, dry_run=False):
    """Fetch today's file. Returns 'uploaded', 'stored' or 'dry run'."""
    content = download(SOURCE_URL.format(name=name))
    return store(databricks, volume, kind, f"{name}_{extraction_date(content)}.csv", content, dry_run)


def quarter_days(year, quarter):
    first = date(year, 3 * quarter - 2, 1)
    after = date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)
    return [first + timedelta(days=n) for n in range((after - first).days)]


def fetch_quarter(databricks, volume, kind, name, year, quarter, dry_run=False):
    """Fetch every daily file in one quarterly archive, then report any days the
    archive doesn't have. Returns a count of each outcome."""
    log(f"{kind} {year}Q{quarter}: downloading archive")
    started = time.monotonic()
    counts, seen = {}, []
    for member_name, content in archive_members(ARCHIVE_URL.format(name=name, year=year, quarter=quarter)):
        day = extraction_date(content, member_name)
        seen.append(day)
        outcome = store(databricks, volume, kind, f"{name}_{day}.csv", content, dry_run)
        counts[outcome] = counts.get(outcome, 0) + 1

    summary = ", ".join(f"{count} {outcome}" for outcome, count in sorted(counts.items()))
    minutes, seconds = divmod(round(time.monotonic() - started), 60)
    log(f"{kind} {year}Q{quarter}: {summary or 'no files'} in {minutes}m {seconds:02d}s")
    missing = [str(day) for day in quarter_days(year, quarter) if str(day) not in seen]
    if missing:
        log(f"{kind} {year}Q{quarter}: no file for {len(missing)} day(s): {', '.join(missing)}")
    repeated = sorted({day for day in seen if seen.count(day) > 1})
    if repeated:
        log(f"{kind} {year}Q{quarter}: more than one file for {', '.join(repeated)}")
    return counts


def store(databricks, volume, kind, file_name, content, dry_run=False):
    """Upload one file to the landing folder unless it's already stored.
    Returns 'uploaded', 'stored' or 'dry run'."""
    landing = f"{volume}/landing/{kind}/{file_name}"
    archive = f"{volume}/{kind}/{file_name}"

    # Auto Loader only reads a file once, so keep whichever copy was loaded.
    if databricks.exists(landing) or databricks.exists(archive):
        outcome = "stored"
    elif dry_run:
        outcome = "dry run"
    else:
        databricks.upload(landing, content)
        outcome = "uploaded"
    log(f"{kind}: {file_name} ({len(content)} bytes): {outcome}")
    return outcome


def credentials(profile=None):
    """The workspace host and token, from a ~/.databrickscfg profile if one is given,
    otherwise from DATABRICKS_HOST and DATABRICKS_TOKEN."""
    if profile is None:
        return os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
    path = os.environ.get("DATABRICKS_CONFIG_FILE") or os.path.expanduser("~/.databrickscfg")
    config = configparser.ConfigParser()
    if not config.read(path) or not config.has_section(profile):
        raise ValueError(f"no profile {profile!r} in {path}")
    return config.get(profile, "host", fallback=None), config.get(profile, "token", fallback=None)


def quarter(value):
    match = re.fullmatch(r"(\d{4})Q([1-4])", value.upper())
    if not match:
        raise argparse.ArgumentTypeError(f"expected a quarter like 2022Q4, got {value!r}")
    return int(match.group(1)), int(match.group(2))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--volume", default=DEFAULT_VOLUME, help=f"raw volume path (default {DEFAULT_VOLUME})")
    parser.add_argument("--profile", help="read the host and token from this ~/.databrickscfg profile")
    parser.add_argument("--dry-run", action="store_true", help="download and check, but don't upload")
    parser.add_argument("--quarter", nargs="+", type=quarter, metavar="YYYYQN",
                        help="fetch these quarterly archives instead of today's files")
    parser.add_argument("--type", nargs="+", choices=list(FILES), default=list(FILES), dest="types",
                        help="only fetch these file types (default: both)")
    args = parser.parse_args(argv)

    try:
        host, token = credentials(args.profile)
    except ValueError as error:
        parser.error(str(error))
    if not host or not token:
        parser.error(f"profile {args.profile!r} needs a host and token" if args.profile
                     else "set DATABRICKS_HOST and DATABRICKS_TOKEN, or use --profile")
    databricks = Databricks(host, token)

    volume = args.volume.rstrip("/")
    failed = False
    for year, number in args.quarter or [(None, None)]:
        for kind in args.types:
            name = FILES[kind]
            try:
                if year is None:
                    fetch(databricks, volume, kind, name, args.dry_run)
                else:
                    fetch_quarter(databricks, volume, kind, name, year, number, args.dry_run)
            except Exception as error:
                # Keep going, so one failure doesn't stop the rest.
                label = kind if year is None else f"{kind} {year}Q{number}"
                log(f"{label}: failed: {error}", error=True)
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
