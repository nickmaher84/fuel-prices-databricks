import sys
from pathlib import Path

import urllib.error

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import fetch_daily
from fetch_daily import Databricks, credentials, extraction_date, fetch, fetch_quarter, quarter, quarter_days

VOLUME = "/Volumes/workspace/fuel_prices/raw"
CONTENT = b"Estrazione del 2026-11-10\nidImpianto|descCarburante|prezzo|isSelf|dtComu\n1|Benzina|1.9|1|x\n"


class FakeDatabricks:
    def __init__(self, stored=()):
        self.stored, self.uploads = set(stored), {}

    def exists(self, path):
        return path in self.stored

    def upload(self, path, content):
        self.uploads[path] = content


@pytest.fixture(autouse=True)
def fake_download(monkeypatch):
    monkeypatch.setattr(fetch_daily, "download", lambda url: CONTENT)


def test_extraction_date():
    assert extraction_date(CONTENT) == "2026-11-10"


def test_extraction_date_missing():
    with pytest.raises(ValueError, match="No extraction date"):
        extraction_date(b"<html>Service unavailable</html>")


def test_uploads_new_file_to_landing_under_its_extraction_date():
    databricks = FakeDatabricks()
    assert fetch(databricks, VOLUME, "prices", "prezzo_alle_8") == "uploaded"
    assert databricks.uploads == {f"{VOLUME}/landing/prices/prezzo_alle_8_2026-11-10.csv": CONTENT}


@pytest.mark.parametrize("stored", [
    f"{VOLUME}/landing/prices/prezzo_alle_8_2026-11-10.csv",
    f"{VOLUME}/prices/prezzo_alle_8_2026-11-10.csv",
])
def test_skips_file_already_in_landing_or_archive(stored):
    databricks = FakeDatabricks(stored=[stored])
    assert fetch(databricks, VOLUME, "prices", "prezzo_alle_8") == "stored"
    assert databricks.uploads == {}


def test_dry_run_uploads_nothing():
    databricks = FakeDatabricks()
    assert fetch(databricks, VOLUME, "prices", "prezzo_alle_8", dry_run=True) == "dry run"
    assert databricks.uploads == {}


def test_main_carries_on_after_one_file_fails(monkeypatch, capsys):
    calls = []

    def fake_fetch(databricks, volume, kind, name, dry_run):
        calls.append(kind)
        if kind == "stations":
            raise OSError("connection timed out")
        return "uploaded"

    monkeypatch.setattr(fetch_daily, "fetch", fake_fetch)
    monkeypatch.setenv("DATABRICKS_HOST", "example.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    assert fetch_daily.main([]) == 1
    assert calls == ["stations", "prices"]
    assert "stations: failed: connection timed out" in capsys.readouterr().err


def test_main_requires_credentials(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        fetch_daily.main([])


def test_databricks_adds_https_and_quotes_paths():
    databricks = Databricks("example.invalid/", "token")
    assert databricks.host == "https://example.invalid"


class FakeResponse:
    """An HTTP response that can drop the connection after cut_after bytes."""

    def __init__(self, data, status=200, headers=None, cut_after=None):
        self.data, self.status, self.headers, self.cut_after = data, status, headers or {}, cut_after
        self.sent = 0

    def read(self, size):
        if self.cut_after is not None and self.sent >= self.cut_after:
            raise TimeoutError("The read operation timed out")
        end = len(self.data) if self.cut_after is None else min(len(self.data), self.cut_after)
        chunk = self.data[self.sent:min(end, self.sent + size)]
        self.sent += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeServer:
    """Serves one file, honouring Range and If-Range, and dropping the first
    `drops` connections partway through."""

    def __init__(self, data, etag='"v1"', drops=0):
        self.data, self.etag, self.drops, self.requests = data, etag, drops, []

    def __call__(self, request):
        headers = {k.lower(): v for k, v in request.header_items()}
        self.requests.append(headers)
        start = 0
        if "range" in headers and headers.get("if-range") == self.etag:
            start = int(headers["range"].split("=")[1].rstrip("-"))
            if start >= len(self.data):
                raise urllib.error.HTTPError(request.full_url, 416, "Range Not Satisfiable", {}, None)
        body = self.data[start:]
        cut = len(body) // 2 if self.drops > 0 else None
        self.drops -= 1
        if start:
            return FakeResponse(body, 206, {"Content-Range": f"bytes {start}-{len(self.data) - 1}/{len(self.data)}"}, cut)
        return FakeResponse(body, 200, {"Content-Length": str(len(body)), "ETag": self.etag}, cut)


@pytest.fixture(autouse=True)
def archive_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_daily, "ARCHIVE_DIR", str(tmp_path / "archives"))
    monkeypatch.setattr(fetch_daily.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(fetch_daily, "CHUNK", 64)
    return tmp_path / "archives"


def serve(monkeypatch, data, **kwargs):
    server = FakeServer(data, **kwargs)
    monkeypatch.setattr(fetch_daily, "open_url", server)
    return server


def make_archive(members):
    """A .tar.gz in memory, from (name, content) pairs; content None makes a directory."""
    import io, tarfile
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, content in members:
            info = tarfile.TarInfo(name)
            if content is None:
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
            else:
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def test_extraction_date_falls_back_to_archive_filename():
    header_only = b"idImpianto;descCarburante;prezzo;isSelf;dtComu\n1;Benzina;1.5;1;x\n"
    name = "ftproot/osservaprezzi/copied/prezzo_alle_8-20150108.csv"
    assert extraction_date(header_only, name) == "2015-01-08"
    with pytest.raises(ValueError):
        extraction_date(header_only, "no-date.csv")


def test_fetch_quarter_uploads_each_day_and_skips_what_is_stored(monkeypatch, capsys):
    old_style = b"idImpianto;descCarburante;prezzo;isSelf;dtComu\n1;Benzina;1.5;1;x\n"
    archive = make_archive([
        ("./2026_3_tr", None),
        ("./2026_3_tr/prezzo_alle_8-20260926.csv", b"Estrazione del 2026-09-26\nh\n1\n"),
        ("./2026_3_tr/prezzo_alle_8-20260927.csv", b"Estrazione del 2026-09-27\nh\n1\n"),
        ("ftproot/osservaprezzi/copied/prezzo_alle_8-20260928.csv", old_style),
        ("./2026_3_tr/prezzo_alle_8-20260929.csv", b""),
    ])
    server = serve(monkeypatch, archive)
    databricks = FakeDatabricks(stored=[f"{VOLUME}/prices/prezzo_alle_8_2026-09-27.csv"])

    counts = fetch_quarter(databricks, VOLUME, "prices", "prezzo_alle_8", 2026, 3)

    assert len(server.requests) == 1
    assert counts == {"uploaded": 2, "stored": 1}
    assert sorted(databricks.uploads) == [
        f"{VOLUME}/landing/prices/prezzo_alle_8_2026-09-26.csv",
        f"{VOLUME}/landing/prices/prezzo_alle_8_2026-09-28.csv",
    ]
    assert databricks.uploads[f"{VOLUME}/landing/prices/prezzo_alle_8_2026-09-28.csv"] == old_style
    output = capsys.readouterr().out
    assert "prices: prezzo_alle_8_2026-09-26.csv" in output
    assert "no file for 89 day(s): 2026-07-01, " in output
    assert "more than one file" not in output


@pytest.mark.parametrize("value, expected", [("2022Q4", (2022, 4)), ("2015q1", (2015, 1))])
def test_quarter_argument(value, expected):
    assert quarter(value) == expected


@pytest.mark.parametrize("value", ["2022Q5", "2022", "Q4-2022"])
def test_quarter_argument_rejects_bad_values(value):
    import argparse
    with pytest.raises(argparse.ArgumentTypeError):
        quarter(value)


def test_main_runs_each_quarter_for_both_file_types(monkeypatch):
    calls = []
    monkeypatch.setattr(fetch_daily, "fetch_quarter",
                        lambda databricks, volume, kind, name, year, number, dry_run: calls.append((year, number, kind)))
    monkeypatch.setattr(fetch_daily, "fetch", lambda *args: pytest.fail("daily fetch shouldn't run"))
    monkeypatch.setenv("DATABRICKS_HOST", "example.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    assert fetch_daily.main(["--quarter", "2022Q4", "2023Q1"]) == 0
    assert calls == [(2022, 4, "stations"), (2022, 4, "prices"), (2023, 1, "stations"), (2023, 1, "prices")]


@pytest.mark.parametrize("year, number, days, first, last", [
    (2026, 1, 90, "2026-01-01", "2026-03-31"),
    (2024, 1, 91, "2024-01-01", "2024-03-31"),
    (2026, 3, 92, "2026-07-01", "2026-09-30"),
    (2026, 4, 92, "2026-10-01", "2026-12-31"),
])
def test_quarter_days(year, number, days, first, last):
    result = quarter_days(year, number)
    assert (len(result), str(result[0]), str(result[-1])) == (days, first, last)


def test_fetch_quarter_reports_repeated_days(monkeypatch, capsys):
    archive = make_archive([
        ("a/prezzo_alle_8-20260926.csv", b"Estrazione del 2026-09-26\nh\n1\n"),
        ("b/prezzo_alle_8-20260926.csv", b"Estrazione del 2026-09-26\nh\n2\n"),
    ])
    serve(monkeypatch, archive)
    fetch_quarter(FakeDatabricks(), VOLUME, "prices", "prezzo_alle_8", 2026, 3, dry_run=True)
    assert "more than one file for 2026-09-26" in capsys.readouterr().out


def test_main_fetches_only_the_requested_types(monkeypatch):
    calls = []
    monkeypatch.setattr(fetch_daily, "fetch_quarter",
                        lambda databricks, volume, kind, name, year, number, dry_run: calls.append((kind, name)))
    monkeypatch.setenv("DATABRICKS_HOST", "example.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    assert fetch_daily.main(["--quarter", "2026Q3", "--type", "prices"]) == 0
    assert calls == [("prices", "prezzo_alle_8")]


def test_type_rejects_unknown_values(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "example.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    with pytest.raises(SystemExit):
        fetch_daily.main(["--type", "diesel"])


def test_log_lines_start_with_the_time(capsys):
    import re
    fetch_daily.log("hello")
    assert re.fullmatch(r"\d{2}:\d{2}:\d{2} hello\n", capsys.readouterr().out)


def test_fetch_quarter_reports_how_long_it_took(monkeypatch, capsys):
    archive = make_archive([("a/prezzo_alle_8-20260926.csv", b"Estrazione del 2026-09-26\nh\n1\n")])
    serve(monkeypatch, archive)
    fetch_quarter(FakeDatabricks(), VOLUME, "prices", "prezzo_alle_8", 2026, 3, dry_run=True)
    assert "prices 2026Q3: 1 dry run in 0m 00s" in capsys.readouterr().out


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    path = tmp_path / ".databrickscfg"
    path.write_text("[fuel-prices]\nhost = https://profile.invalid\ntoken = profile-token\n\n[empty]\nhost = https://x.invalid\n")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(path))
    return path


def test_credentials_from_profile(config_file, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "env.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
    assert credentials("fuel-prices") == ("https://profile.invalid", "profile-token")


def test_credentials_from_environment_without_profile(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "env.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
    assert credentials() == ("env.invalid", "env-token")


def test_credentials_unknown_profile(config_file):
    with pytest.raises(ValueError, match="no profile 'other'"):
        credentials("other")


def test_main_rejects_profile_without_token(config_file, capsys):
    with pytest.raises(SystemExit):
        fetch_daily.main(["--profile", "empty"])
    assert "profile 'empty' needs a host and token" in capsys.readouterr().err


def test_main_uses_profile(config_file, monkeypatch):
    seen = []
    monkeypatch.setattr(fetch_daily, "fetch", lambda databricks, *args: seen.append((databricks.host, databricks.token)))
    assert fetch_daily.main(["--profile", "fuel-prices", "--type", "prices"]) == 0
    assert seen == [("https://profile.invalid", "profile-token")]


def test_download_archive_resumes_after_dropped_connections(monkeypatch, tmp_path, capsys):
    data = bytes(range(256)) * 40
    server = serve(monkeypatch, data, drops=2)
    path = str(tmp_path / "a.tar.gz")
    assert fetch_daily.download_archive("https://example.invalid/a.tar.gz", path) == path
    assert open(path, "rb").read() == data
    assert [r.get("range") for r in server.requests] == [None, "bytes=5120-", "bytes=7680-"]
    assert all(r.get("if-range") == '"v1"' for r in server.requests[1:])
    output = capsys.readouterr().out
    assert output.count("download interrupted") == 2
    assert "resuming download at" in output


def test_download_archive_resumes_a_partial_file_from_an_earlier_run(monkeypatch, tmp_path):
    data = b"x" * 1000
    path = tmp_path / "a.tar.gz"
    path.write_bytes(data[:300])
    (tmp_path / "a.tar.gz.etag").write_text('"v1"')
    server = serve(monkeypatch, data)
    fetch_daily.download_archive("https://example.invalid/a.tar.gz", str(path))
    assert path.read_bytes() == data
    assert server.requests[0]["range"] == "bytes=300-"


def test_download_archive_restarts_if_the_archive_changed(monkeypatch, tmp_path):
    path = tmp_path / "a.tar.gz"
    path.write_bytes(b"old" * 100)
    (tmp_path / "a.tar.gz.etag").write_text('"v0"')
    serve(monkeypatch, b"new" * 50, etag='"v1"')
    fetch_daily.download_archive("https://example.invalid/a.tar.gz", str(path))
    assert path.read_bytes() == b"new" * 50
    assert (tmp_path / "a.tar.gz.etag").read_text() == '"v1"'


def test_download_archive_accepts_an_already_complete_file(monkeypatch, tmp_path):
    data = b"y" * 500
    path = tmp_path / "a.tar.gz"
    path.write_bytes(data)
    (tmp_path / "a.tar.gz.etag").write_text('"v1"')
    serve(monkeypatch, data)
    assert fetch_daily.download_archive("https://example.invalid/a.tar.gz", str(path)) == str(path)


def test_download_archive_gives_up_eventually(monkeypatch, tmp_path):
    serve(monkeypatch, b"z" * 1000, drops=100)
    with pytest.raises(TimeoutError):
        fetch_daily.download_archive("https://example.invalid/a.tar.gz", str(tmp_path / "a.tar.gz"), attempts=3)


def test_archive_is_deleted_once_processed(monkeypatch, archive_dir):
    serve(monkeypatch, make_archive([("a/prezzo_alle_8-20260926.csv", b"Estrazione del 2026-09-26\nh\n1\n")]))
    fetch_quarter(FakeDatabricks(), VOLUME, "prices", "prezzo_alle_8", 2026, 3)
    assert list(archive_dir.iterdir()) == []


def test_archive_is_kept_if_processing_fails(monkeypatch, archive_dir):
    serve(monkeypatch, make_archive([("a/prezzo_alle_8-20260926.csv", b"Estrazione del 2026-09-26\nh\n1\n")]))

    class FailingDatabricks(FakeDatabricks):
        def upload(self, path, content):
            raise OSError("upload failed")

    with pytest.raises(OSError):
        fetch_quarter(FailingDatabricks(), VOLUME, "prices", "prezzo_alle_8", 2026, 3)
    assert sorted(p.name for p in archive_dir.iterdir()) == ["prezzo_alle_8_2026_2026_3_tr.tar.gz", "prezzo_alle_8_2026_2026_3_tr.tar.gz.etag"]
