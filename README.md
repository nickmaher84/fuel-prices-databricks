# fuel-prices-databricks

Loads Italy's daily fuel
[station registry](https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv)
and [prices](https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv) from mimit.gov.it
into Databricks, deployed with a Declarative Automation Bundle.

## How it works

```
fetch script ──▶ landing folder ──▶ bronze tables
(you run it)     (job starts on      (Auto Loader)
                  new files)
```

1. `scripts/fetch_daily.py` downloads both daily files and uploads them to the
   landing folder of the `raw` volume. It has to run from an Italian connection
   (see [Fetching the files](#fetching-the-files)).
2. New files in the landing folder start the `fuel-prices-daily` job, through a file
   arrival trigger.
3. `scripts/load_files.py` uses Auto Loader to find files it hasn't processed before.
   Each line becomes a row of strings in `bronze_stations` or `bronze_prices`.
   Lines it can't split into columns go to `quarantine_stations` or
   `quarantine_prices` instead of failing the load, and each file gets a row in
   `bronze_files` (size, upload time, content hash, rows loaded and rejected).

Everything lives in the `workspace.fuel_prices` schema.

### Raw file layout

Each file is named after the date it covers (from its `Estrazione del` line, or its
filename for older files). New files go to the landing folder, and Auto Loader moves
them to the archive 7 days after loading:

```
/Volumes/workspace/fuel_prices/raw/
├── landing/
│   ├── prices/prezzo_alle_8_2026-11-10.csv
│   └── stations/anagrafica_impianti_attivi_2026-11-10.csv
├── prices/prezzo_alle_8_2026-11-06.csv
└── stations/anagrafica_impianti_attivi_2026-11-06.csv
```

Files only move during a run that has new files to process, so they can stay in
the landing folder for a few days longer than 7. Bronze rows record the archive
path, so they stay valid after the move.

Auto Loader reads each file once. Replacing a file it has already processed won't
reload it; upload under a new name or reset the checkpoint in the `checkpoints`
volume instead. Uploading to the landing folder only starts the job for new file
names, so after resetting a checkpoint, start the job by hand.

## Setup

Install the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)
and add a `fuel-prices` profile for your workspace to `~/.databrickscfg`:

```ini
[fuel-prices]
host = https://<your-workspace-host>
token = <your-token>
```

To use a different profile name, change `workspace.profile` under the `dev` target
in `databricks.yml`.

## Deploy and run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run daily
```

## dbt models

The models in `dbt/` build on the bronze tables. To run them, add a dbt profile
for your workspace and run `dbt build` from the `dbt` folder:

- `stg_stations`, `stg_prices`: typed views over bronze
- `station_changes`, `price_changes`: each distinct line and the span of dates
  it appeared in, built incrementally
- `current_stations`, `current_prices`: the latest version of each
- `historical_prices`: the price for each station and fuel on each day, from when
  each price was submitted (a view, so filter by station)

## Fetching the files

mimit.gov.it only accepts connections from Italian addresses. Databricks Free
Edition also restricts outbound internet access, and hosted CI runners such as
GitHub Actions are outside Italy, so neither can download the files. Instead, run
`scripts/fetch_daily.py` from somewhere with an Italian connection.

The script uses only the Python standard library, so it runs anywhere Python 3.9+
does. It needs your workspace host and a token, either from the profile in
`~/.databrickscfg` or from environment variables:

```bash
python scripts/fetch_daily.py --profile fuel-prices --dry-run   # download and check, without uploading
python scripts/fetch_daily.py --profile fuel-prices

export DATABRICKS_HOST=https://<your-workspace-host>
export DATABRICKS_TOKEN=<your-token>
python scripts/fetch_daily.py
```

It uploads each file as `<name>_<extraction date>.csv`, skipping any already in the
landing folder or the archive, so running it more than once a day is harmless. The
files seem to be published around 08:45 Rome time, dated the previous day.

### Quarterly archives

The daily URLs only ever serve the latest files. Every daily file also goes into a
quarterly archive, available from early in the following quarter and back to 2015.
To fill gaps or load history, fetch whole quarters:

```bash
python scripts/fetch_daily.py --quarter 2026Q3
python scripts/fetch_daily.py --quarter 2022Q1 2022Q2 2022Q3 2022Q4
python scripts/fetch_daily.py --quarter 2026Q3 --type prices   # one file type only
```

Each day in the archive is uploaded under the same name a daily fetch would use, so
days you already have are skipped. A quarter is roughly 750 MB to upload across both
file types, and each one checks for every day before uploading, so expect it to take
a while; each line it prints starts with the time, and each quarter ends with how
long it took. The archive server often cuts long downloads short, so downloads
resume where they stopped, including after the script itself is interrupted: run the
same quarter again, and days already uploaded are skipped. Load about a year at a
time: the job's trigger stops working if the landing folder ever holds more than
10,000 files.

### Where to run it

How and where to run it is up to you: a cron job, a cloud scheduler in an Italian
region, or by hand. For [Pythonista](https://omz-software.com/pythonista/) on iOS,
`scripts/pythonista_fetch.py` is a ready-made wrapper that reads the host and token
from the iOS keychain; see the top of that file for setup.

The job starts on its own when new files arrive. If a run fails, fix the cause and
start it by hand with `databricks bundle run daily`; Auto Loader picks up any files
it hasn't processed yet.

## Source data quirks

The parsing rules handle these, so most need no attention:

- Files back to 2015 use `;` or `|` delimiters, and header names vary.
- Older files have no `Estrazione del` line; the date comes from the filename.
- Some fields contain the delimiter, such as names ending
  ` | gestori.prezzibenzina.it`. These are joined back together and kept as published.
- Missing values may be blank or the text `NULL`.
- The 2022-12-27 price file has pieces of a tar archive spliced into it; these
  are removed, and torn lines go to quarantine.
- Station types changed from `Altro` / `Strada Statale` / `Autostradale` (2015) to
  `Stradale` / `Autostradale`, and are kept as published.

## Test

```bash
python -m pytest
```
