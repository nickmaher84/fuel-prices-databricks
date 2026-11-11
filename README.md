# fuel-prices-databricks

Loads Italy's daily fuel
[station registry](https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv)
and [prices](https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv) from mimit.gov.it
into Databricks, deployed with a Declarative Automation Bundle.

Everything lives in the `workspace.fuel_prices` schema.

### Raw file layout

Each file is named after the date it covers (from its `Estrazione del` line, or its
filename for older files). The fetch script uploads new files to the landing folder:

```
/Volumes/workspace/fuel_prices/raw/landing/
├── prices/prezzo_alle_8_2026-11-10.csv
└── stations/anagrafica_impianti_attivi_2026-11-10.csv
```

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

## Deploy

```bash
databricks bundle validate
databricks bundle deploy
```

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

## Test

```bash
python -m pytest
```
