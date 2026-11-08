# fuel-prices-databricks

Loads Italy's daily fuel
[station registry](https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv)
and [prices](https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv) from mimit.gov.it
into Databricks, deployed with a Declarative Automation Bundle.

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
