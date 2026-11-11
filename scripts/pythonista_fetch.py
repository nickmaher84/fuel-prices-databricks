"""Run fetch_daily.py in Pythonista on iOS, with credentials from the keychain.

Store your workspace host and token once, from the Pythonista console:

    import console, keychain
    keychain.set_password("databricks", "host", "https://<your-workspace-host>")
    keychain.set_password("databricks", "token", console.password_alert("Databricks token"))

Then put this file and fetch_daily.py in the same Pythonista folder and run this one.
Arguments are passed through to fetch_daily.py, so "Run with Arguments" works for
--dry-run or --quarter. Without any, it uses ARGS below.

The screen is kept awake while it runs, as a quarterly archive can take a long time
to download and iOS may suspend Pythonista if the phone locks.
"""

import os
import sys

import console
import keychain

import fetch_daily

ARGS = []  # For example ["--dry-run"], ["--quarter", "2026Q3"] or ["--quarter", "2026Q3", "--type", "prices"]

host = keychain.get_password("databricks", "host")
token = keychain.get_password("databricks", "token")
if not host or not token:
    sys.exit("Store the Databricks host and token in the keychain first (see the top of this file).")

os.environ["DATABRICKS_HOST"] = host
os.environ["DATABRICKS_TOKEN"] = token
console.set_idle_timer_disabled(True)
try:
    status = fetch_daily.main(sys.argv[1:] or ARGS)
finally:
    console.set_idle_timer_disabled(False)
print("Done." if status == 0 else "Finished with errors, see above.")
