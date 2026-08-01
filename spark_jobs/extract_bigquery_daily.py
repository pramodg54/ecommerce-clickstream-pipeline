from datetime import datetime, timedelta
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CREDENTIALS_FILE = PROJECT_ROOT / "configs" / "gcp_credentials.json"

OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze" / "google_analytics"

START_DATE = "2016-08-01"
END_DATE = "2016-10-31"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# AUTHENTICATION
# -----------------------------------------------------------------------------

credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_FILE
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

print("=" * 70)
print("Connected to BigQuery")
print("Project :", credentials.project_id)
print("=" * 70)

# -----------------------------------------------------------------------------
# DATE LOOP
# -----------------------------------------------------------------------------

start = datetime.strptime(START_DATE, "%Y-%m-%d")
end = datetime.strptime(END_DATE, "%Y-%m-%d")

current = start

while current <= end:

    suffix = current.strftime("%Y%m%d")

    outfile = OUTPUT_DIR / f"ga_sessions_{suffix}.parquet"

    if outfile.exists():
        print(f"Skipping {suffix} (already exists)")
        current += timedelta(days=1)
        continue

    print(f"\nExtracting {suffix}")

    query = f"""
    SELECT *
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX = '{suffix}'
    """

    try:

        job = client.query(query)

        df = job.to_dataframe()

        print(f"Rows : {len(df):,}")

        if len(df) == 0:
            current += timedelta(days=1)
            continue

        df.to_parquet(outfile, index=False)

        print(f"Saved -> {outfile.name}")

    except Exception as e:

        print(f"Failed for {suffix}")
        print(e)

    current += timedelta(days=1)

print("\nBronze extraction completed.")