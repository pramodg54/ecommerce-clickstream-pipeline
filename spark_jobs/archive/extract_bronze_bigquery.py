from google.cloud import bigquery
from pathlib import Path
import pandas as pd

client = bigquery.Client()

query = """
SELECT *
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20161031'
"""

print("=" * 60)
print("Starting Bronze Extraction...")
print("=" * 60)

job = client.query(query)

df = job.to_dataframe()

output_path = Path(
    "data/bronze/google_analytics/ga_sessions_raw_bronze.parquet"
)

output_path.parent.mkdir(parents=True, exist_ok=True)

df.to_parquet(output_path, index=False)

print("\nExtraction Completed Successfully!")
print("-" * 60)
print(f"Rows Extracted : {len(df):,}")
print(f"Columns        : {len(df.columns)}")
print(f"Output File    : {output_path}")

file_size = output_path.stat().st_size / (1024 * 1024)

print(f"File Size      : {file_size:.2f} MB")