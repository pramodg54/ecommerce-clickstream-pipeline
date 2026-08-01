import pandas as pd
from great_expectations.dataset import PandasDataset
from pathlib import Path
import json
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if Path("/opt/project").exists():
    PROJECT_ROOT = Path("/opt/project")

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "google_analytics"
    / "ga_sessions_silver.parquet"
)

print(f"Reading silver data from: {SILVER_PATH}")

df = pd.read_parquet(SILVER_PATH)

gx_df = PandasDataset(df)

results = []

results.append(
    gx_df.expect_column_values_to_not_be_null(
        "fullVisitorId"
    )
)

results.append(
    gx_df.expect_compound_columns_to_be_unique(
        ["fullVisitorId", "visitId"]
    )
)

results.append(
    gx_df.expect_column_values_to_be_between(
        "revenue_usd",
        min_value=0
    )
)

results.append(
    gx_df.expect_column_values_to_be_in_set(
        "deviceCategory",
        ["desktop", "mobile", "tablet"]
    )
)

all_passed = True

for result in results:
    print(json.dumps(result, indent=2, default=str))

    if not result["success"]:
        all_passed = False

if not all_passed:
    print("\nValidation failed.")
    sys.exit(1)

print("\nAll validations passed successfully.")