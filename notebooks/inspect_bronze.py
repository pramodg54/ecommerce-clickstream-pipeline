import pandas as pd

df = pd.read_parquet(
    "data/bronze/google_analytics/ga_sessions_bronze.parquet"
)

print(df.head())
print()
print("Shape:", df.shape)
print()
print(df.dtypes)