import pandas as pd

df = pd.read_parquet(
    "data/silver/google_analytics/ga_sessions_silver.parquet"
)

print(df.head())

print("\nShape:", df.shape)

print("\nColumns:")
print(df.columns)

print("\nRevenue:")
print(df["revenue_usd"].describe())