from pathlib import Path

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.config import BRONZE_PATH

spark = get_spark("schema_check")

tables = sorted(Path(BRONZE_PATH).iterdir())

print(f"Found {len(tables)} Bronze tables\n")

for table in tables:
    print("=" * 80)
    print(table.name)

    df = spark.read.format("delta").load(str(table))

    print(df.schema["trafficSource"].dataType)