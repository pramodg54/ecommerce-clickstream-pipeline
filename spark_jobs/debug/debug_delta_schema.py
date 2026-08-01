from pyspark.sql import DataFrame

from spark_jobs.common.spark_session import get_spark

spark = get_spark("debug_delta_schema")

delta_path = "/opt/project/data/silver/stg_hits"

print("=" * 80)
print("DELTA TABLE SCHEMA")
print("=" * 80)

delta_df = spark.read.format("delta").load(delta_path)

delta_df.printSchema()

print("\nColumn Types\n")

for field in delta_df.schema.fields:
    print(f"{field.name:<40} {field.dataType}")