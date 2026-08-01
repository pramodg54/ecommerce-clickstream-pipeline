from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path

builder = (
    SparkSession.builder
    .appName("debug_schema")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "/opt/project/data/silver/stg_products"

print("Checking path...")

if Path(path).exists():
    print("YES - Delta folder exists")

    df = spark.read.format("delta").load(path)

    print("\nSchema:\n")
    df.printSchema()

else:
    print("NO - Delta folder does not exist")

spark.stop()