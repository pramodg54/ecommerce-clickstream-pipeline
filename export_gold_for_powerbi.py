from pathlib import Path

from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("Export Gold Tables for Power BI")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

PROJECT_ROOT = Path("/opt/project")

GOLD_PATH = PROJECT_ROOT / "data" / "gold"

OUTPUT_PATH = PROJECT_ROOT / "dashboard_data"

OUTPUT_PATH.mkdir(exist_ok=True)

TABLES = [

    "dim_date",

    "dim_device",

    "dim_product",

    "dim_traffic_source",

    "dim_user",

    "fact_sessions",

    "fact_hits",

    "fact_product_affinity",

]

def export_table(table_name):

    print("=" * 80)
    print(f"Exporting {table_name}")

    input_path = str(GOLD_PATH / table_name)

    output_path = str(OUTPUT_PATH / table_name)

    df = (

        spark.read

        .format("delta")

        .load(input_path)

    )

    print(f"Rows : {df.count():,}")

    (

        df

        .coalesce(1)

        .write

        .mode("overwrite")

        .parquet(output_path)

    )

    print(f"Completed : {table_name}")

for table in TABLES:

    export_table(table)

print("=" * 80)
print("ALL TABLES EXPORTED SUCCESSFULLY")
print("=" * 80)

spark.stop()