from pathlib import Path

from pyspark.sql.functions import explode, col

from spark_jobs.common.spark_session import SparkSessionManager
from spark_jobs.common.data_reader import DataReader
from spark_jobs.common.pipeline_constants import BRONZE_PATH


def main():

    spark = SparkSessionManager().get_or_create()

    reader = DataReader(spark)

    tables = sorted(reader.list_delta_tables(BRONZE_PATH))

    print(f"Found {len(tables)} Bronze tables")

    # Inspect first two valid tables
    for table in tables[:2]:

        print("\n" + "=" * 80)
        print(f"TABLE : {Path(table).name}")
        print("=" * 80)

        bronze_df = reader.read_delta(table)

        hits_df = (
            bronze_df
            .withColumn("hit", explode(col("hits")))
            .select(
                col("hit.page.searchKeyword").alias("page_search_keyword")
            )
        )

        print("\nSchema")
        hits_df.printSchema()

        print("\nSample Data")
        hits_df.show(20, truncate=False)


if __name__ == "__main__":
    main()