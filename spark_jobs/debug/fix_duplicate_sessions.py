from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.config import STG_SESSIONS_PATH


def main():

    spark = get_spark("fix_duplicate_sessions")

    df = (
        spark.read
        .format("delta")
        .load(str(STG_SESSIONS_PATH))
    )

    print(f"Rows before : {df.count():,}")

    window_spec = (
        Window
        .partitionBy("session_id")
        .orderBy(col("visit_start_time").desc())
    )

    dedup_df = (
        df
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    print(f"Rows after : {dedup_df.count():,}")

    (
        dedup_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(STG_SESSIONS_PATH))
    )

    print("Duplicate cleanup completed.")

    spark.stop()


if __name__ == "__main__":
    main()