from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Bronze GA Extraction")
    .config(
        "spark.jars.packages",
        ",".join([
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1",
            "io.delta:delta-spark_2.12:3.2.0"
        ])
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.enable",
        "true"
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "../configs/gcp_credentials.json"
    )
    .getOrCreate()
)

query = """
SELECT *
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20161031'
"""

df = (
    spark.read
        .format("bigquery")
        .option("query", query)
        .load()
)

print("Rows :", df.count())
print("Columns :", len(df.columns))

df.printSchema()

(
    df.write
      .format("delta")
      .mode("overwrite")
      .save("../data/bronze/google_analytics")
)

print("Bronze Delta created successfully.")

spark.stop()