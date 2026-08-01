"""
===============================================================================
Spark Session Utility
===============================================================================
Creates a reusable Spark Session with Delta Lake support.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from spark_jobs.common.config import APP_NAME


def get_spark(
    app_name: str | None = None,
) -> SparkSession:
    """
    Create or return a Spark Session.

    Parameters
    ----------
    app_name : str, optional
        Spark application name.
        Uses APP_NAME from config if omitted.
    """

    if app_name is None:
        app_name = APP_NAME

    builder = (

        SparkSession.builder

        .appName(app_name)

        # Delta Lake
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )

        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # Performance
        .config(
            "spark.sql.shuffle.partitions",
            "8",
        )

        # Delta Schema Evolution
        .config(
            "spark.databricks.delta.schema.autoMerge.enabled",
            "true",
        )

        # Dynamic Partition Overwrite
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )

    )

    spark = (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark