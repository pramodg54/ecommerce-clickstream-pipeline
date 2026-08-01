"""
===============================================================================
File Name : metadata.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Adds standard ETL metadata columns to DataFrames.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from spark_jobs.common.pipeline_constants import (
    CREATED_BY,
    DEFAULT_CREATED_BY,
    DEFAULT_JOB_VERSION,
    ETL_LOADED_TIMESTAMP,
    JOB_VERSION,
    LAYER,
    PIPELINE_NAME,
)


class MetadataManager:
    """
    Add standard metadata columns to a DataFrame.

    Example
    -------
    config = PipelineConfig("stg_products")

    df = MetadataManager(config).add_metadata(df)
    """

    def __init__(self, pipeline_config):

        self.pipeline = pipeline_config

    def add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add framework metadata columns.
        """

        return (

            df

            .withColumn(
                ETL_LOADED_TIMESTAMP,
                current_timestamp(),
            )

            .withColumn(
                PIPELINE_NAME,
                lit(self.pipeline.name),
            )

            .withColumn(
                LAYER,
                lit(self.pipeline.layer),
            )

            .withColumn(
                JOB_VERSION,
                lit(self.pipeline.version),
            )

            .withColumn(
                CREATED_BY,
                lit(DEFAULT_CREATED_BY),
            )

        )