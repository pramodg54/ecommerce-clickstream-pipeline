"""
===============================================================================
File Name : base_gold_pipeline.py
Layer     : Gold Framework
Purpose   : Base class for all Gold pipelines.
===============================================================================
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.common.logger import get_logger
from spark_jobs.common.data_reader import DataReader
from spark_jobs.common.data_writer import DataWriter
from spark_jobs.common.metadata import MetadataManager
from spark_jobs.common.pipeline_metrics import PipelineMetrics
from spark_jobs.common.config import SILVER_PATH
from spark_jobs.common.pipeline_config import PIPELINE_CONFIG


class BaseGoldPipeline(ABC):
    """
    Base class for all Gold pipelines.

    Gold pipelines:
        - Read one or more Silver tables
        - Perform business transformations
        - Validate output
        - Write Gold Delta tables
    """

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
    ):

        self.spark = spark

        self.pipeline_name = pipeline_name

        self.config = PIPELINE_CONFIG[
            pipeline_name
        ]

        self.logger = get_logger(
            pipeline_name
        )

        self.reader = DataReader(
            spark
        )

        self.writer = DataWriter(
            spark
        )

        self.metadata = MetadataManager(
            self.config
        )

        self.metrics = PipelineMetrics(
            pipeline_name=pipeline_name,
        )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def read_silver_table(
        self,
        table_name: str,
        cache: bool = True,
    ) -> DataFrame:
        """
        Read a Silver Delta table.

        Example
        -------
        self.read_silver_table("stg_sessions")
        """

        self.logger.info(
            f"Reading Silver table : {table_name}"
        )

        return self.reader.read_delta(
            f"{SILVER_PATH}/{table_name}"
        )

    def read_multiple_silver_tables(
        self,
        table_names: list[str],
        cache: bool = True,
    ) -> Dict[str, DataFrame]:
        """
        Read multiple Silver tables.

        Returns
        -------
        {
            "stg_sessions": DataFrame,
            "stg_hits": DataFrame,
            ...
        }
        """

        dataframes = {}

        for table in table_names:

            dataframes[table] = self.read_silver_table(
                table_name=table,
                cache=cache,
            )

        return dataframes
    # =========================================================================
    # Abstract Methods
    # =========================================================================

    @abstractmethod
    def extract(
        self,
    ) -> Dict[str, DataFrame]:
        """
        Read all required Silver tables.

        Returns
        -------
        Dictionary of DataFrames.
        """
        pass

    @abstractmethod
    def transform(
        self,
        dataframes: Dict[str, DataFrame],
    ) -> DataFrame:
        """
        Business transformation.

        Returns
        -------
        Gold DataFrame.
        """
        pass

    @abstractmethod
    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Execute Gold validations.
        """
        pass

    @abstractmethod
    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Gold Delta table.
        """
        pass

    # =========================================================================
    # Execute Pipeline
    # =========================================================================

    def run(self):

        self.logger.info(
            "=" * 80
        )

        self.logger.info(
            f"Starting {self.pipeline_name}"
        )


        try:

            # --------------------------------------------------------------
            # Extract
            # --------------------------------------------------------------

            silver_tables = self.extract()

            # --------------------------------------------------------------
            # Transform
            # --------------------------------------------------------------

            gold_df = self.transform(
                silver_tables
            )

            # --------------------------------------------------------------
            # Metadata
            # --------------------------------------------------------------

            gold_df = self.metadata.add_metadata(
                gold_df
            )

            gold_df.cache()

            # --------------------------------------------------------------
            # Validate
            # --------------------------------------------------------------

            self.validate(
                gold_df
            )

            # --------------------------------------------------------------
            # Load
            # --------------------------------------------------------------

            self.load(
                gold_df
            )

            rows = gold_df.count()

            self.metrics.rows_written = rows

            self.logger.info(
                f"Rows Written : {rows:,}"
            )

            gold_df.unpersist()

            self.metrics.finish_success()

            self.logger.info(
                f"{self.pipeline_name} completed successfully."
            )

        except Exception as ex:

            self.metrics.finish_failure(
                str(ex)
            )

            self.logger.exception(ex)

            raise

        finally:

            self.metrics.log(
                self.logger
            )

            self.logger.info(
                "=" * 80
            )