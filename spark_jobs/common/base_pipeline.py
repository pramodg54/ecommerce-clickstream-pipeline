"""
===============================================================================
File Name : base_pipeline.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Base class for all ETL pipelines.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from spark_jobs.common.data_reader import DataReader
from spark_jobs.common.data_writer import DataWriter
from spark_jobs.common.metadata import MetadataManager
from spark_jobs.common.pipeline_config import PipelineConfig
from spark_jobs.common.pipeline_metrics import PipelineMetrics
from spark_jobs.common.exceptions import PipelineExecutionException


class BasePipeline(ABC):
    """
    Base class for all ETL pipelines.

    Execution Flow

        Extract
            ↓
        Transform
            ↓
        Add Metadata
            ↓
        Validate
            ↓
        Load
    """

    def __init__(
        self,
        spark: SparkSession,
        logger,
        pipeline_name: str,
    ):

        self.spark = spark
        self.logger = logger

        pipeline = PipelineConfig(pipeline_name)

        # PipelineDefinition
        self.config = pipeline.config

        self.reader = DataReader(spark)

        self.writer = DataWriter(spark)

        self.metadata = MetadataManager(self.config)

        self.metrics = PipelineMetrics(
            pipeline_name=self.config.name
        )

    # ======================================================================
    # Abstract Methods
    # ======================================================================

    @abstractmethod
    def extract(
        self,
        work_item=None,
    ) -> DataFrame:
        """
        Extract one unit of work.

        Default:
            work_item = None (single-source pipelines)

        Incremental pipelines:
            work_item = Bronze Delta table path
        """
        pass

    @abstractmethod
    def transform(
        self,
        source_df: DataFrame,
    ) -> DataFrame:
        pass

    @abstractmethod
    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        pass

    @abstractmethod
    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        pass

    # ======================================================================
    # Work Discovery
    # ======================================================================

    def discover(self):
        """
        Return iterable of work items.

        Default behaviour:
            Execute pipeline once.
        """

        return [None]

    # ======================================================================
    # Pipeline Runner
    # ======================================================================

    def run(self):

        try:

            self.logger.info(
                "=" * 80
            )

            self.logger.info(
                "Starting Pipeline : %s",
                self.config.name,
            )

            total_rows_read = 0
            total_rows_written = 0

            for index, work_item in enumerate(self.discover()):

                source_df = self.extract(work_item)

                rows_read = 0
                total_rows_read += rows_read

                target_df = self.transform(source_df)

                target_df = self.metadata.add_metadata(target_df)

                self.validate(target_df)

                self.load(target_df)

                rows_written = 0
                total_rows_written += rows_written

                # -------------------------
                # Free memory
                # -------------------------
                try:
                    source_df.unpersist(blocking=False)
                except Exception:
                    pass

                try:
                    target_df.unpersist(blocking=False)
                except Exception:
                    pass

                del source_df
                del target_df

            self.metrics.rows_read = total_rows_read
            self.metrics.rows_written = total_rows_written

            self.metrics.finish_success()

            self.metrics.log(
                self.logger
            )

            self.logger.info(
                "Pipeline completed successfully."
            )

        except Exception as exc:

            self.metrics.finish_failure(
                str(exc)
            )

            self.metrics.log(
                self.logger
            )

            raise PipelineExecutionException(
                f"Pipeline '{self.config.name}' failed."
            ) from exc