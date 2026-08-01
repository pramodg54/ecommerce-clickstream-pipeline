"""
===============================================================================
File Name : stg_promotions.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Silver
Purpose   : Flatten Promotion records into Silver Delta table.
===============================================================================
"""

from __future__ import annotations
from zipfile import Path
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    explode_outer,
)

from spark_jobs.common.base_pipeline import BasePipeline
from spark_jobs.common.config import BRONZE_PATH
from spark_jobs.common.logger import get_logger
from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.validations import ValidationSuite


# --------------------------------------------------------------------------
# Development Filter
# --------------------------------------------------------------------------

START_AFTER_DATE = None


class StgPromotionsPipeline(BasePipeline):

    def __init__(self, spark):

        logger = get_logger(self.__class__.__name__)

        super().__init__(
            spark=spark,
            logger=logger,
            pipeline_name="stg_promotions",
        )

    # ======================================================================
    # Discover
    # ======================================================================

    def discover(self):

        tables = self.reader.list_delta_tables(
            BRONZE_PATH
        )

        self.logger.info(
            f"Discovered {len(tables)} Bronze tables."
        )

        tables = sorted(tables)

        if START_AFTER_DATE is not None:
            tables = [
                table
                for table in tables
                if Path(table).name >= f"ga_sessions_{START_AFTER_DATE}"
            ]

            self.logger.info(
                f"Processing {len(tables)} tables "
                f"starting from {START_AFTER_DATE}"
            )

        return tables

    # ======================================================================
    # Extract
    # ======================================================================

    def extract(
        self,
        work_item,
    ) -> DataFrame:

        self.logger.info(
            f"Reading {Path(work_item).name}"
        )

        return self.reader.read_delta(
            work_item,
        )

    # ======================================================================
    # Helper Methods
    # ======================================================================

    def _explode_hits(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return df.withColumn(
            "hit",
            explode_outer(col("hits")),
        )

    def _explode_promotions(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return df.withColumn(
            "promotion",
            explode_outer(
                col("hit.promotion")
            ),
        )

    def _create_keys(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "session_id",
                concat_ws(
                    "_",
                    col("fullVisitorId"),
                    col("visitId"),
                ),
            )

            .withColumn(
                "hit_number",
                col("hit.hitNumber"),
            )

            .withColumn(
                "promotion_key",
                concat_ws(
                    "_",
                    col("fullVisitorId"),
                    col("visitId"),
                    col("hit.hitNumber"),
                    col("promotion.promoId"),
                ),
            )

        )

    def _promotion_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "promotion_id",
                col("promotion.promoId").cast("string"),
            )

            .withColumn(
                "promotion_name",
                col("promotion.promoName").cast("string"),
            )

            .withColumn(
                "creative_name",
                col("promotion.promoCreative").cast("string"),
            )

            .withColumn(
                "promotion_position",
                col("promotion.promoPosition").cast("string"),
            )

        )
    # ======================================================================
    # Transform
    # ======================================================================

    def transform(
        self,
        bronze_df: DataFrame,
    ) -> DataFrame:
        """
        Transform Bronze clickstream data into a Promotion-level Silver table.

        One Row = One Promotion
        """

        self.logger.info(
            "Transforming promotion data..."
        )

        silver_df = bronze_df

        # ------------------------------------------------------------------
        # Explode Nested Arrays
        # ------------------------------------------------------------------

        silver_df = self._explode_hits(
            silver_df
        )

        silver_df = self._explode_promotions(
            silver_df
        )

        # ------------------------------------------------------------------
        # Create Business Keys
        # ------------------------------------------------------------------

        silver_df = self._create_keys(
            silver_df
        )

        # ------------------------------------------------------------------
        # Promotion Attributes
        # ------------------------------------------------------------------

        silver_df = self._promotion_columns(
            silver_df
        )

        # ------------------------------------------------------------------
        # Session Attributes
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "user_id",
                col("fullVisitorId").cast("string"),
            )

            .withColumn(
                "visit_id",
                col("visitId").cast("string"),
            )

            .withColumn(
                "visit_date",
                col("date").cast("string"),
            )

            .withColumn(
                "channel_grouping",
                col("channelGrouping").cast("string"),
            )

            .withColumn(
                "traffic_source",
                col("trafficSource.source").cast("string"),
            )

            .withColumn(
                "traffic_medium",
                col("trafficSource.medium").cast("string"),
            )

            .withColumn(
                "device_category",
                col("device.deviceCategory").cast("string"),
            )

            .withColumn(
                "country",
                col("geoNetwork.country").cast("string"),
            )

        )

        # ------------------------------------------------------------------
        # Hit Attributes
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "hit_type",
                col("hit.type").cast("string"),
            )

            .withColumn(
                "hit_time",
                col("hit.time").cast("long"),
            )

            .withColumn(
                "page_path",
                col("hit.page.pagePath").cast("string"),
            )

            .withColumn(
                "promotion_action_type",
                col("hit.promotionActionInfo.promoIsView")
                .cast("integer"),
            )

            .withColumn(
                "promotion_click",
                col("hit.promotionActionInfo.promoIsClick")
                .cast("integer"),
            )

        )

        # ------------------------------------------------------------------
        # Remove Invalid Promotions
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .filter(
                col("promotion_id").isNotNull()
            )

            .dropDuplicates(
                [
                    "promotion_key",
                ]
            )

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .select(

                # Keys
                "promotion_key",
                "session_id",
                "user_id",
                "visit_id",
                "hit_number",

                # Promotion
                "promotion_id",
                "promotion_name",
                "creative_name",
                "promotion_position",

                # Promotion Metrics
                "promotion_action_type",
                "promotion_click",

                # Session
                "visit_date",
                "channel_grouping",
                "traffic_source",
                "traffic_medium",
                "device_category",
                "country",

                # Hit
                "hit_type",
                "hit_time",
                "page_path",

            )

        )

        self.logger.info(
            "Promotion transformation completed."
        )

        self.logger.info(
            "Promotion transformation completed."
        )

        silver_df = silver_df.dropDuplicates()

        return silver_df
    # ======================================================================
    # Validate
    # ======================================================================

    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Execute framework validations.
        """

        self.logger.info(
            "Running Promotion validations..."
        )

        ValidationSuite(

            required_columns=self.config.required_columns,

            key_columns=self.config.key_columns,

            non_negative_columns=self.config.non_negative_columns,

            minimum_rows=self.config.min_rows,

        ).run(target_df)

        self.logger.info(
            "Promotion validation completed."
        )

    # ======================================================================
    # Load
    # ======================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Promotion Silver table.
        """

        self.logger.info(
            "Writing Promotion Silver table..."
        )

        self.writer.write_delta(

            df=target_df,

            output_path=self.config.output_path,

            mode=self.config.write_mode,

            partition_columns=[
                self.config.partition_column,
            ],

        )

        rows = self.writer.verify_write(
            self.config.output_path
        )

        self.logger.info(
            f"Rows Written : {rows:,}"
        )
# ==============================================================================
# Main
# ==============================================================================

def main():
    """
    Entry point for the Staging Promotions pipeline.
    """

    spark = get_spark(
        app_name="stg_promotions",
    )

    pipeline = StgPromotionsPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    main()