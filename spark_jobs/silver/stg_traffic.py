"""
===============================================================================
File Name : stg_traffic.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Silver
Purpose   : Flatten Traffic, Device and Geographic information.
===============================================================================
"""

from __future__ import annotations
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
)

from spark_jobs.common.base_pipeline import BasePipeline
from spark_jobs.common.config import BRONZE_PATH
from spark_jobs.common.logger import get_logger
from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.validations import ValidationSuite


# --------------------------------------------------------------------------
# Development Filter
# --------------------------------------------------------------------------

START_AFTER_DATE = '20161012'


class StgTrafficPipeline(BasePipeline):

    def __init__(self, spark):

        logger = get_logger(self.__class__.__name__)

        super().__init__(
            spark=spark,
            logger=logger,
            pipeline_name="stg_traffic",
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

    def _create_session_key(
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

        )

    def _traffic_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "traffic_source",
                col("trafficSource.source").cast("string"),
            )

            .withColumn(
                "traffic_medium",
                col("trafficSource.medium").cast("string"),
            )

            .withColumn(
                "campaign",
                col("trafficSource.campaign").cast("string"),
            )

            .withColumn(
                "keyword",
                col("trafficSource.keyword").cast("string"),
            )

            .withColumn(
                "ad_content",
                col("trafficSource.adContent").cast("string"),
            )

            .withColumn(
                "referral_path",
                col("trafficSource.referralPath").cast("string"),
            )

            .withColumn(
                "campaign_code",
                col("trafficSource.campaignCode").cast("string"),
            )

        )

    def _device_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "browser",
                col("device.browser").cast("string"),
            )

            .withColumn(
                "browser_version",
                col("device.browserVersion").cast("string"),
            )

            .withColumn(
                "operating_system",
                col("device.operatingSystem").cast("string"),
            )

            .withColumn(
                "device_category",
                col("device.deviceCategory").cast("string"),
            )

            .withColumn(
                "mobile_device_brand",
                col("device.mobileDeviceBranding").cast("string"),
            )

            .withColumn(
                "mobile_device_model",
                col("device.mobileDeviceModel").cast("string"),
            )

            .withColumn(
                "language",
                col("device.language").cast("string"),
            )

        )

    def _geo_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "continent",
                col("geoNetwork.continent").cast("string"),
            )

            .withColumn(
                "sub_continent",
                col("geoNetwork.subContinent").cast("string"),
            )

            .withColumn(
                "country",
                col("geoNetwork.country").cast("string"),
            )

            .withColumn(
                "region",
                col("geoNetwork.region").cast("string"),
            )

            .withColumn(
                "metro",
                col("geoNetwork.metro").cast("string"),
            )

            .withColumn(
                "city",
                col("geoNetwork.city").cast("string"),
            )

            .withColumn(
                "network_domain",
                col("geoNetwork.networkDomain").cast("string"),
            )

            .withColumn(
                "latitude",
                col("geoNetwork.latitude").cast("double"),
            )

            .withColumn(
                "longitude",
                col("geoNetwork.longitude").cast("double"),
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
        Transform Bronze clickstream data into a Traffic-level Silver table.

        One Row = One Session
        """

        self.logger.info(
            "Transforming traffic data..."
        )

        silver_df = bronze_df

        # ------------------------------------------------------------------
        # Create Session Key
        # ------------------------------------------------------------------

        silver_df = self._create_session_key(
            silver_df
        )

        # ------------------------------------------------------------------
        # Flatten Traffic Source
        # ------------------------------------------------------------------

        silver_df = self._traffic_columns(
            silver_df
        )

        # ------------------------------------------------------------------
        # Flatten Device Information
        # ------------------------------------------------------------------

        silver_df = self._device_columns(
            silver_df
        )

        # ------------------------------------------------------------------
        # Flatten Geographic Information
        # ------------------------------------------------------------------

        silver_df = self._geo_columns(
            silver_df
        )

        # ------------------------------------------------------------------
        # Session Information
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
                "visit_start_time",
                col("visitStartTime").cast("long"),
            )

            .withColumn(
                "channel_grouping",
                col("channelGrouping").cast("string"),
            )

            .withColumn(
                "visitor_type",
                col("totals.newVisits").cast("integer"),
            )

        )

        # ------------------------------------------------------------------
        # Engagement Metrics
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "page_views",
                col("totals.pageviews").cast("integer"),
            )

            .withColumn(
                "session_duration",
                col("totals.timeOnSite").cast("long"),
            )

            .withColumn(
                "bounce",
                col("totals.bounces").cast("integer"),
            )

            .withColumn(
                "transactions",
                col("totals.transactions").cast("integer"),
            )

            .withColumn(
                "transaction_revenue",
                (
                    col("totals.transactionRevenue")
                    / 1_000_000
                ).cast("double"),
            )

        )

        # ------------------------------------------------------------------
        # Remove Invalid Sessions
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .filter(
                col("session_id").isNotNull()
            )

            .dropDuplicates()

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .select(

                # Keys
                "session_id",
                "user_id",
                "visit_id",

                # Session
                "visit_date",
                "visit_start_time",
                "channel_grouping",

                # Traffic
                "traffic_source",
                "traffic_medium",
                "campaign",
                "campaign_code",
                "keyword",
                "ad_content",
                "referral_path",

                # Device
                "browser",
                "browser_version",
                "operating_system",
                "device_category",
                "mobile_device_brand",
                "mobile_device_model",
                "language",

                # Geography
                "continent",
                "sub_continent",
                "country",
                "region",
                "metro",
                "city",
                "network_domain",
                "latitude",
                "longitude",

                # Metrics
                "visitor_type",
                "page_views",
                "session_duration",
                "bounce",
                "transactions",
                "transaction_revenue",

            )

        )

        self.logger.info(
            f"Rows after transformation: {silver_df.count():,}"
        )

        self.logger.info(
            "Traffic transformation completed."
        )

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
            "Running Traffic validations..."
        )

        ValidationSuite(

            required_columns=self.config.required_columns,

            key_columns=self.config.key_columns,

            non_negative_columns=self.config.non_negative_columns,

            minimum_rows=self.config.min_rows,

        ).run(target_df)

        self.logger.info(
            "Traffic validation completed."
        )

    # ======================================================================
    # Load
    # ======================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Traffic Silver table.
        """

        self.logger.info(
            "Writing Traffic Silver table..."
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
    Entry point for the Staging Traffic pipeline.
    """

    spark = get_spark(
        app_name="stg_traffic",
    )

    pipeline = StgTrafficPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    main()