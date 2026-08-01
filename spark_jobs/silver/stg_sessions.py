"""
===============================================================================
File Name : stg_sessions.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Silver
Purpose   : Build Session level staging table from Bronze clickstream data.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    when,
    coalesce,
)
from pathlib import Path
from spark_jobs.common.base_pipeline import BasePipeline
from spark_jobs.common.validations import ValidationSuite
from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.logger import get_logger
from spark_jobs.common.config import (
    BRONZE_PATH,
)


class StgSessionsPipeline(BasePipeline):
    """
    ---------------------------------------------------------------------------
    Session Level Silver Pipeline

    Bronze
        ↓

    Read all bronze delta tables

        ↓

    Flatten nested Google Analytics session information

        ↓

    Standard Validation

        ↓

    Delta Silver Table
    ---------------------------------------------------------------------------
    """

    def __init__(self, spark):

        logger = get_logger(self.__class__.__name__)

        super().__init__(
            spark=spark,
            logger=logger,
            pipeline_name="stg_sessions",
        )

    START_AFTER_DATE = None
    END_DATE = None
    # None = process everything
    # "20160915" = start from 16 Sept onwards
    # "20161023" = process after 23 Oct


    def discover(self):
        """
        Return one Bronze table per work item.
        """

        tables = self.reader.list_delta_tables(BRONZE_PATH)

        if self.START_AFTER_DATE is not None:

            tables = [
                table
                for table in tables
                if Path(table).name.replace(
                    "ga_sessions_",
                    ""
                ) > self.START_AFTER_DATE
            ]

            self.logger.info(
                "Processing %d Bronze tables after %s",
                len(tables),
                self.START_AFTER_DATE,
            )

        else:

            self.logger.info(
                "Processing all %d Bronze tables",
                len(tables),
            )

        if not tables:
            raise ValueError(
                "No Bronze tables found after applying START_AFTER_DATE filter."
            )

        return tables


    # ==========================================================================
    # Extract
    # ==========================================================================

    def extract(
        self,
        work_item,
    ) -> DataFrame:
        """
        Read a single Bronze Delta table.
        """

        self.logger.info(
            "Reading Bronze table: %s",
            Path(work_item).name,
        )


        df = self.reader.read_delta(
            input_path=work_item,
        )

        df = self.reader._normalize_schema(df)

        return df

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def _session_identifier(self, df: DataFrame) -> DataFrame:
        """
        Create business session identifier.
        """

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

    def _traffic_columns(self, df: DataFrame) -> DataFrame:
        """
        Flatten traffic source.
        """

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
                "adwords_click_info",
                col("trafficSource.adwordsClickInfo.gclId").cast("string"),
            )

        )

    def _device_columns(self, df: DataFrame) -> DataFrame:
        """
        Flatten device attributes.
        """

        return (

            df

            .withColumn(
                "device_category",
                col("device.deviceCategory").cast("string"),
            )

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
                "operating_system_version",
                col("device.operatingSystemVersion").cast("string"),
            )

            .withColumn(
                "mobile_brand",
                col("device.mobileDeviceBranding").cast("string"),
            )

            .withColumn(
                "mobile_model",
                col("device.mobileDeviceModel").cast("string"),
            )

            .withColumn(
                "language",
                col("device.language").cast("string"),
            )

            .withColumn(
                "screen_resolution",
                col("device.screenResolution").cast("string"),
            )

        )

    def _geo_columns(self, df: DataFrame) -> DataFrame:
        """
        Flatten geographic information.
        """

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
                lit(None).cast("double"),
            )

            .withColumn(
                "longitude",
                lit(None).cast("double"),
            )

        )

    def _totals_columns(self, df: DataFrame) -> DataFrame:
        """
        Flatten totals section.
        """

        return (

            df

            .withColumn(
                "page_views",
                coalesce(
                    col("totals.pageviews"),
                    lit(0),
                ).cast("long"),
            )

            .withColumn(
                "hits",
                coalesce(
                    col("totals.hits"),
                    lit(0),
                ).cast("long"),
            )

            .withColumn(
                "transactions",
                coalesce(
                    col("totals.transactions"),
                    lit(0)
                ).cast("long"),
            )

            .withColumn(
                "transaction_revenue",
                coalesce(
                    col("totals.totalTransactionRevenue"),
                    lit(0),
                ).cast("long"),
            )

            .withColumn(
                "bounces",
                coalesce(
                    col("totals.bounces"),
                    lit(0),
                ).cast("long"),
            )

            .withColumn(
                "new_visits",
                coalesce(
                    col("totals.newVisits"),
                    lit(0),
                ).cast("long"),
            )

            .withColumn(
                "time_on_site",
                coalesce(
                    col("totals.timeOnSite"),
                    lit(0),
                ).cast("long"),
            )

        )
# ==============================================================================
# Transform
# ==============================================================================

    def transform(
        self,
        bronze_df: DataFrame,
    ) -> DataFrame:
        """
        Transform Bronze clickstream data into a flattened session-level table.
        """

        self.logger.info(
            "Transforming session data..."
        )

        silver_df = bronze_df

        # ------------------------------------------------------------------
        # Business Session Identifier
        # ------------------------------------------------------------------

        silver_df = self._session_identifier(silver_df)

        # ------------------------------------------------------------------
        # Flatten nested structures
        # ------------------------------------------------------------------

        silver_df = self._traffic_columns(silver_df)

        silver_df = self._device_columns(silver_df)

        silver_df = self._geo_columns(silver_df)

        silver_df = self._totals_columns(silver_df)

        # ------------------------------------------------------------------
        # Standard Session Columns
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "user_id",
                col("fullVisitorId").cast("string"),
            )

            .withColumn(
                "visit_id",
                col("visitId").cast("long"),
            )

            .withColumn(
                "visit_number",
                col("visitNumber").cast("long"),
            )

            .withColumn(
                "visit_start_time",
                col("visitStartTime").cast("long"),
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
                "social_engagement_type",
                col("socialEngagementType").cast("string"),
            )

            .withColumn(
                "is_mobile",

                when(
                    col("device.deviceCategory") == "mobile",
                    True,
                ).otherwise(False),

            )

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

                # Visit Information
                "visit_id",
                "visit_number",
                "visit_start_time",
                "visit_date",

                # Traffic
                "channel_grouping",
                "traffic_source",
                "traffic_medium",
                "campaign",
                "keyword",
                "ad_content",
                "referral_path",
                "adwords_click_info",

                # Device
                "device_category",
                "browser",
                "browser_version",
                "operating_system",
                "operating_system_version",
                "mobile_brand",
                "mobile_model",
                "language",
                "screen_resolution",
                "is_mobile",

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

                # Session Metrics
                "page_views",
                "hits",
                "transactions",
                "transaction_revenue",
                "bounces",
                "new_visits",
                "time_on_site",

                # Other
                "social_engagement_type",

            )

        )

        self.logger.info(
            "Session transformation completed."
        )

        # ------------------------------------------------------------------
        # Keep Latest Version of Each Session
        # ------------------------------------------------------------------

        window_spec = (
            Window
            .partitionBy("session_id")
            .orderBy(col("visit_start_time").desc())
        )

        silver_df = (
            silver_df
            .withColumn(
                "_rn",
                row_number().over(window_spec),
            )
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        return silver_df
    # ==============================================================================
    # Validate
    # ==============================================================================

    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Run standard framework validations.
        """

        self.logger.info(
            "Running data validations..."
        )

        ValidationSuite(

            required_columns=self.config.required_columns,

            key_columns=self.config.key_columns,

            non_negative_columns=self.config.non_negative_columns,

            minimum_rows=self.config.min_rows,

        ).run(target_df)

        self.logger.info(
            "Validation completed successfully."
        )

    # ==============================================================================
    # Load
    # ==============================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Silver Session table.
        """

        self.logger.info(
            "Writing Silver Session table..."
        )

        print("=" * 80)
        print("SCHEMA BEFORE WRITE")
        target_df.printSchema()

        print(target_df.schema["ad_content"])

        print(self.config.write_mode)

        self.writer.write_delta(

            df=target_df,

            output_path=self.config.output_path,

            mode="append",

            partition_columns=[
                self.config.partition_column,
            ],

        )

        rows = self.writer.verify_write(
            self.config.output_path
        )

        self.logger.info(
            f"Rows written : {rows:,}"
        )

    # ==============================================================================
    # Main
    # ==============================================================================

def main():

    spark = get_spark(
        app_name="stg_sessions"
    )

    pipeline = StgSessionsPipeline(
        spark=spark,
    )

    pipeline.run()

# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()