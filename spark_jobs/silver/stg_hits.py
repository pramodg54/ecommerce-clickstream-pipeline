"""
===============================================================================
File Name : stg_hits.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Silver
Purpose   : Flatten Google Analytics Hit records into a Silver Delta table.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode_outer,
    concat_ws,
    coalesce,
    lit,
)

from spark_jobs.common.base_pipeline import BasePipeline
from spark_jobs.common.validations import ValidationSuite
from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.logger import get_logger
from spark_jobs.common.config import BRONZE_PATH


class StgHitsPipeline(BasePipeline):

    """
    --------------------------------------------------------------------------
    Bronze Delta

        ↓

    Explode Hits

        ↓

    Flatten Hit

        ↓

    Validation

        ↓

    Silver Delta
    --------------------------------------------------------------------------
    """

    START_AFTER_DATE = None

    def __init__(self, spark):

        logger = get_logger(self.__class__.__name__)

        super().__init__(
            spark=spark,
            logger=logger,
            pipeline_name="stg_hits",
        )

    # ==========================================================================
    # Discover
    # ==========================================================================

    def discover(self):

        self.logger.info(
            "Discovering Bronze Delta tables..."
        )

        tables = self.reader.list_delta_tables(
            BRONZE_PATH,
        )

        filtered = []

        for table in tables:

            table_date = Path(table).name.replace(
                "ga_sessions_",
                "",
            )

            if self.START_AFTER_DATE is None or table_date > self.START_AFTER_DATE:

                filtered.append(table)

        self.logger.info(
            "Discovered %d Bronze tables.",
            len(filtered),
        )

        return filtered

    # ==========================================================================
    # Extract
    # ==========================================================================

    def extract(
        self,
        work_item,
    ) -> DataFrame:

        self.logger.info(
            "Reading Bronze table: %s",
            Path(work_item).name,
        )

        return self.reader.read_delta(
            work_item,
        )

    # ==========================================================================
    # Helper Methods
    # ==========================================================================
    def _explode_hits(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "hit",
                explode_outer(
                    col("hits")
                ),
            )

        )

    # ======================================================================

    def _session_key(
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

    # ======================================================================

    def _hit_key(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "hit_number",
                col("hit.hitNumber"),
            )

            .withColumn(

                "hit_key",

                concat_ws(

                    "_",

                    col("session_id"),

                    col("hit.hitNumber"),

                ),

            )

        )

    # ======================================================================

    def _page_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "page_path",
                col("hit.page.pagePath"),
            )

            .withColumn(
                "page_title",
                col("hit.page.pageTitle"),
            )

            .withColumn(
                "hostname",
                col("hit.page.hostname"),
            )

            .withColumn(
                "page_search_keyword",
                col("hit.page.searchKeyword").cast("string"),
            )

            .withColumn(
                "page_path_level1",
                col("hit.page.pagePathLevel1"),
            )

            .withColumn(
                "page_path_level2",
                col("hit.page.pagePathLevel2"),
            )

            .withColumn(
                "page_path_level3",
                col("hit.page.pagePathLevel3"),
            )

        )

    # ======================================================================

    def _event_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "event_category",
                col("hit.eventInfo.eventCategory"),
            )

            .withColumn(
                "event_action",
                col("hit.eventInfo.eventAction"),
            )

            .withColumn(
                "event_label",
                col("hit.eventInfo.eventLabel"),
            )

            .withColumn(
                "event_value",
                col("hit.eventInfo.eventValue"),
            )

        )

    # ======================================================================

    def _transaction_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "transaction_id",
                col("hit.transaction.transactionId").cast("string"),
            )

            .withColumn(
                "transaction_affiliation",
                col("hit.transaction.affiliation").cast("string"),
            )

            .withColumn(

                "transaction_revenue",

                (
                    coalesce(
                        col("hit.transaction.transactionRevenue"),
                        lit(0),
                    ) / 1_000_000
                ).cast("double"),

            )

            .withColumn(
                "transaction_shipping",
                col("hit.transaction.transactionShipping").cast("long"),
            )

            .withColumn(
                "transaction_tax",
                col("hit.transaction.transactionTax").cast("long"),
            )

        )

    # ==========================================================================
    # Transform
    # ==========================================================================
    def transform(
        self,
        bronze_df: DataFrame,
    ) -> DataFrame:
        """
        Transform Bronze clickstream data into a Hit-level Silver table.

        One Row = One Hit
        """

        self.logger.info(
            "Transforming hit data..."
        )

        silver_df = bronze_df

        # ------------------------------------------------------------------
        # Explode Hits
        # ------------------------------------------------------------------

        silver_df = self._explode_hits(
            silver_df
        )

        # ------------------------------------------------------------------
        # Keys
        # ------------------------------------------------------------------

        silver_df = self._session_key(
            silver_df
        )

        silver_df = self._hit_key(
            silver_df
        )

        # ------------------------------------------------------------------
        # Flatten Nested Structures
        # ------------------------------------------------------------------

        silver_df = self._page_columns(
            silver_df
        )

        silver_df = self._event_columns(
            silver_df
        )

        silver_df = self._transaction_columns(
            silver_df
        )

        # ------------------------------------------------------------------
        # Hit Level Columns
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
                "visit_date",
                col("date").cast("string"),
            )

            .withColumn(
                "hit_time",
                col("hit.time").cast("long"),
            )

            .withColumn(
                "hit_hour",
                col("hit.hour").cast("int"),
            )

            .withColumn(
                "hit_minute",
                col("hit.minute").cast("int"),
            )

            .withColumn(
                "hit_type",
                col("hit.type").cast("string"),
            )

            .withColumn(
                "is_interaction",
                col("hit.isInteraction").cast("boolean"),
            )

            .withColumn(
                "is_entrance",
                col("hit.isEntrance").cast("boolean"),
            )

            .withColumn(
                "is_exit",
                col("hit.isExit").cast("boolean"),
            )

            .withColumn(
                "referer",
                col("hit.referer").cast("string"),
            )

            .withColumn(
                "data_source",
                col("hit.dataSource").cast("int"),
            )

            .withColumn(
                "content_group1",
                col("hit.contentGroup.contentGroup1").cast("string"),
            )

            .withColumn(
                "content_group2",
                col("hit.contentGroup.contentGroup2").cast("string"),
            )

            .withColumn(
                "content_group3",
                col("hit.contentGroup.contentGroup3").cast("string"),
            )

            .withColumn(
                "ecommerce_action_type",
                col("hit.eCommerceAction.action_type").cast("string"),
            )

            .withColumn(
                "ecommerce_step",
                col("hit.eCommerceAction.step").cast("long"),
            )

            .withColumn(
                "ecommerce_option",
                col("hit.eCommerceAction.option").cast("string"),
            )

        )

        # ------------------------------------------------------------------
        # Remove Invalid Hits
        # ------------------------------------------------------------------

        silver_df = (

            silver_df

            .filter(
                col("hit_number").isNotNull()
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
                "hit_key",
                "user_id",
                "visit_id",
                "visit_date",

                # Hit
                "hit_number",
                "hit_time",
                "hit_hour",
                "hit_minute",
                "hit_type",

                # Page
                "page_path",
                "page_title",
                "hostname",
                "page_search_keyword",
                "page_path_level1",
                "page_path_level2",
                "page_path_level3",

                # Event
                "event_category",
                "event_action",
                "event_label",
                "event_value",

                # Ecommerce
                "ecommerce_action_type",
                "ecommerce_step",
                "ecommerce_option",

                # Transaction
                "transaction_id",
                "transaction_affiliation",
                "transaction_revenue",
                "transaction_shipping",
                "transaction_tax",

                # Flags
                "is_interaction",
                "is_entrance",
                "is_exit",

                # Misc
                "referer",
                "data_source",
                "content_group1",
                "content_group2",
                "content_group3",

            )

        )

        self.logger.info(
            "Hit transformation completed."
        )

        return silver_df

    # ==========================================================================
    # Validate
    # ==========================================================================
    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Execute framework validations.
        """

        self.logger.info(
            "Validating hit data..."
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

    # ==========================================================================
    # Load
    # ==========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Silver Hit table.
        """

        self.logger.info(
            "Writing Silver Hit table..."
        )

        print("=" * 80)
        print("TARGET DATAFRAME SCHEMA")
        print("=" * 80)

        target_df.printSchema()

        for f in target_df.schema.fields:
            print(f"{f.name:<35} {f.dataType}")

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
            "Rows Written : %d",
            rows,
        )


# ==============================================================================
# Main
# ==============================================================================

def main():

    spark = get_spark(
        app_name="stg_hits",
    )

    pipeline = StgHitsPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()