"""
===============================================================================
File Name : fact_hits.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Hit Fact table from Silver Hit records.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
)

from spark_jobs.gold.framework.base_gold_pipeline import (
    BaseGoldPipeline,
)

from spark_jobs.common.spark_session import (
    get_spark,
)

from spark_jobs.common.validations import (
    ValidationSuite,
)


class FactHitsPipeline(BaseGoldPipeline):

    """
    ===========================================================================
            Silver stg_hits
                    │
                    ▼
            Business Transformations
                    │
                    ▼
               Validation Checks
                    │
                    ▼
               Gold fact_hits
    ===========================================================================
    """

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(

            spark=spark,

            pipeline_name="fact_hits",

        )

    # ==========================================================================
    # Extract
    # ==========================================================================

    def extract(
        self,
    ) -> dict[str, DataFrame]:

        self.logger.info(
            "Reading Silver Hit table..."
        )

        return self.read_multiple_silver_tables(

            [
                "stg_hits",
            ]

        )

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def _rename_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Rename Silver columns to Gold-friendly names.
        """

        return (

            df

            .withColumnRenamed(
                "visit_id",
                "session_visit_id",
            )


            .withColumnRenamed(
                "user_id",
                "visitor_id",
            )

        )

    # ==========================================================================

    def _standardize_nulls(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Replace nullable boolean flags.
        """

        return (

            df

            .fillna(

                {

                    "is_interaction": False,
                    "is_entrance": False,
                    "is_exit": False,

                }

            )

        )

    # ==========================================================================

    def _derive_hit_category(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Business-friendly hit category.
        """

        return (

            df

            .withColumn(

                "hit_category",

                col("hit_type"),

            )

        )

    # ==========================================================================

    def _derive_transaction_flag(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Flag transaction hits.
        """

        return (

            df

            .withColumn(

                "is_transaction",

                col("transaction_id").isNotNull(),

            )

        )

    # ==========================================================================

    def _derive_event_flag(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Flag event hits.
        """

        return (

            df

            .withColumn(

                "is_event",

                col("event_category").isNotNull(),

            )

        )

    # ==========================================================================

    def _derive_pageview_flag(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Flag page views.
        """

        return (

            df

            .withColumn(

                "is_pageview",

                col("page_path").isNotNull(),

            )

        )

    # ==========================================================================

    def _derive_engagement_flags(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Additional engagement flags.
        """

        return (

            df

            .withColumn(

                "is_entry_page",

                col("is_entrance"),

            )

            .withColumn(

                "is_exit_page",

                col("is_exit"),

            )

            .withColumn(

                "is_engagement_hit",

                col("is_interaction"),

            )

        )

    # ==========================================================================

    def _add_audit_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Standard Gold audit columns.
        """

        return (

            df

            .withColumn(

                "etl_loaded_timestamp",

                current_timestamp(),

            )

            .withColumn(

                "pipeline_name",

                lit("fact_hits"),

            )

            .withColumn(

                "layer",

                lit("gold"),

            )

            .withColumn(

                "job_version",

                lit("1.0.0"),

            )

            .withColumn(

                "created_by",

                lit("spark_pipeline"),

            )

        )

    # ==========================================================================

    def _apply_business_rules(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Execute all helper transformations.
        """

        df = self._rename_columns(df)

        df = self._standardize_nulls(df)

        df = self._derive_hit_category(df)

        df = self._derive_transaction_flag(df)

        df = self._derive_event_flag(df)

        df = self._derive_pageview_flag(df)

        df = self._derive_engagement_flags(df)

        df = self._add_audit_columns(df)

        return df

    # ==========================================================================
    # Transform
    # ==========================================================================

    def transform(
        self,
        dataframes: dict[str, DataFrame],
    ) -> DataFrame:

        self.logger.info(
            "Building Hit Fact..."
        )

        hits_df = dataframes[
            "stg_hits"
        ]

        fact_df = self._apply_business_rules(
            hits_df
        )

        fact_df = (

            fact_df

            .select(

                # ==========================================================
                # Keys
                # ==========================================================

                col("hit_key"),

                col("session_id"),

                col("visitor_id"),

                col("session_visit_id"),

                col("visit_date"),

                # ==========================================================
                # Hit Information
                # ==========================================================

                col("hit_number"),

                col("hit_time"),

                col("hit_hour"),

                col("hit_minute"),

                col("hit_type"),

                col("hit_category"),

                # ==========================================================
                # Page Information
                # ==========================================================

                col("page_path"),

                col("page_title"),

                col("hostname"),

                col("page_path_level1"),

                col("page_path_level2"),

                col("page_path_level3"),

                col("page_search_keyword"),

                # ==========================================================
                # Event Information
                # ==========================================================

                col("event_category"),

                col("event_action"),

                col("event_label"),

                col("event_value"),

                # ==========================================================
                # Ecommerce Information
                # ==========================================================

                col("ecommerce_action_type"),

                col("ecommerce_step"),

                col("ecommerce_option"),

                # ==========================================================
                # Transaction Information
                # ==========================================================

                col("transaction_id"),

                col("transaction_affiliation"),

                col("transaction_revenue"),

                col("transaction_shipping"),

                col("transaction_tax"),

                # ==========================================================
                # Flags
                # ==========================================================

                col("is_pageview"),

                col("is_event"),

                col("is_transaction"),

                col("is_interaction"),

                col("is_entry_page"),

                col("is_exit_page"),

                col("is_engagement_hit"),

                # ==========================================================
                # Miscellaneous
                # ==========================================================

                col("referer"),

                col("data_source"),

                col("content_group1"),

                col("content_group2"),

                col("content_group3"),

                # ==========================================================
                # Audit Columns
                # ==========================================================

                col("etl_loaded_timestamp"),

                col("pipeline_name"),

                col("layer"),

                col("job_version"),

                col("created_by"),

            )

        )

        self.logger.info(
            "Hit Fact transformation completed."
        )

        return fact_df

    # ==========================================================================
    # Validate
    # ==========================================================================

    def validate(
        self,
        target_df: DataFrame,
    ) -> None:

        self.logger.info(
            "Running Hit Fact validations..."
        )

        ValidationSuite(

            required_columns=self.config.required_columns,

            key_columns=self.config.key_columns,

            non_negative_columns=self.config.non_negative_columns,

            minimum_rows=self.config.min_rows,

        ).run(target_df)

        self.logger.info(
            "Hit Fact validation completed."
        )

    # ==========================================================================
    # Load
    # ==========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:

        self.logger.info(
            "Writing Hit Fact table..."
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

    spark = get_spark(
        app_name="fact_hits",
    )

    pipeline = FactHitsPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()