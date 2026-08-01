"""
===============================================================================
File Name : fact_sessions.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Session Fact Table.
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from pyspark.sql.functions import (
    col,
    xxhash64,
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


class FactSessionsPipeline(BaseGoldPipeline):

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(

            spark=spark,

            pipeline_name="fact_sessions",

        )

    # ==========================================================================
    # Extract
    # ==========================================================================

    def extract(
        self,
    ) -> dict[str, DataFrame]:

        self.logger.info(
            "Reading Silver Session table..."
        )

        return self.read_multiple_silver_tables(

            [
                "stg_sessions",
            ]

        )

    # ==========================================================================
    # Transform
    # ==========================================================================

    def transform(
        self,
        dataframes: dict[str, DataFrame],
    ) -> DataFrame:

        self.logger.info(
            "Building Session Fact..."
        )

        sessions_df = dataframes[
            "stg_sessions"
        ]

        fact_df = (

            sessions_df

            .select(

                xxhash64(
                    col("session_id")
                ).alias(
                    "session_key"
                ),

                col("session_id"),

                col("user_id"),

                col("visit_id"),

                col("visit_number"),

                col("visit_start_time"),

                col("visit_date"),

                col("channel_grouping"),

                col("traffic_source"),

                col("traffic_medium"),

                col("device_category"),

                col("browser"),

                col("operating_system"),

                col("country"),

                col("city"),

                col("page_views"),

                col("hits"),

                col("transactions"),

                col("transaction_revenue"),

                col("bounces"),

                col("new_visits"),

                col("time_on_site"),

                col("is_mobile"),

                current_timestamp().alias(
                    "created_at"
                ),

            )

        )

        self.logger.info(
            "Session Fact transformation completed."
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
            "Running Session Fact validations..."
        )

        ValidationSuite(

            required_columns=self.config.required_columns,

            key_columns=self.config.key_columns,

            non_negative_columns=self.config.non_negative_columns,

            minimum_rows=self.config.min_rows,

        ).run(target_df)

        self.logger.info(
            "Session Fact validation completed."
        )

    # ==========================================================================
    # Load
    # ==========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:

        self.logger.info(
            "Writing Session Fact table..."
        )

        self.writer.write_delta(

            df=target_df,

            output_path=self.config.output_path,

            mode=self.config.write_mode,

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
        app_name="fact_sessions",
    )

    pipeline = FactSessionsPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()