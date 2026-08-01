"""
===============================================================================
File Name : dim_user.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build User Dimension from Silver Sessions.
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from pyspark.sql.functions import (
    col,
    min,
    max,
    countDistinct,
    sum,
    coalesce,
    lit,
    xxhash64,
    current_timestamp,
    to_date,
)

from spark_jobs.gold.framework.base_gold_pipeline import (
    BaseGoldPipeline,
)

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.validations import ValidationSuite


class DimUserPipeline(BaseGoldPipeline):

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(
            spark=spark,
            pipeline_name="dim_user",
        )

    # =========================================================================
    # Extract
    # =========================================================================

    def extract(
        self,
    ) -> dict[str, DataFrame]:
        """
        Read required Silver tables.
        """

        self.logger.info(
            "Reading Silver Sessions..."
        )

        return self.read_multiple_silver_tables(
            [
                "stg_sessions",
            ]
        )

    # =========================================================================
    # Transform
    # =========================================================================

    def transform(
        self,
        dataframes: dict[str, DataFrame],
    ) -> DataFrame:
        """
        Build User Dimension.

        One Row = One User
        """

        self.logger.info(
            "Building User Dimension..."
        )

        sessions_df = dataframes[
            "stg_sessions"
        ]

        # ------------------------------------------------------------------
        # Build User Metrics
        # ------------------------------------------------------------------

        user_df = (

            sessions_df

            .filter(
                col("user_id").isNotNull()
            )

            .groupBy(
                "user_id"
            )

            .agg(

                min(

                    to_date(

                        col("visit_date").cast("string"),

                        "yyyyMMdd",

                    )

                ).alias(
                    "first_visit_date"
                ),

                max(

                    to_date(

                        col("visit_date").cast("string"),

                        "yyyyMMdd",

                    )

                ).alias(
                    "last_visit_date"
                ),

                countDistinct(
                    "session_id"
                ).alias(
                    "total_sessions"
                ),

                coalesce(

                    sum(
                        "transaction_revenue"
                    ),

                    lit(0.0),

                ).alias(
                    "lifetime_revenue"
                ),

            )

        )

        # ------------------------------------------------------------------
        # Derived Columns
        # ------------------------------------------------------------------

        user_df = (

            user_df

            .withColumn(

                "is_returning_customer",

                col("total_sessions") > 1,

            )

        )

        # ------------------------------------------------------------------
        # Surrogate Key
        # ------------------------------------------------------------------

        user_df = (

            user_df

            .withColumn(

                "user_key",

                xxhash64(

                    col("user_id")

                )

            )

        )

        # ------------------------------------------------------------------
        # Audit Columns
        # ------------------------------------------------------------------

        user_df = (

            user_df

            .withColumn(

                "etl_loaded_timestamp",

                current_timestamp(),

            )

            .withColumn(

                "pipeline_name",

                lit("dim_user"),

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

                lit("Data Engineering"),

            )

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        user_df = (

            user_df

            .select(

                "user_key",

                "user_id",

                "first_visit_date",

                "last_visit_date",

                "total_sessions",

                "lifetime_revenue",

                "is_returning_customer",

                "etl_loaded_timestamp",

                "pipeline_name",

                "layer",

                "job_version",

                "created_by",

            )

            .orderBy(

                "user_id"

            )

        )

        self.logger.info(
            "User Dimension transformation completed."
        )

        return user_df
    # =========================================================================
    # Validate
    # =========================================================================

    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Execute framework validations.
        """

        self.logger.info(
            "Running User Dimension validations..."
        )

        ValidationSuite(

            required_columns=[

                "user_key",

                "user_id",

                "first_visit_date",

                "last_visit_date",

                "total_sessions",

                "lifetime_revenue",

            ],

            key_columns=[

                "user_key",

            ],

            non_negative_columns=[

                "total_sessions",

                "lifetime_revenue",

            ],

            minimum_rows=1,

        ).run(target_df)

        self.logger.info(
            "User Dimension validation completed."
        )

    # =========================================================================
    # Load
    # =========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write User Dimension.
        """

        self.logger.info(
            "Writing User Dimension..."
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
        app_name="dim_user",
    )

    pipeline = DimUserPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()