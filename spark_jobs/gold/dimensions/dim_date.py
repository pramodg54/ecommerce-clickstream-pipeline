"""
===============================================================================
File Name : dim_date.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Date Dimension from Silver Sessions.
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofmonth,
    quarter,
    weekofyear,
    dayofweek,
    date_format,
    current_timestamp,
    lit,
    last_day,
)

from spark_jobs.gold.framework.base_gold_pipeline import (
    BaseGoldPipeline,
)

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.validations import ValidationSuite


class DimDatePipeline(BaseGoldPipeline):

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(
            spark=spark,
            pipeline_name="dim_date",
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
        Build Date Dimension.

        One Row = One Calendar Date
        """

        self.logger.info(
            "Building Date Dimension..."
        )

        sessions_df = dataframes[
            "stg_sessions"
        ]

        # ------------------------------------------------------------------
        # Extract Distinct Dates
        # ------------------------------------------------------------------

        date_df = (

            sessions_df

            .select(

                to_date(

                    col("visit_date").cast("string"),

                    "yyyyMMdd",

                ).alias("calendar_date")

            )

            .filter(

                col("calendar_date").isNotNull()

            )

            .dropDuplicates()

        )

        # ------------------------------------------------------------------
        # Calendar Attributes
        # ------------------------------------------------------------------

        date_df = (

            date_df

            .withColumn(

                "date_key",

                date_format(

                    col("calendar_date"),

                    "yyyyMMdd",

                ).cast("integer")

            )

            .withColumn(

                "year",

                year("calendar_date")

            )

            .withColumn(

                "quarter",

                quarter("calendar_date")

            )

            .withColumn(

                "month",

                month("calendar_date")

            )

            .withColumn(

                "month_name",

                date_format(

                    col("calendar_date"),

                    "MMMM",

                )

            )

            .withColumn(

                "week_of_year",

                weekofyear("calendar_date")

            )

            .withColumn(

                "day",

                dayofmonth("calendar_date")

            )

            .withColumn(

                "day_of_week",

                dayofweek("calendar_date")

            )

            .withColumn(

                "day_name",

                date_format(

                    col("calendar_date"),

                    "EEEE",

                )

            )

        )

        # ------------------------------------------------------------------
        # Derived Flags
        # ------------------------------------------------------------------

        date_df = (

            date_df

            .withColumn(

                "is_weekend",

                col("day_of_week").isin(1, 7)

            )

            .withColumn(

                "is_month_start",

                col("day") == 1

            )

            .withColumn(

                "is_month_end",

                last_day(
                    col("calendar_date")
                ) == col("calendar_date")

            )

        )

        # ------------------------------------------------------------------
        # Audit Columns
        # ------------------------------------------------------------------

        date_df = (

            date_df

            .withColumn(

                "etl_loaded_timestamp",

                current_timestamp()

            )

            .withColumn(

                "pipeline_name",

                lit("dim_date")

            )

            .withColumn(

                "layer",

                lit("gold")

            )

            .withColumn(

                "job_version",

                lit("1.0.0")

            )

            .withColumn(

                "created_by",

                lit("Data Engineering")

            )

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        date_df = (

            date_df

            .select(

                "date_key",

                "calendar_date",

                "year",

                "quarter",

                "month",

                "month_name",

                "week_of_year",

                "day",

                "day_of_week",

                "day_name",

                "is_weekend",

                "is_month_start",

                "is_month_end",

                "etl_loaded_timestamp",

                "pipeline_name",

                "layer",

                "job_version",

                "created_by",

            )

            .orderBy(

                "calendar_date"

            )

        )

        self.logger.info(
            "Date Dimension transformation completed."
        )

        return date_df
    
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
            "Running Date Dimension validations..."
        )

        ValidationSuite(

            required_columns=[

                "date_key",

                "calendar_date",

                "year",

                "month",

            ],

            key_columns=[

                "date_key",

            ],

            non_negative_columns=[

                "year",

                "quarter",

                "month",

                "week_of_year",

                "day",

            ],

            minimum_rows=1,

        ).run(target_df)

        self.logger.info(
            "Date Dimension validation completed."
        )

    # =========================================================================
    # Load
    # =========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Date Dimension.
        """

        self.logger.info(
            "Writing Date Dimension..."
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
        app_name="dim_date",
    )

    pipeline = DimDatePipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()