"""
===============================================================================
File Name : dim_device.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Device Dimension from Silver Traffic.
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

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.validations import ValidationSuite


class DimDevicePipeline(BaseGoldPipeline):

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(
            spark=spark,
            pipeline_name="dim_device",
        )

    # =========================================================================
    # Extract
    # =========================================================================

    def extract(
        self,
    ) -> dict[str, DataFrame]:

        return self.read_multiple_silver_tables(

            [
                "stg_traffic",
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
        Build Device Dimension.

        One Row = One Device Configuration
        """

        self.logger.info(
            "Building Device Dimension..."
        )

        traffic_df = dataframes[
            "stg_traffic"
        ]

        device_df = (

            traffic_df

            .select(

                "browser",

                "browser_version",

                "operating_system",

                "device_category",

                "mobile_device_brand",

                "mobile_device_model",

                "language",

            )

            .dropDuplicates()

        )

        # ------------------------------------------------------------------
        # Derived Columns
        # ------------------------------------------------------------------

        device_df = (

            device_df

            .withColumn(

                "is_mobile",

                (
                    col(
                        "device_category"
                    ) == "mobile"
                )

            )

        )

        # ------------------------------------------------------------------
        # Surrogate Key
        # ------------------------------------------------------------------

        device_df = (

            device_df

            .withColumn(

                "device_key",

                xxhash64(

                    col("browser"),

                    col("browser_version"),

                    col("operating_system"),

                    col("device_category"),

                    col("mobile_device_brand"),

                    col("mobile_device_model"),

                    col("language"),

                )

            )

        )

        # ------------------------------------------------------------------
        # Audit Column
        # ------------------------------------------------------------------

        device_df = (

            device_df

            .withColumn(

                "created_at",

                current_timestamp()

            )

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        device_df = (

            device_df

            .select(

                "device_key",

                "browser",

                "browser_version",

                "operating_system",

                "device_category",

                "mobile_device_brand",

                "mobile_device_model",

                "language",

                "is_mobile",

                "created_at",

            )

        )

        self.logger.info(
            "Device Dimension transformation completed."
        )

        return device_df

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
            "Running Device Dimension validations..."
        )

        ValidationSuite(

            required_columns=[
                "device_key",
                "browser",
                "operating_system",
                "device_category",
            ],

            key_columns=[
                "device_key",
            ],

            minimum_rows=1,

        ).run(target_df)

        self.logger.info(
            "Device Dimension validation completed."
        )

    # =========================================================================
    # Load
    # =========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Device Dimension.
        """

        self.logger.info(
            "Writing Device Dimension..."
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
        app_name="dim_device"
    )

    pipeline = DimDevicePipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()