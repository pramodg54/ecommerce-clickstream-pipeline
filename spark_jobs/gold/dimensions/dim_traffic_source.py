"""
===============================================================================
File Name : dim_traffic_source.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Traffic Source Dimension.
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


class DimTrafficSourcePipeline(BaseGoldPipeline):

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(
            spark=spark,
            pipeline_name="dim_traffic_source",
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
        Build Traffic Source Dimension.

        One Row = One Traffic Source Combination
        """

        self.logger.info(
            "Building Traffic Source Dimension..."
        )

        traffic_df = dataframes[
            "stg_traffic"
        ]

        traffic_source_df = (

            traffic_df

            .select(

                "traffic_source",

                "traffic_medium",

                "campaign",

                "campaign_code",

                "keyword",

                "ad_content",

                "referral_path",

            )

            .dropDuplicates()

        )

        # ------------------------------------------------------------------
        # Surrogate Key
        # ------------------------------------------------------------------

        traffic_source_df = (

            traffic_source_df

            .withColumn(

                "traffic_source_key",

                xxhash64(

                    col("traffic_source"),

                    col("traffic_medium"),

                    col("campaign"),

                    col("campaign_code"),

                    col("keyword"),

                    col("ad_content"),

                    col("referral_path"),

                )

            )

        )

        # ------------------------------------------------------------------
        # Audit Column
        # ------------------------------------------------------------------

        traffic_source_df = (

            traffic_source_df

            .withColumn(

                "created_at",

                current_timestamp()

            )

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        traffic_source_df = (

            traffic_source_df

            .select(

                "traffic_source_key",

                "traffic_source",

                "traffic_medium",

                "campaign",

                "campaign_code",

                "keyword",

                "ad_content",

                "referral_path",

                "created_at",

            )

        )

        self.logger.info(
            "Traffic Source Dimension transformation completed."
        )

        return traffic_source_df
    
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
            "Running Traffic Source Dimension validations..."
        )

        ValidationSuite(

            required_columns=[
                "traffic_source_key",
                "traffic_source",
                "traffic_medium",
            ],

            key_columns=[
                "traffic_source_key",
            ],

            minimum_rows=1,

        ).run(target_df)

        self.logger.info(
            "Traffic Source Dimension validation completed."
        )

    # =========================================================================
    # Load
    # =========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Traffic Source Dimension.
        """

        self.logger.info(
            "Writing Traffic Source Dimension..."
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
        app_name="dim_traffic_source"
    )

    pipeline = DimTrafficSourcePipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()