"""
===============================================================================
File Name : dim_product.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Product Dimension from Silver Products.
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


class DimProductPipeline(BaseGoldPipeline):

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(
            spark=spark,
            pipeline_name="dim_product",
        )

    # =========================================================================
    # Extract
    # =========================================================================

    def extract(
        self,
    ) -> dict[str, DataFrame]:

        return self.read_multiple_silver_tables(

            [
                "stg_products",
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
        Build Product Dimension.

        One Row = One Product
        """

        self.logger.info(
            "Building Product Dimension..."
        )

        product_df = dataframes[
            "stg_products"
        ]


        product_dim_df = (

            product_df

            .select(

                "product_sku",

                "product_name",

                "product_brand",

                "product_category",

                "product_variant",

                "currency_code",

                "product_price",

                "local_product_price",

                "coupon_code",

            )

            .dropDuplicates(["product_sku"])

        )

        # ------------------------------------------------------------------
        # Surrogate Key
        # ------------------------------------------------------------------

        product_dim_df = (

            product_dim_df

            .withColumn(

                "product_key",

                xxhash64(

                    col("product_sku"),

                )

            )

        )

        # ------------------------------------------------------------------
        # Audit Column
        # ------------------------------------------------------------------

        product_dim_df = (

            product_dim_df

            .withColumn(

                "created_at",

                current_timestamp()

            )

        )

        # ------------------------------------------------------------------
        # Final Projection
        # ------------------------------------------------------------------

        product_dim_df = (

            product_dim_df

            .select(

                "product_key",

                "product_sku",

                "product_name",

                "product_brand",

                "product_category",

                "product_variant",

                "currency_code",

                "product_price",

                "local_product_price",

                "coupon_code",

                "created_at",

            )

        )

        self.logger.info(
            "Product Dimension transformation completed."
        )

        return product_dim_df

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
            "Running Product Dimension validations..."
        )

        ValidationSuite(

            required_columns=[
                "product_key",
                "product_sku",
                "product_name",
            ],

            key_columns=[
                "product_key",
            ],

            minimum_rows=1,

        ).run(target_df)

        self.logger.info(
            "Product Dimension validation completed."
        )

    # =========================================================================
    # Load
    # =========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Write Product Dimension.
        """

        self.logger.info(
            "Writing Product Dimension..."
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
        app_name="dim_product"
    )

    pipeline = DimProductPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()