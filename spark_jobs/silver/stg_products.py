"""
===============================================================================
File Name : stg_products.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Silver
Purpose   : Flatten Product records into Silver Delta table.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

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
from pathlib import Path


class StgProductsPipeline(BasePipeline):

    """
    -----------------------------------------------------------------------
    Bronze

        ↓

    Explode Hits

        ↓

    Explode Products

        ↓

    One Row = One Product

        ↓

    Validation

        ↓

    Silver Delta
    -----------------------------------------------------------------------
    """

    def __init__(self, spark):

        logger = get_logger(self.__class__.__name__)

        super().__init__(
            spark=spark,
            logger=logger,
            pipeline_name="stg_products",
        )

    # ==========================================================================
    # Extract
    # ==========================================================================

    def discover(self):

        self.logger.info(
            "Discovering Bronze Delta tables..."
        )

        tables = self.reader.list_delta_tables(BRONZE_PATH)

        self.logger.info(
            f"Discovered {len(tables)} Bronze tables."
        )

        return tables


    def extract(
        self,
        work_item,
    ) -> DataFrame:

        self.logger.info(
            f"Reading Bronze table: {Path(work_item).name}"
        )

        return self.reader.read_delta(work_item)

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

    def _explode_products(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "product",
                explode_outer(
                    col("hit.product")
                ),
            )

        )

    def _create_keys(
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

            .withColumn(
                "hit_number",
                col("hit.hitNumber"),
            )

            .withColumn(

                "product_key",

                concat_ws(

                    "_",

                    col("fullVisitorId"),

                    col("visitId"),

                    col("hit.hitNumber"),

                    col("product.productSKU"),

                ),

            )

        )

    def _product_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        return (

            df

            .withColumn(
                "product_sku",
                col("product.productSKU"),
            )

            .withColumn(
                "product_name",
                col("product.v2ProductName"),
            )

            .withColumn(
                "product_brand",
                col("product.productBrand"),
            )

            .withColumn(
                "product_category",
                col("product.v2ProductCategory"),
            )

            .withColumn(
                "product_variant",
                col("product.productVariant"),
            )

            .withColumn(
                "currency_code",
                lit(None).cast("string"),
            )

            .withColumn(

                "product_price",

                (
                    coalesce(
                        col("product.productPrice"),
                        lit(0)
                    ) / 1_000_000
                ).cast("double"),

            )

            .withColumn(

                "local_product_price",

                (
                    coalesce(
                        col("product.localProductPrice"),
                        lit(0)
                    ) / 1_000_000
                ).cast("double"),

            )

            .withColumn(
                "quantity",
                coalesce(
                    col("product.productQuantity"),
                    lit(0),
                ),
            )

            .withColumn(

                "product_revenue",

                (
                    coalesce(
                        col("product.productRevenue"),
                        lit(0)
                    ) / 1_000_000
                ).cast("double"),

            )

            .withColumn(

                "refund_amount",

                (
                    coalesce(
                        col("product.productRefundAmount"),
                        lit(0)
                    ) / 1_000_000
                ).cast("double"),

            )

            .withColumn(
                "is_impression",
                col("product.productListPosition").isNotNull(),
            )

            .withColumn(
                "coupon_code",
                col("product.productCouponCode").cast("string"),
            )

        )
# ==============================================================================
# Transform
# ==============================================================================

    def transform(
        self,
        source_df: DataFrame,
    ) -> DataFrame:
        """
        Transform Bronze clickstream data into a Product-level Silver table.

        One Row = One Product
        """

        self.logger.info(
            "Transforming product data..."
        )

        silver_df = source_df

    # --------------------------------------------------------------------------
    # Explode Nested Arrays
    # --------------------------------------------------------------------------

        silver_df = self._explode_hits(
            silver_df
        )

        silver_df = self._explode_products(
            silver_df
        )

    # --------------------------------------------------------------------------
    # Business Keys
    # --------------------------------------------------------------------------

        silver_df = self._create_keys(
            silver_df
        )

    # --------------------------------------------------------------------------
    # Flatten Product Attributes
    # --------------------------------------------------------------------------

        silver_df = self._product_columns(
            silver_df
        )

    # --------------------------------------------------------------------------
    # Session Attributes
    # --------------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "user_id",
                col("fullVisitorId"),
            )

            .withColumn(
                "visit_id",
                col("visitId"),
            )

            .withColumn(
                "visit_date",
                col("date"),
            )

            .withColumn(
                "channel_grouping",
                col("channelGrouping"),
            )

            .withColumn(
                "traffic_source",
                col("trafficSource.source"),
            )

            .withColumn(
                "traffic_medium",
                col("trafficSource.medium"),
            )

            .withColumn(
                "device_category",
                col("device.deviceCategory"),
            )

            .withColumn(
                "country",
                col("geoNetwork.country"),
            )

        )

    # --------------------------------------------------------------------------
    # Hit Attributes
    # --------------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "hit_type",
                col("hit.type"),
            )

            .withColumn(
                "hit_time",
                col("hit.time"),
            )

            .withColumn(
                "page_path",
                col("hit.page.pagePath"),
            )

            .withColumn(
                "transaction_id",
                col("hit.transaction.transactionId").cast("string"),
            )

            .withColumn(
                "ecommerce_action_type",
                col("hit.eCommerceAction.action_type"),
            )

        )

        # --------------------------------------------------------------------------
        # Product Metrics
        # --------------------------------------------------------------------------

        silver_df = (

            silver_df

            .withColumn(
                "product_list_name",
                col("product.productListName"),
            )

            .withColumn(
                "product_list_position",
                col("product.productListPosition"),
            )

            .withColumn(
                "promotion_name",
                lit(None).cast("string"),
            )

            .withColumn(
                "promotion_id",
                lit(None).cast("string"),
            )

        )

        # --------------------------------------------------------------------------
        # Remove Invalid Products
        # --------------------------------------------------------------------------

        silver_df = (

            silver_df

            .filter(
                col("product_sku").isNotNull()
            )

        )

    # --------------------------------------------------------------------------
    # Final Projection
    # --------------------------------------------------------------------------

        silver_df = (

            silver_df

            .select(

                # Keys
                "product_key",
                "session_id",
                "user_id",
                "visit_id",
                "hit_number",

                # Product
                "product_sku",
                "product_name",
                "product_brand",
                "product_category",
                "product_variant",

                # Pricing
                "currency_code",
                "product_price",
                "local_product_price",
                "quantity",
                "product_revenue",
                "refund_amount",

                # Product Behaviour
                "coupon_code",
                "is_impression",
                "product_list_name",
                "product_list_position",

                # Promotion
                "promotion_name",
                "promotion_id",

                # Session
                "visit_date",
                "channel_grouping",
                "traffic_source",
                "traffic_medium",
                "device_category",
                "country",

                # Hit
                "hit_type",
                "hit_time",
                "page_path",
                "transaction_id",
                "ecommerce_action_type",

            )

        )

        self.logger.info(
            "Product transformation completed."
        )

        silver_df = silver_df.dropDuplicates()

        return silver_df
# ==============================================================================
# Validate
# ==============================================================================

    def validate(
        self,
        target_df: DataFrame,
    ) -> None:
        """
        Execute standard framework validations.
        """

        self.logger.info(
            "Running Product validations..."
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
        Write Product Silver table.
        """

        self.logger.info(
            "Writing Product Silver table..."
        )

        print("=" * 80)
        print("TARGET DATAFRAME SCHEMA")
        print("=" * 80)

        target_df.printSchema()

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
        app_name="stg_products"
    )

    pipeline = StgProductsPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()