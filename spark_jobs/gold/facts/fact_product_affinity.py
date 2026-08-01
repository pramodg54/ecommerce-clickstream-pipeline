"""
===============================================================================
File Name : fact_product_affinity.py
Project   : Ecommerce Clickstream Data Pipeline
Layer     : Gold
Purpose   : Build Product Affinity Fact table
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
    collect_set,
    explode,
    array,
    least,
    greatest,
    count,
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


class FactProductAffinityPipeline(BaseGoldPipeline):

    """
    ===========================================================================
                    Silver stg_products
                              │
                              ▼
                 Session Product Collection
                              │
                              ▼
                  Generate Product Pairs
                              │
                              ▼
                  Aggregate Affinity Counts
                              │
                              ▼
               Gold fact_product_affinity
    ===========================================================================
    """

    def __init__(
        self,
        spark: SparkSession,
    ):

        super().__init__(

            spark=spark,

            pipeline_name="fact_product_affinity",

        )

    # ==========================================================================
    # Extract
    # ==========================================================================

    def extract(
        self,
    ) -> dict[str, DataFrame]:

        self.logger.info(
            "Reading Silver Product table..."
        )

        return self.read_multiple_silver_tables(

            [
                "stg_products",
            ]

        )

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def _collect_session_products(
        self,
        df: DataFrame,
    ) -> DataFrame:

        """
        Collect unique products for each session.
        """

        return (

            df

            .filter(
                col("product_sku").isNotNull()
            )

            .groupBy(
                "session_id",
            )

            .agg(

                collect_set(
                    "product_sku"
                ).alias(
                    "products"
                )

            )

        )

    # ==========================================================================

    def _generate_product_pairs(
        self,
        session_products_df: DataFrame,
    ) -> DataFrame:

        """
        Generate unique product pairs for every session.
        """

        left_products = (

            session_products_df

            .select(

                col("session_id"),

                explode(
                    col("products")
                ).alias(
                    "product_sku_1"
                ),

                col("products"),

            )

        )

        right_products = (

            session_products_df

            .select(

                col("session_id"),

                explode(
                    col("products")
                ).alias(
                    "product_sku_2"
                ),

            )

        )

        return (

            left_products.alias("l")

            .join(

                right_products.alias("r"),

                col("l.session_id")
                ==
                col("r.session_id"),

                "inner",

            )

            .filter(

                col("l.product_sku_1")
                <
                col("r.product_sku_2")

            )

            .select(

                col("l.session_id"),

                least(

                    col("l.product_sku_1"),

                    col("r.product_sku_2"),

                ).alias(
                    "product_sku_1"
                ),

                greatest(

                    col("l.product_sku_1"),

                    col("r.product_sku_2"),

                ).alias(
                    "product_sku_2"
                ),

            )

        )

    # ==========================================================================

    def _calculate_affinity(
        self,
        pair_df: DataFrame,
    ) -> DataFrame:

        """
        Aggregate affinity counts.
        """

        return (

            pair_df

            .groupBy(

                "product_sku_1",

                "product_sku_2",

            )

            .agg(

                count(
                    "*"
                ).alias(
                    "pair_count"
                )

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

                lit(
                    "fact_product_affinity"
                ),

            )

            .withColumn(

                "layer",

                lit(
                    "gold"
                ),

            )

            .withColumn(

                "job_version",

                lit(
                    "1.0.0"
                ),

            )

            .withColumn(

                "created_by",

                lit(
                    "spark_pipeline"
                ),

            )

        )

    # ==========================================================================

    def _apply_business_rules(
        self,
        df: DataFrame,
    ) -> DataFrame:

        session_products = self._collect_session_products(
            df
        )

        affinity_pairs = self._generate_product_pairs(
            session_products
        )

        affinity_df = self._calculate_affinity(
            affinity_pairs
        )

        affinity_df = self._add_audit_columns(
            affinity_df
        )

        return affinity_df

    # ==========================================================================
    # Transform
    # ==========================================================================

    def transform(
        self,
        dataframes: dict[str, DataFrame],
    ) -> DataFrame:

        self.logger.info(
            "Building Product Affinity Fact..."
        )

        products_df = dataframes[
            "stg_products"
        ]

        affinity_df = self._apply_business_rules(
            products_df
        )

        affinity_df = (

            affinity_df

            .select(

                # ==========================================================
                # Product Pair
                # ==========================================================

                col("product_sku_1"),

                col("product_sku_2"),

                # ==========================================================
                # Metrics
                # ==========================================================

                col("pair_count"),

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
            "Product Affinity transformation completed."
        )

        return affinity_df

    # ==========================================================================
    # Validate
    # ==========================================================================

    def validate(
        self,
        target_df: DataFrame,
    ) -> None:

        self.logger.info(
            "Running Product Affinity validations..."
        )

        ValidationSuite(

            required_columns=self.config.required_columns,

            key_columns=self.config.key_columns,

            non_negative_columns=self.config.non_negative_columns,

            minimum_rows=self.config.min_rows,

        ).run(target_df)

        self.logger.info(
            "Product Affinity validation completed."
        )

    # ==========================================================================
    # Load
    # ==========================================================================

    def load(
        self,
        target_df: DataFrame,
    ) -> None:

        self.logger.info(
            "Writing Product Affinity table..."
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
        app_name="fact_product_affinity",
    )

    pipeline = FactProductAffinityPipeline(
        spark=spark,
    )

    pipeline.run()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    main()