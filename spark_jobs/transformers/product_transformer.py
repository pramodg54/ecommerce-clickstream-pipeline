"""
===============================================================================
File Name : product_transformer.py
Layer     : Gold
Purpose   : Business transformations for Product Dimension
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ProductTransformer:
    """
    Product business transformation utilities.

    This class contains only business transformations.
    No reading, writing, validation or metadata logic should exist here.
    """

    @staticmethod
    def clean(df: DataFrame) -> DataFrame:
        """
        Execute complete product transformation pipeline.
        """

        return (
            df.transform(ProductTransformer.normalize_strings)
              .transform(ProductTransformer.handle_nulls)
              .transform(ProductTransformer.calculate_metrics)
              .transform(ProductTransformer.select_columns)
        )

    @staticmethod
    def normalize_strings(df: DataFrame) -> DataFrame:
        """
        Standardize string columns.
        """

        string_columns = [
            "product_name",
            "product_brand",
            "product_category",
            "product_variant",
            "product_sku",
        ]

        for column in string_columns:

            if column in df.columns:

                df = df.withColumn(
                    column,
                    F.trim(F.col(column))
                )

        return df

    @staticmethod
    def handle_nulls(df: DataFrame) -> DataFrame:
        """
        Fill common null values.
        """

        defaults = {
            "product_brand": "Unknown",
            "product_category": "Unknown",
            "product_variant": "Unknown",
            "product_revenue": 0,
            "product_quantity": 0,
        }

        return df.fillna(defaults)

    @staticmethod
    def calculate_metrics(df: DataFrame) -> DataFrame:
        """
        Calculate derived product columns.
        """

        if (
            "product_revenue" in df.columns
            and "product_quantity" in df.columns
        ):

            df = df.withColumn(

                "unit_price",

                F.when(
                    F.col("product_quantity") > 0,

                    F.round(

                        F.col("product_revenue")
                        /
                        F.col("product_quantity"),

                        2,

                    )

                ).otherwise(F.lit(0))

            )

        return df

    @staticmethod
    def select_columns(df: DataFrame) -> DataFrame:
        """
        Select standardized columns.
        """

        required_columns = [

            "product_sku",

            "product_name",

            "product_brand",

            "product_category",

            "product_variant",

            "product_quantity",

            "product_revenue",

            "unit_price",

            "session_id",

            "visit_date",

        ]

        available = [

            column

            for column in required_columns

            if column in df.columns

        ]

        return df.select(*available)