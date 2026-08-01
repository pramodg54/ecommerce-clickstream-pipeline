"""
===============================================================================
File Name : aggregation_helper.py
Purpose   : Common aggregation utilities for Gold pipelines.
===============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class AggregationHelper:
    """
    Reusable aggregation methods for Gold layer.
    """

    @staticmethod
    def build_user_metrics(df: DataFrame) -> DataFrame:
        """
        Aggregate user-level metrics.
        """

        return (

            df

            .groupBy("user_id")

            .agg(

                F.countDistinct("session_id").alias(
                    "total_sessions"
                ),

                F.sum("transaction_revenue").alias(
                    "total_revenue"
                ),

                F.sum("total_transactions").alias(
                    "total_transactions"
                ),

                F.min("visit_date").alias(
                    "first_visit_date"
                ),

                F.max("visit_date").alias(
                    "last_visit_date"
                ),

                F.first(
                    "traffic_source",
                    ignorenulls=True
                ).alias(
                    "traffic_source"
                ),

                F.first(
                    "medium",
                    ignorenulls=True
                ).alias(
                    "medium"
                ),

                F.first(
                    "device_category",
                    ignorenulls=True
                ).alias(
                    "device_category"
                ),

                F.first(
                    "country",
                    ignorenulls=True
                ).alias(
                    "country"
                ),

                F.first(
                    "city",
                    ignorenulls=True
                ).alias(
                    "city"
                )

            )

        )