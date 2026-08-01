"""
===============================================================================
File Name : merge_helper.py
Purpose   : Generic Delta MERGE utility
===============================================================================
"""

from delta.tables import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class MergeHelper:

    """
    Generic Delta MERGE helper.
    """

    @staticmethod
    def merge(
        spark: SparkSession,
        source_df: DataFrame,
        target_path: str,
        merge_keys: list[str],
    ) -> None:

        if DeltaTable.isDeltaTable(
            spark,
            target_path,
        ):

            target = DeltaTable.forPath(
                spark,
                target_path,
            )

            condition = " AND ".join(

                [
                    f"target.{c}=source.{c}"
                    for c in merge_keys
                ]

            )

            (

                target.alias("target")

                .merge(

                    source_df.alias("source"),

                    condition,

                )

                .whenMatchedUpdateAll()

                .whenNotMatchedInsertAll()

                .execute()

            )

        else:

            (

                source_df

                .write

                .format("delta")

                .mode("overwrite")

                .save(target_path)

            )