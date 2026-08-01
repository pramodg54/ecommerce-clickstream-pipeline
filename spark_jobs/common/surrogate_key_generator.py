"""
===============================================================================
File Name : surrogate_key_generator.py
Purpose   : Generate deterministic surrogate keys for Gold tables.
===============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
)


class SurrogateKeyGenerator:

    """
    Utility class for generating surrogate keys.
    """

    @staticmethod
    def generate(
        df: DataFrame,
        key_name: str,
        business_columns: list[str],
    ) -> DataFrame:
        """
        Create a SHA-256 surrogate key.

        Parameters
        ----------
        df
            Source dataframe.

        key_name
            Name of surrogate key column.

        business_columns
            Columns used to build the business key.

        Returns
        -------
        DataFrame
        """

        return df.withColumn(

            key_name,

            sha2(

                concat_ws(
                    "||",
                    *[
                        col(c)
                        for c in business_columns
                    ],
                ),

                256,

            ),

        )