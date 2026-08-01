"""
===============================================================================
Utility Functions
===============================================================================
Reusable helper functions used across the pipeline.
"""

from pyspark.sql import DataFrame


def log_row_count(df: DataFrame, title: str) -> None:
    """
    Print the number of rows in a DataFrame.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    title : str
        Description displayed with the row count.
    """

    print(f"{title}: {df.count():,} rows")


def print_schema(df: DataFrame) -> None:
    """
    Print the schema of a DataFrame.
    """

    df.printSchema()


def show_sample(
    df: DataFrame,
    rows: int = 5,
    truncate: bool = False,
) -> None:
    """
    Display sample rows.

    Parameters
    ----------
    rows : int
        Number of rows.

    truncate : bool
        Whether to truncate long columns.
    """

    df.show(rows, truncate=truncate)