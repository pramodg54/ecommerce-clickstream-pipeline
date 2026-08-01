"""
===============================================================================
Bronze Reader
===============================================================================

Reads and combines all Bronze Delta tables into a unified DataFrame.

Supports schema evolution using unionByName(allowMissingColumns=True).
"""

from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from spark_jobs.common.config import BRONZE_PATH
from spark_jobs.common.logger import get_logger
from spark_jobs.common.spark_session import get_spark

spark = get_spark()

logger = get_logger("bronze_reader")


def discover_tables() -> list[Path]:
    """
    Discover all Bronze Delta tables.
    """

    bronze_root = BRONZE_PATH

    if not bronze_root.exists():
        raise FileNotFoundError(
            f"Bronze directory not found: {bronze_root}"
        )

    tables = sorted(
        [
            table
            for table in bronze_root.iterdir()
            if table.is_dir()
            and table.name.startswith("ga_sessions_")
        ]
    )

    if not tables:
        raise FileNotFoundError(
            f"No Bronze tables found in {bronze_root}"
        )

    logger.info(
        "Discovered %d Bronze tables.",
        len(tables),
    )

    return tables


def read_single_table(path: Path) -> DataFrame:
    """
    Read one Bronze Delta table.
    """

    logger.info("Reading %s", path.name)

    return (
        spark.read
        .format("delta")
        .load(str(path))
        .withColumn("source_table", lit(path.name))
    )


def read_all_tables(cache: bool = True) -> DataFrame:
    """
    Read and combine every Bronze Delta table.

    Parameters
    ----------
    cache : bool
        Cache the resulting DataFrame.
    """

    tables = discover_tables()

    dfs = [
        read_single_table(table)
        for table in tables
    ]

    bronze_df = reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=True,
        ),
        dfs,
    )

    if cache:
        bronze_df = bronze_df.cache()
        bronze_df.count()

    logger.info(
        "Unified Bronze rows : %s",
        format(bronze_df.count(), ","),
    )

    logger.info(
        "Unified Bronze columns : %d",
        len(bronze_df.columns),
    )

    return bronze_df


if __name__ == "__main__":

    df = read_all_tables()

    df.printSchema()

    print(f"Rows : {df.count():,}")