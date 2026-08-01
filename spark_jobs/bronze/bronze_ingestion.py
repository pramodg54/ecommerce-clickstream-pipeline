"""
===============================================================================
Bronze Ingestion
===============================================================================

Orchestrates the Bronze layer by:

1. Reading all Bronze Delta tables
2. Validating the unified dataset
3. Returning the validated DataFrame

This DataFrame becomes the source for all Silver transformations.
"""

from pyspark.sql import DataFrame

from spark_jobs.bronze.bronze_reader import read_all_tables
from spark_jobs.bronze.bronze_validator import validate_bronze
from spark_jobs.common.logger import get_logger

logger = get_logger("bronze_ingestion")


def run(cache: bool = True) -> DataFrame:
    """
    Execute Bronze ingestion.

    Parameters
    ----------
    cache : bool
        Cache the unified Bronze DataFrame.

    Returns
    -------
    DataFrame
        Validated Bronze DataFrame.
    """

    logger.info("=" * 70)
    logger.info("Starting Bronze Ingestion")
    logger.info("=" * 70)

    bronze_df = read_all_tables(cache=cache)

    validate_bronze(bronze_df)

    logger.info(
        "Bronze ingestion completed successfully."
    )

    logger.info(
        "Rows : %s",
        format(bronze_df.count(), ","),
    )

    logger.info(
        "Columns : %d",
        len(bronze_df.columns),
    )

    return bronze_df


if __name__ == "__main__":

    df = run()

    print(f"Rows : {df.count():,}")