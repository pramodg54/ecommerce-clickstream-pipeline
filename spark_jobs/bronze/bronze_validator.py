"""
===============================================================================
Bronze Validator
===============================================================================

Validation entry point for the unified Bronze DataFrame.
"""

from pyspark.sql import DataFrame

from spark_jobs.common.logger import get_logger
from spark_jobs.common.validations import (
    validate_dataframe_not_empty,
    validate_required_columns,
)

logger = get_logger("bronze_validator")


REQUIRED_COLUMNS = [
    "fullVisitorId",
    "visitId",
    "visitNumber",
    "visitStartTime",
    "date",
    "channelGrouping",
    "device",
    "geoNetwork",
    "totals",
    "trafficSource",
    "hits",
    "source_table",
]


def validate_bronze(df: DataFrame) -> None:
    """
    Execute Bronze-level validation.
    """

    logger.info("=" * 70)
    logger.info("Starting Bronze validation")
    logger.info("=" * 70)

    validate_dataframe_not_empty(df)

    validate_required_columns(
        df,
        REQUIRED_COLUMNS,
    )

    logger.info(
        "Validation successful."
    )

    logger.info(
        "Rows    : %s",
        format(df.count(), ","),
    )

    logger.info(
        "Columns : %d",
        len(df.columns),
    )


if __name__ == "__main__":

    from spark_jobs.bronze.bronze_reader import read_all_tables

    bronze_df = read_all_tables()

    validate_bronze(bronze_df)

    print("Bronze validation completed successfully.")