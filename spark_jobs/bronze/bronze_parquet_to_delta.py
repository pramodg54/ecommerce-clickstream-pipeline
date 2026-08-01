"""
===============================================================================
Bronze Parquet → Delta
===============================================================================

Converts every Google Analytics Parquet file into an individual
Delta Lake table.

Input
-----
data/bronze/parquet

Output
------
data/bronze/delta
"""

from pathlib import Path
import shutil

from spark_jobs.common.config import BRONZE_PATH
from spark_jobs.common.logger import get_logger
from spark_jobs.common.spark_session import get_spark

logger = get_logger("bronze_parquet_to_delta")

spark = get_spark()


def ensure_directory(path: Path) -> None:
    """
    Create directory if it does not exist.
    """

    path.mkdir(
        parents=True,
        exist_ok=True,
    )


def remove_existing_delta(path: Path) -> None:
    """
    Remove an existing Delta table.
    """

    if path.exists():

        logger.info(
            "Removing existing table: %s",
            path.name,
        )

        shutil.rmtree(path)


def convert_parquet_to_delta() -> None:
    """
    Convert every Parquet file into Delta format.
    """

    parquet_root = BRONZE_PATH / "parquet"

    delta_root = BRONZE_PATH / "delta"

    ensure_directory(delta_root)

    parquet_files = sorted(
        parquet_root.glob("*.parquet")
    )

    logger.info("=" * 70)
    logger.info("Bronze Parquet → Delta")
    logger.info("=" * 70)

    logger.info(
        "Parquet files discovered : %d",
        len(parquet_files),
    )

    if not parquet_files:

        raise FileNotFoundError(
            f"No parquet files found in {parquet_root}"
        )

    success = 0

    failed = 0

    for index, parquet_file in enumerate(
        parquet_files,
        start=1,
    ):

        logger.info(
            "[%d/%d] %s",
            index,
            len(parquet_files),
            parquet_file.name,
        )

        try:

            df = spark.read.parquet(
                str(parquet_file)
            )

            output_path = (
                delta_root
                / parquet_file.stem
            )

            remove_existing_delta(
                output_path
            )

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(str(output_path))
            )

            verify_rows = (
                spark.read
                .format("delta")
                .load(str(output_path))
                .count()
            )

            logger.info(
                "Rows : %s",
                format(verify_rows, ","),
            )

            success += 1

        except Exception:

            logger.exception(
                "Failed processing %s",
                parquet_file.name,
            )

            failed += 1

    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    logger.info(
        "Processed : %d",
        len(parquet_files),
    )

    logger.info(
        "Success : %d",
        success,
    )

    logger.info(
        "Failed : %d",
        failed,
    )


if __name__ == "__main__":

    convert_parquet_to_delta()