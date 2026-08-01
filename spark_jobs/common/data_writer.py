"""
===============================================================================
File Name : data_writer.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Generic DataFrame writer for Delta, Parquet and CSV.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from delta.tables import DeltaTable

from spark_jobs.common.exceptions import (
    DataWriteException,
)

from spark_jobs.common.pipeline_constants import (
    DELTA,
    PARQUET,
    CSV,
)


class DataWriter:

    """
    Generic DataFrame writer.
    """

    def __init__(self, spark):

        self.spark = spark

    # ==========================================================================
    # Generic Write
    # ==========================================================================

    def write(
        self,
        df: DataFrame,
        output_path: str,
        file_format: str,
        mode: str,
        partition_columns: list[str] | None = None,
    ) -> None:

        try:

            writer = (
                df.write
                .format(file_format)
            )

            # ----------------------------------------------------------
            # Delta Incremental Partition Overwrite
            # ----------------------------------------------------------

            if file_format == DELTA:

                self.spark.conf.set(
                    "spark.sql.sources.partitionOverwriteMode",
                    "dynamic",
                )

                writer = (
                    writer
                    .mode("overwrite")
                    .option("overwriteSchema", "false")
                )

            else:

                writer = writer.mode(mode)

            # ----------------------------------------------------------

            if partition_columns:

                writer = writer.partitionBy(*partition_columns)

            writer.save(str(output_path))

        except Exception as exc:

            raise DataWriteException(
                f"Failed writing {file_format} to {output_path}"
            ) from exc
    # ==========================================================================
    # Delta
    # ==========================================================================

    def write_delta(
        self,
        df: DataFrame,
        output_path: str,
        mode: str,
        partition_columns: list[str] | None = None,
    ) -> None:

        self.write(
            df=df,
            output_path=output_path,
            file_format=DELTA,
            mode=mode,
            partition_columns=partition_columns,
        )

    # ==========================================================================
    # Parquet
    # ==========================================================================

    def write_parquet(
        self,
        df: DataFrame,
        output_path: str,
        mode: str,
        partition_columns: list[str] | None = None,
    ) -> None:

        self.write(
            df=df,
            output_path=output_path,
            file_format=PARQUET,
            mode=mode,
            partition_columns=partition_columns,
        )

    # ==========================================================================
    # CSV
    # ==========================================================================

    def write_csv(
        self,
        df: DataFrame,
        output_path: str,
        mode: str,
        header: bool = True,
    ) -> None:

        try:

            (
                df.write
                .format(CSV)
                .option("header", header)
                .mode(mode)
                .save(str(output_path))
            )

        except Exception as exc:

            raise DataWriteException(
                f"Failed writing CSV to {output_path}"
            ) from exc

    # ==========================================================================
    # Verification
    # ==========================================================================

    def verify_write(
        self,
        output_path: str,
        file_format: str = DELTA,
    ) -> int:

        try:

            df = (
                self.spark.read
                .format(file_format)
                .load(str(output_path))
            )

            if df.rdd.isEmpty():

                raise DataWriteException(
                    f"No data found after writing {output_path}"
                )

            return df.count()

        except Exception as exc:

            raise DataWriteException(
                f"Write verification failed for {output_path}"
            ) from exc

    # ==========================================================================
    # Exists
    # ==========================================================================

    def exists(
        self,
        output_path: str,
        file_format: str = DELTA,
    ) -> bool:

        try:

            (
                self.spark.read
                .format(file_format)
                .load(str(output_path))
            )

            return True

        except Exception:

            return False

    # ==========================================================================
    # Delete
    # ==========================================================================

    def delete(
        self,
        output_path: str,
    ) -> None:

        jvm = self.spark._jvm

        fs = (
            jvm.org.apache.hadoop.fs.FileSystem
            .get(self.spark._jsc.hadoopConfiguration())
        )

        path = jvm.org.apache.hadoop.fs.Path(str(output_path))

        if fs.exists(path):

            fs.delete(path, True)

    # ==========================================================================
    # Vacuum
    # ==========================================================================

    def vacuum(
        self,
        output_path: str,
        retention_hours: int = 168,
    ) -> None:

        table = DeltaTable.forPath(
            self.spark,
            str(output_path),
        )

        table.vacuum(retention_hours)

    # ==========================================================================
    # Optimize
    # ==========================================================================

    def optimize(
        self,
        output_path: str,
    ) -> None:

        self.spark.sql(

            f"""
            OPTIMIZE delta.`{str(output_path)}`
            """

        )