"""
===============================================================================
File Name : data_reader.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Generic data reader for Delta, Parquet, CSV and Bronze Layer
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from spark_jobs.common.exceptions import DataReadException
from spark_jobs.common.pipeline_constants import (
    DELTA,
    PARQUET,
    CSV,
)

from pyspark.sql.functions import (
    col,
    struct,
    lit,
)


class DataReader:
    """
    Generic Data Reader
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ==========================================================================
    # Generic Reader
    # ==========================================================================

    def read(
        self,
        input_path: str,
        file_format: str,
    ) -> DataFrame:

        try:

            return (
                self.spark.read
                .format(file_format)
                .load(input_path)
            )

        except Exception as exc:

            raise DataReadException(
                f"Unable to read {input_path}"
            ) from exc

    # ==========================================================================
    # Delta
    # ==========================================================================

    def read_delta(
        self,
        input_path: str,
    ) -> DataFrame:

        return self.read(
            input_path=input_path,
            file_format=DELTA,
        )

    # ==========================================================================
    # Parquet
    # ==========================================================================

    def read_parquet(
        self,
        input_path: str,
    ) -> DataFrame:

        return self.read(
            input_path=input_path,
            file_format=PARQUET,
        )

    # ==========================================================================
    # CSV
    # ==========================================================================

    def read_csv(
        self,
        input_path: str,
        header: bool = True,
        infer_schema: bool = True,
    ) -> DataFrame:

        try:

            return (
                self.spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .csv(input_path)
            )

        except Exception as exc:

            raise DataReadException(
                f"Unable to read CSV {input_path}"
            ) from exc

    def _normalize_schema(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        Normalize known schema differences across GA export dates.
        """

        return (

            df

            .withColumn(

                "trafficSource",

                struct(

                    col("trafficSource.adContent").alias("adContent"),

                    struct(

                        col("trafficSource.adwordsClickInfo.adGroupId").alias("adGroupId"),
                        col("trafficSource.adwordsClickInfo.adNetworkType").alias("adNetworkType"),
                        col("trafficSource.adwordsClickInfo.campaignId").alias("campaignId"),
                        col("trafficSource.adwordsClickInfo.creativeId").alias("creativeId"),
                        col("trafficSource.adwordsClickInfo.criteriaId").alias("criteriaId"),
                        col("trafficSource.adwordsClickInfo.criteriaParameters").alias("criteriaParameters"),
                        col("trafficSource.adwordsClickInfo.customerId").alias("customerId"),
                        col("trafficSource.adwordsClickInfo.gclId").alias("gclId"),
                        col("trafficSource.adwordsClickInfo.isVideoAd").alias("isVideoAd"),
                        col("trafficSource.adwordsClickInfo.page").alias("page"),
                        col("trafficSource.adwordsClickInfo.slot").alias("slot"),

                        struct(lit(None).cast("int").alias("boomUserlistId")).alias("targetingCriteria"),

                    ).alias("adwordsClickInfo"),

                    col("trafficSource.campaign").alias("campaign"),
                    col("trafficSource.campaignCode").alias("campaignCode"),
                    col("trafficSource.isTrueDirect").alias("isTrueDirect"),
                    col("trafficSource.keyword").alias("keyword"),
                    col("trafficSource.medium").alias("medium"),
                    col("trafficSource.referralPath").alias("referralPath"),
                    col("trafficSource.source").alias("source"),

                ),

            )

        )


    def list_delta_tables(
        self,
        bronze_directory: str,
    ) -> list[str]:
        """
        Return valid Bronze Delta tables.
        """

        tables = []

        for path in sorted(Path(bronze_directory).iterdir()):

            if not path.is_dir():
                continue

            delta_log = path / "_delta_log"

            # Skip if _delta_log doesn't exist
            if not delta_log.exists():
                continue

            # Skip incomplete Delta tables
            if not any(delta_log.iterdir()):
                self.spark._jvm.org.apache.log4j.LogManager\
                    .getLogger(__name__)\
                    .warn(f"Skipping incomplete Delta table: {path}")
                continue

            tables.append(str(path))

        return tables       

    # ==========================================================================
    # Read Multiple Delta Tables
    # ==========================================================================

    def read_multiple_delta(
        self,
        paths: list[str],
        cache: bool = False,
    ) -> DataFrame:

        dataframe = None

        print("\n" + "=" * 80)
        print("Starting Bronze Delta Read")
        print("=" * 80)

        for index, path in enumerate(sorted(paths), start=1):

            print(f"\n[{index}] Reading Delta Table:")
            print(path)

            df = self.read_delta(path)

            df = self._normalize_schema(df)

            if dataframe is None:

                dataframe = df

                print("✓ First Delta table loaded successfully")

            else:

                try:

                    dataframe = dataframe.unionByName(
                        df,
                        allowMissingColumns=True,
                    )

                    print("✓ Union successful")

                except Exception as exc:

                    print("\n" + "=" * 80)
                    print("❌ UNION FAILED")
                    print("=" * 80)
                    print(f"Current Table : {path}")
                    print(f"Table Number  : {index}")
                    print("=" * 80)

                    print("\nCurrent Schema:")
                    dataframe.printSchema()

                    print("\nIncoming Schema:")
                    df.printSchema()

                    raise exc

        print("\n" + "=" * 80)
        print("Finished Reading Bronze Layer")
        print("=" * 80)

        if dataframe is None:

            raise DataReadException(
                "No Delta tables found."
            )

        if cache:

            print("Caching dataframe...")

            dataframe.cache()

            dataframe.count()

            print("✓ Cache completed")

        return dataframe
    # ==========================================================================
    # Bronze Reader
    # ==========================================================================

    def read_bronze(
        self,
        bronze_directory: str,
        cache: bool = False,
    ) -> DataFrame:

        paths = [

            str(path)

            for path in Path(bronze_directory).iterdir()

            if path.is_dir()

        ]

        return self.read_multiple_delta(
            paths=paths,
            cache=cache,
        )

    # ==========================================================================
    # Compatibility Wrapper
    # ==========================================================================

    def read_delta_directory(
        self,
        bronze_directory: str,
        cache: bool = False,
    ) -> DataFrame:
        """
        Compatibility wrapper for existing Silver pipelines.
        """

        return self.read_bronze(
            bronze_directory=bronze_directory,
            cache=cache,
        )

    # ==========================================================================
    # Exists
    # ==========================================================================

    def exists(
        self,
        input_path: str,
        file_format: str = DELTA,
    ) -> bool:

        try:

            self.read(
                input_path=input_path,
                file_format=file_format,
            )

            return True

        except Exception:

            return False