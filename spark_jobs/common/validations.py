"""
===============================================================================
Validation Utilities
===============================================================================
Reusable validation framework for Bronze, Silver and Gold layers.
"""

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class ValidationSuite:
    """
    Wrapper around the reusable validation functions.
    Compatible with the Silver pipelines.
    """

    def __init__(
        self,
        required_columns=None,
        key_columns=None,
        non_negative_columns=None,
        minimum_rows=1,
    ):
        self.required_columns = required_columns or []
        self.key_columns = key_columns or []
        self.non_negative_columns = non_negative_columns or []
        self.minimum_rows = minimum_rows

    def run(self, df: DataFrame) -> None:

        validate_dataframe_not_empty(df)

        validate_required_columns(
            df,
            self.required_columns,
        )

        if self.key_columns:
            validate_no_duplicate_keys(
                df,
                self.key_columns,
            )

        if self.non_negative_columns:
            validate_non_negative_values(
                df,
                self.non_negative_columns,
            )

        row_count = df.count()

        if row_count < self.minimum_rows:
            raise ValueError(
                f"Validation failed. Expected at least "
                f"{self.minimum_rows} rows but found {row_count}."
            )

    @staticmethod
    def validate_dataframe_not_empty(df):
        validate_dataframe_not_empty(df)

    @staticmethod
    def validate_required_columns(
        df,
        required_columns,
    ):
        validate_required_columns(
            df,
            required_columns,
        )

    @staticmethod
    def validate_no_duplicate_keys(
        df,
        key_columns,
    ):
        validate_no_duplicate_keys(
            df,
            key_columns,
        )

    @staticmethod
    def validate_non_negative_values(
        df,
        numeric_columns,
    ):
        validate_non_negative_values(
            df,
            numeric_columns,
        )

    @staticmethod
    def validate_row_count(
        source_df,
        target_df,
    ):
        validate_row_count(
            source_df,
            target_df,
        )

    @staticmethod
    def validate_schema(
        df,
        expected_columns,
    ):
        validate_schema(
            df,
            expected_columns,
        )

    @staticmethod
    def run_standard_validations(
        source_df,
        target_df,
        required_columns,
        key_columns,
        non_negative_columns=None,
    ):
        run_standard_validations(
            source_df=source_df,
            target_df=target_df,
            required_columns=required_columns,
            key_columns=key_columns,
            non_negative_columns=non_negative_columns,
        )

    @staticmethod
    def validate_write_success(
        path,
        spark,
    ):
        validate_write_success(
            path,
            spark,
        )

        
def validate_dataframe_not_empty(
    df: DataFrame,
) -> None:

    if df.rdd.isEmpty():
        raise ValueError(
            "DataFrame is empty."
        )


def validate_required_columns(
    df: DataFrame,
    required_columns,
) -> None:

    missing = [
        c
        for c in required_columns
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )


def validate_no_duplicate_keys(
    df: DataFrame,
    key_columns,
) -> None:

    duplicates = (

        df

        .groupBy(*key_columns)

        .count()

        .filter(
            col("count") > 1
        )

        .count()

    )

    if duplicates:
        raise ValueError(
            f"Duplicate keys found using {key_columns}"
        )


def validate_non_negative_values(
    df: DataFrame,
    numeric_columns,
) -> None:

    for column in numeric_columns:

        invalid = (

            df

            .filter(
                col(column) < 0
            )

            .count()

        )

        if invalid:

            raise ValueError(
                f"Negative values found in {column}"
            )


def validate_row_count(
    source_df: DataFrame,
    target_df: DataFrame,
) -> None:

    if target_df.count() == 0:

        raise ValueError(
            "Target DataFrame contains no rows."
        )


def validate_schema(
    df: DataFrame,
    expected_columns,
) -> None:

    validate_required_columns(
        df,
        expected_columns,
    )


def run_standard_validations(
    source_df,
    target_df,
    required_columns,
    key_columns,
    non_negative_columns=None,
):

    validate_dataframe_not_empty(
        target_df
    )

    validate_required_columns(
        target_df,
        required_columns,
    )

    validate_no_duplicate_keys(
        target_df,
        key_columns,
    )

    if non_negative_columns:

        validate_non_negative_values(
            target_df,
            non_negative_columns,
        )


def validate_write_success(
    path,
    spark,
):

    df = (

        spark.read

        .format("delta")

        .load(path)

    )

    validate_dataframe_not_empty(df)