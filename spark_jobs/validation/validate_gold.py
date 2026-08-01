"""
==============================================================================
Great Expectations Validation
==============================================================================

Validates Gold Layer tables before Power BI consumption.

Tables:
    - fact_sessions
    - fact_hits

Author  : Pramod
Project : E-Commerce Clickstream Pipeline
============================================================================== 
"""

import sys

import great_expectations as gx

from spark_jobs.common.spark_session import get_spark

from spark_jobs.common.config import (
    FACT_SESSIONS_PATH,
    FACT_HITS_PATH,
)


# ==============================================================================
# Spark Session
# ==============================================================================

spark = get_spark()


# ==============================================================================
# Helper Functions
# ==============================================================================

def print_header(title):

    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def print_success(message):

    print(f"✓ {message}")


def print_failure(message):

    print(f"✗ {message}")


def load_delta_table(table_name, table_path):

    print_header(f"Loading {table_name}")

    df = (
        spark.read
        .format("delta")
        .load(str(table_path))
    )

    row_count = df.count()

    print_success(f"Rows Loaded : {row_count:,}")

    gx_df = gx.from_pandas(df.toPandas())

    return gx_df

# ==============================================================================
# Validation Functions
# ==============================================================================

def validate_fact_sessions(gx_df):

    print_header("Validating fact_sessions")

    validations = [

        (
            "Table is not empty",
            gx_df.expect_table_row_count_to_be_between(
                min_value=1
            )
        ),

        (
            "session_key is unique",
            gx_df.expect_column_values_to_be_unique(
                "session_key"
            )
        ),

        (
            "session_key is not null",
            gx_df.expect_column_values_to_not_be_null(
                "session_key"
            )
        ),

        (
            "visit_date is not null",
            gx_df.expect_column_values_to_not_be_null(
                "visit_date"
            )
        ),

        (
            "page_views >= 0",
            gx_df.expect_column_values_to_be_between(
                "page_views",
                min_value=0
            )
        ),

        (
            "hits >= 0",
            gx_df.expect_column_values_to_be_between(
                "hits",
                min_value=0
            )
        ),

        (
            "transaction_revenue >= 0",
            gx_df.expect_column_values_to_be_between(
                "transaction_revenue",
                min_value=0
            )
        )

    ]

    passed = True

    for name, result in validations:

        if result["success"]:

            print_success(name)

        else:

            print_failure(name)

            if "result" in result:

                unexpected = result["result"].get(
                    "unexpected_count",
                    "Unknown"
                )

                print(f"    Unexpected Records : {unexpected}")

            passed = False

    return passed


def validate_fact_hits(gx_df):

    print_header("Validating fact_hits")

    validations = [

        (
            "Table is not empty",
            gx_df.expect_table_row_count_to_be_between(
                min_value=1
            )
        ),

        (
            "session_key is not null",
            gx_df.expect_column_values_to_not_be_null(
                "session_key"
            )
        ),

        (
            "event_action is not null",
            gx_df.expect_column_values_to_not_be_null(
                "event_action"
            )
        ),

        (
            "event_category is not null",
            gx_df.expect_column_values_to_not_be_null(
                "event_category"
            )
        )

    ]

    # Validate hit_key only if present

    if "hit_key" in gx_df.columns:

        validations.append(

            (
                "hit_key is unique",
                gx_df.expect_column_values_to_be_unique(
                    "hit_key"
                )
            )

        )

    passed = True

    for name, result in validations:

        if result["success"]:

            print_success(name)

        else:

            print_failure(name)

            if "result" in result:

                unexpected = result["result"].get(
                    "unexpected_count",
                    "Unknown"
                )

                print(f"    Unexpected Records : {unexpected}")

            passed = False

    return passed

# ==============================================================================
# Main
# ==============================================================================

def main():

    print()
    print("=" * 80)
    print("GREAT EXPECTATIONS GOLD LAYER VALIDATION")
    print("=" * 80)

    try:

        # ------------------------------------------------------------------
        # fact_sessions
        # ------------------------------------------------------------------

        fact_sessions = load_delta_table(
            "fact_sessions",
            FACT_SESSIONS_PATH
        )

        sessions_passed = validate_fact_sessions(
            fact_sessions
        )

        # ------------------------------------------------------------------
        # fact_hits
        # ------------------------------------------------------------------

        fact_hits = load_delta_table(
            "fact_hits",
            FACT_HITS_PATH
        )

        hits_passed = validate_fact_hits(
            fact_hits
        )

        # ------------------------------------------------------------------
        # Final Summary
        # ------------------------------------------------------------------

        print()
        print("=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)

        print(
            f"fact_sessions : {'PASS' if sessions_passed else 'FAIL'}"
        )

        print(
            f"fact_hits     : {'PASS' if hits_passed else 'FAIL'}"
        )

        print("=" * 80)

        if sessions_passed and hits_passed:

            print()
            print_success("ALL GREAT EXPECTATIONS VALIDATIONS PASSED")
            print()

            sys.exit(0)

        else:

            print()
            print_failure("VALIDATION FAILED")
            print()

            sys.exit(1)

    except Exception as ex:

        print()
        print("=" * 80)
        print("VALIDATION ERROR")
        print("=" * 80)

        print(ex)

        sys.exit(1)


if __name__ == "__main__":
    main()