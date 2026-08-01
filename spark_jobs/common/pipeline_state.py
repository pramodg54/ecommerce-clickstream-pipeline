"""
===============================================================================
File Name : pipeline_state.py
Project   : Ecommerce Clickstream Data Pipeline

Purpose
-------
Determine whether Bronze, Silver and Gold pipelines should execute
based on business dates rather than filesystem timestamps.

Bronze
-------
Latest available Bronze table:
ga_sessions_YYYYMMDD

Silver
-------
MAX(visit_date) from stg_sessions

Gold
-----
MAX(visit_date) from fact_sessions

Author : Pramod
===============================================================================
"""

from __future__ import annotations

# =============================================================================
# Imports
# =============================================================================

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pyspark.sql.functions import max as spark_max

from spark_jobs.common.config import (
    BRONZE_PATH,
    STG_SESSIONS_PATH,
    FACT_SESSIONS_PATH,
)

from spark_jobs.common.spark_session import get_spark

# =============================================================================
# Spark Session
# =============================================================================

spark = get_spark(app_name="pipeline_state")

# =============================================================================
# Decision Model
# =============================================================================


@dataclass
class PipelineDecision:
    """
    Pipeline execution decision.
    """

    should_run: bool
    reason: str
    upstream_timestamp: Optional[str]
    downstream_timestamp: Optional[str]


# =============================================================================
# Bronze Helper
# =============================================================================

def get_latest_bronze_date() -> Optional[str]:
    """
    Find latest Bronze table by folder name.

    Example

        ga_sessions_20160801
        ga_sessions_20160802

    Returns

        20160802
    """

    bronze_root = Path(BRONZE_PATH)

    if not bronze_root.exists():
        return None

    dates = []

    for table in bronze_root.iterdir():

        if (
            table.is_dir()
            and table.name.startswith("ga_sessions_")
        ):

            try:

                folder_date = table.name.replace(
                    "ga_sessions_",
                    ""
                )

                if folder_date.isdigit():
                    dates.append(folder_date)

            except Exception:
                pass

    if not dates:
        return None

    return max(dates)


# =============================================================================
# Delta Helper
# =============================================================================

def get_max_visit_date(
    delta_path: str | Path,
) -> Optional[str]:
    """
    Return MAX(visit_date) from a Delta table.
    """

    path = Path(delta_path)

    if not path.exists():
        return None

    try:

        df = (
            spark.read
            .format("delta")
            .load(str(path))
        )

        result = (

            df

            .select(
                spark_max("visit_date").alias(
                    "max_date"
                )
            )

            .collect()[0]["max_date"]

        )

        if result is None:
            return None

        return str(result)

    except Exception as e:

        print(f"Error reading {path}: {e}")

        return None


# =============================================================================
# Logging
# =============================================================================

def log_decision(
    stage: str,
    decision: PipelineDecision,
) -> None:

    print()

    print("=" * 80)

    print(f"{stage.upper()} CHECK")

    print("=" * 80)

    print(
        f"Decision     : {'RUN' if decision.should_run else 'SKIP'}"
    )

    print(
        f"Reason       : {decision.reason}"
    )

    print(
        f"Upstream     : {decision.upstream_timestamp}"
    )

    print(
        f"Downstream   : {decision.downstream_timestamp}"
    )

    print("=" * 80)

    print()

# =============================================================================
# Bronze
# =============================================================================

def should_run_bronze(
    force: bool = False,
) -> PipelineDecision:
    """
    Decide whether Bronze should execute.

    Rules
    -----
    1. Force execution -> RUN
    2. No Bronze tables -> RUN
    3. Bronze tables already exist -> SKIP
    """

    if force:

        decision = PipelineDecision(
            should_run=True,
            reason="Forced execution.",
            upstream_timestamp=None,
            downstream_timestamp=None,
        )

        log_decision("Bronze", decision)

        return decision

    latest_bronze = get_latest_bronze_date()

    if latest_bronze is None:

        decision = PipelineDecision(
            should_run=True,
            reason="No Bronze Delta tables found.",
            upstream_timestamp=None,
            downstream_timestamp=None,
        )

    else:

        decision = PipelineDecision(
            should_run=False,
            reason="Bronze tables already exist.",
            upstream_timestamp=latest_bronze,
            downstream_timestamp=latest_bronze,
        )

    log_decision("Bronze", decision)

    return decision


# =============================================================================
# Silver
# =============================================================================

def should_run_silver(
    force: bool = False,
) -> PipelineDecision:
    """
    Compare latest Bronze folder date with
    MAX(visit_date) in stg_sessions.
    """

    if force:

        decision = PipelineDecision(
            should_run=True,
            reason="Forced execution.",
            upstream_timestamp=None,
            downstream_timestamp=None,
        )

        log_decision("Silver", decision)

        return decision

    bronze_date = get_latest_bronze_date()

    silver_date = get_max_visit_date(
        STG_SESSIONS_PATH
    )

    if bronze_date is None:

        decision = PipelineDecision(
            should_run=False,
            reason="No Bronze data available.",
            upstream_timestamp=None,
            downstream_timestamp=silver_date,
        )

    elif silver_date is None:

        decision = PipelineDecision(
            should_run=True,
            reason="Silver table does not exist.",
            upstream_timestamp=bronze_date,
            downstream_timestamp=None,
        )

    elif bronze_date > silver_date:

        decision = PipelineDecision(
            should_run=True,
            reason="Bronze contains newer business date.",
            upstream_timestamp=bronze_date,
            downstream_timestamp=silver_date,
        )

    else:

        decision = PipelineDecision(
            should_run=False,
            reason="Silver already up-to-date.",
            upstream_timestamp=bronze_date,
            downstream_timestamp=silver_date,
        )

    log_decision("Silver", decision)

    return decision


# =============================================================================
# Gold
# =============================================================================

def should_run_gold(
    force: bool = False,
) -> PipelineDecision:
    """
    Compare Silver MAX(visit_date)
    against Gold MAX(visit_date).
    """

    if force:

        decision = PipelineDecision(
            should_run=True,
            reason="Forced execution.",
            upstream_timestamp=None,
            downstream_timestamp=None,
        )

        log_decision("Gold", decision)

        return decision

    silver_date = get_max_visit_date(
        STG_SESSIONS_PATH
    )

    gold_date = get_max_visit_date(
        FACT_SESSIONS_PATH
    )

    if silver_date is None:

        decision = PipelineDecision(
            should_run=False,
            reason="Silver table not found.",
            upstream_timestamp=None,
            downstream_timestamp=gold_date,
        )

    elif gold_date is None:

        decision = PipelineDecision(
            should_run=True,
            reason="Gold table does not exist.",
            upstream_timestamp=silver_date,
            downstream_timestamp=None,
        )

    elif silver_date > gold_date:

        decision = PipelineDecision(
            should_run=True,
            reason="Silver contains newer business date.",
            upstream_timestamp=silver_date,
            downstream_timestamp=gold_date,
        )

    else:

        decision = PipelineDecision(
            should_run=False,
            reason="Gold already up-to-date.",
            upstream_timestamp=silver_date,
            downstream_timestamp=gold_date,
        )

    log_decision("Gold", decision)

    return decision


# =============================================================================
# Public API
# =============================================================================

def should_run_stage(
    stage: str,
    force: bool = False,
) -> PipelineDecision:
    """
    Airflow entry point.
    """

    stage = stage.lower()

    if stage == "bronze":
        return should_run_bronze(force)

    elif stage == "silver":
        return should_run_silver(force)

    elif stage == "gold":
        return should_run_gold(force)

    raise ValueError(
        f"Unknown stage: {stage}"
    )


# =============================================================================
# Test Harness
# =============================================================================

def test_pipeline_state():

    print("\n")
    print("=" * 80)
    print("PIPELINE STATE CHECK")
    print("=" * 80)

    should_run_bronze()
    should_run_silver()
    should_run_gold()

    print("\nPipeline state check completed.\n")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":

    test_pipeline_state()