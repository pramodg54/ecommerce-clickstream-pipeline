"""
===============================================================================
File Name : ecommerce_pipeline.py
Project   : Ecommerce Clickstream Pipeline
Purpose   : Airflow DAG for orchestrating the complete Bronze → Silver → Gold
            pipeline with incremental execution using pipeline state checks.
Author    : Pramod
===============================================================================
"""

# =============================================================================
# Imports
# =============================================================================

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from spark_jobs.common.pipeline_state import should_run_stage


# =============================================================================
# Project Constants
# =============================================================================

PROJECT_DIR = "/opt/project"

BRONZE_DIR = f"{PROJECT_DIR}/data/bronze"
SILVER_DIR = f"{PROJECT_DIR}/data/silver"
GOLD_DIR = f"{PROJECT_DIR}/data/gold"

GX_DIR = f"{PROJECT_DIR}/great_expectations"


# =============================================================================
# Default Arguments
# =============================================================================

default_args = {
    "owner": "Pramod",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# =============================================================================
# Helper Functions
# =============================================================================

def create_spark_task(
    task_id: str,
    module: str,
):
    """
    Creates a Spark execution task.
    """

    return BashOperator(

        task_id=task_id,

        bash_command=f"""
        cd {PROJECT_DIR}
        python -m {module}
        """,

        execution_timeout=timedelta(minutes=45),

    )


# =============================================================================
# Branch Functions
# =============================================================================

def bronze_branch():

    decision = should_run_stage("bronze")

    if decision.should_run:
        return "bronze_layer.bronze_ingestion"

    return "skip_bronze"


def silver_branch():

    decision = should_run_stage("silver")

    if decision.should_run:
        return "silver_layer.stg_sessions"

    return "skip_silver"


def gold_branch():

    decision = should_run_stage("gold")

    if decision.should_run:
        return "gold_dimensions.dim_date"

    return "skip_gold"


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(

    dag_id="ecommerce_pipeline",

    description="End-to-End Ecommerce Clickstream Bronze → Silver → Gold Pipeline",

    default_args=default_args,

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    render_template_as_native_obj=True,

    tags=[
        "spark",
        "delta-lake",
        "airflow",
        "capstone",
        "ecommerce",
    ],

) as dag:

    dag.doc_md = """
# Ecommerce Clickstream Data Pipeline

## Architecture

Bronze → Silver → Gold

### Features

- Incremental execution
- Pipeline state detection
- Automatic stage skipping
- Duplicate session cleanup
- Delta Lake
- Spark
- Airflow
"""

    # =========================================================================
    # Start / End
    # =========================================================================

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    # =========================================================================
    # Bronze Check
    # =========================================================================

    bronze_check = BranchPythonOperator(
        task_id="bronze_check",
        python_callable=bronze_branch,
    )

    skip_bronze = EmptyOperator(
        task_id="skip_bronze",
    )

    bronze_merge = EmptyOperator(
        task_id="bronze_merge",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    # Silver Check
    # =========================================================================

    silver_check = BranchPythonOperator(
        task_id="silver_check",
        python_callable=silver_branch,
    )

    skip_silver = EmptyOperator(
        task_id="skip_silver",
    )

    silver_merge = EmptyOperator(
        task_id="silver_merge",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    # Gold Check
    # =========================================================================

    gold_check = BranchPythonOperator(
        task_id="gold_check",
        python_callable=gold_branch,
    )

    skip_gold = EmptyOperator(
        task_id="skip_gold",
    )

    gold_merge = EmptyOperator(
        task_id="gold_merge",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    # Bronze Layer
    # =========================================================================

    with TaskGroup(
        group_id="bronze_layer",
        tooltip="Bronze Data Ingestion",
    ) as bronze_layer:

        bronze_ingestion = create_spark_task(
            task_id="bronze_ingestion",
            module="spark_jobs.bronze.bronze_ingestion",
        )

    # =========================================================================
    # Bronze Flow
    # =========================================================================

    (
        start
        >> bronze_check
    )

    (
        bronze_check
        >> bronze_layer
        >> bronze_merge
    )

    (
        bronze_check
        >> skip_bronze
        >> bronze_merge
    )
    # =========================================================================
    # Silver Check
    # =========================================================================

    (
        bronze_merge
        >> silver_check
    )

    (
        silver_check
        >> skip_silver
        >> silver_merge
    )

    # =========================================================================
    # Silver Layer
    # =========================================================================

    with TaskGroup(
        group_id="silver_layer",
        tooltip="Silver Data Transformation",
    ) as silver_layer:

        # ---------------------------------------------------------------------
        # Sessions
        # ---------------------------------------------------------------------

        stg_sessions = create_spark_task(
            task_id="stg_sessions",
            module="spark_jobs.silver.stg_sessions",
        )

        # ---------------------------------------------------------------------
        # Hits
        # ---------------------------------------------------------------------

        stg_hits = create_spark_task(
            task_id="stg_hits",
            module="spark_jobs.silver.stg_hits",
        )

        # ---------------------------------------------------------------------
        # Products
        # ---------------------------------------------------------------------

        stg_products = create_spark_task(
            task_id="stg_products",
            module="spark_jobs.silver.stg_products",
        )

        # ---------------------------------------------------------------------
        # Promotions
        # ---------------------------------------------------------------------

        stg_promotions = create_spark_task(
            task_id="stg_promotions",
            module="spark_jobs.silver.stg_promotions",
        )

        # ---------------------------------------------------------------------
        # Traffic
        # ---------------------------------------------------------------------

        stg_traffic = create_spark_task(
            task_id="stg_traffic",
            module="spark_jobs.silver.stg_traffic",
        )

        (
            stg_sessions
            >> stg_hits
            >> stg_products
            >> stg_promotions
            >> stg_traffic
        )

    # =========================================================================
    # Duplicate Session Cleanup
    # =========================================================================

    fix_duplicate_sessions = create_spark_task(
        task_id="fix_duplicate_sessions",
        module="spark_jobs.debug.fix_duplicate_sessions",
    )

    # =========================================================================
    # Silver Validation
    # =========================================================================

    validate_silver = EmptyOperator(
        task_id="validate_silver",
    )

    # =========================================================================
    # Silver Flow
    # =========================================================================

    (
        silver_check
        >> silver_layer
        >> fix_duplicate_sessions
        >> validate_silver
        >> silver_merge
    )

    # =========================================================================
    # Gold Check
    # =========================================================================

    (
        silver_merge
        >> gold_check
    )

    (
        gold_check
        >> skip_gold
        >> gold_merge
    )

    # =========================================================================
    # Gold Layer - Dimensions
    # =========================================================================

    with TaskGroup(
        group_id="gold_dimensions",
        tooltip="Gold Dimension Tables",
    ) as gold_dimensions:

        dim_date = create_spark_task(
            task_id="dim_date",
            module="spark_jobs.gold.dimensions.dim_date",
        )

        dim_device = create_spark_task(
            task_id="dim_device",
            module="spark_jobs.gold.dimensions.dim_device",
        )

        dim_product = create_spark_task(
            task_id="dim_product",
            module="spark_jobs.gold.dimensions.dim_product",
        )

        dim_traffic_source = create_spark_task(
            task_id="dim_traffic_source",
            module="spark_jobs.gold.dimensions.dim_traffic_source",
        )

        dim_user = create_spark_task(
            task_id="dim_user",
            module="spark_jobs.gold.dimensions.dim_user",
        )

        (
            dim_date
            >> dim_device
            >> dim_product
            >> dim_traffic_source
            >> dim_user
        )

    # =========================================================================
    # Gold Layer - Facts
    # =========================================================================

    with TaskGroup(
        group_id="gold_facts",
        tooltip="Gold Fact Tables",
    ) as gold_facts:

        fact_sessions = create_spark_task(
            task_id="fact_sessions",
            module="spark_jobs.gold.facts.fact_sessions",
        )

        fact_hits = create_spark_task(
            task_id="fact_hits",
            module="spark_jobs.gold.facts.fact_hits",
        )

        fact_product_affinity = create_spark_task(
            task_id="fact_product_affinity",
            module="spark_jobs.gold.facts.fact_product_affinity",
        )

        (
            fact_sessions
            >> fact_hits
            >> fact_product_affinity
        )

    # =========================================================================
    # Gold Validation
    # =========================================================================

    validate_gold = EmptyOperator(
        task_id="validate_gold",
    )

    # =========================================================================
    # Gold Flow
    # =========================================================================

    (
        gold_check
        >> gold_dimensions
        >> gold_facts
        >> validate_gold
        >> gold_merge
    )

    # =========================================================================
    # End
    # =========================================================================

    (
        gold_merge
        >> end
    )