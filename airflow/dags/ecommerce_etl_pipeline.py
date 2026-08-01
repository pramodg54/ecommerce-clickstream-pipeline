from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    "owner": "pramod",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "capstone"],
) as dag:

    extract_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command="""
        export GOOGLE_APPLICATION_CREDENTIALS=/opt/project/configs/gcp_credentials.json
        cd /opt/project
        python -m spark_jobs.bronze.bronze_ingestion
        """
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="""
        cd /opt/project
        python -m spark_jobs.silver.stg_sessions
        """
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command="""
        cd /opt/project
        python -m tests.validate_silver
        """
    )

    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
    cd /opt/project/ecommerce_analytics
    dbt test
    """
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="""
        cd /opt/project/ecommerce_analytics
        dbt deps
        dbt run
        """
    )

    extract_bronze >> transform_silver >> validate_silver >> dbt_gold