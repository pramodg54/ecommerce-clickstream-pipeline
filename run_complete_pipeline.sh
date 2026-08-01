#!/bin/bash

set -e

echo "========================================"
echo "Running Bronze Pipeline"
echo "========================================"

python -m spark_jobs.bronze.bronze_ingestion

echo "========================================"
echo "Running Silver Pipelines"
echo "========================================"

python -m spark_jobs.silver.stg_sessions
python -m spark_jobs.silver.stg_hits
python -m spark_jobs.silver.stg_products

echo "========================================"
echo "Running Gold Dimension Pipelines"
echo "========================================"

python -m spark_jobs.gold.dimensions.dim_date
python -m spark_jobs.gold.dimensions.dim_user
python -m spark_jobs.gold.dimensions.dim_product
python -m spark_jobs.gold.dimensions.dim_device
python -m spark_jobs.gold.dimensions.dim_traffic_source

echo "========================================"
echo "Running Gold Fact Pipelines"
echo "========================================"

python -m spark_jobs.gold.facts.fact_sessions
python -m spark_jobs.gold.facts.fact_hits
python -m spark_jobs.gold.facts.fact_product_affinity

echo "========================================"
echo "PIPELINE COMPLETED SUCCESSFULLY"
echo "========================================"