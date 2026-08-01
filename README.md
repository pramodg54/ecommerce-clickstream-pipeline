# E-Commerce Clickstream Funnel Pipeline

## Business Problem
Digital commerce teams require reliable clickstream analytics data for:
- Funnel conversion analysis
- Revenue attribution
- Customer acquisition tracking
- Device and traffic source optimization

## Architecture
BigQuery
↓
Bronze Parquet
↓
Silver Transformation
↓
Great Expectations
↓
dbt Gold Models
↓
DuckDB
↓
Power BI Dashboard
↓
Airflow Orchestration

## Technology Stack

| Layer | Tool |
|-------|------|
| Source | BigQuery |
| Storage | Parquet |
| Transformation | Pandas + dbt |
| Warehouse | DuckDB |
| Validation | Great Expectations |
| Orchestration | Airflow |
| Dashboard | Power BI |
| Monitoring | Airflow UI |

## Project Structure

data/
bronze/
silver/
gold/

spark_jobs/
tests/
ecommerce_analytics/
airflow/
docs/

## Setup Instructions

1. Create virtual environment
2. Install dependencies
3. Configure GCP credentials
4. Start Airflow
5. Trigger DAG

## Running Pipeline

docker compose up -d

Trigger DAG:
ecommerce_etl_pipeline

## Author
Pramod Godse