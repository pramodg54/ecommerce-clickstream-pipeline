# 🚀 End-to-End E-Commerce Clickstream Data Engineering Pipeline

An end-to-end Data Engineering project that processes Google Analytics clickstream data from **Google BigQuery** using **Apache Spark**, stores it in a **Delta Lake Medallion Architecture (Bronze → Silver → Gold)**, orchestrates the workflow with **Apache Airflow**, validates data quality using **Great Expectations**, models business-ready datasets using **dbt**, and visualizes insights through **Microsoft Power BI**.

---

# 📌 Project Overview

This project demonstrates a complete modern data engineering pipeline for processing e-commerce clickstream data.

The pipeline performs:

- Incremental data extraction from Google BigQuery
- Raw data ingestion into Bronze Layer
- Data cleansing and transformation into Silver Layer
- Business-ready dimensional modeling in Gold Layer
- Automated workflow orchestration using Airflow
- Data Quality validation using Great Expectations
- Power BI Dashboard for business analytics

---

# 🏗️ Solution Architecture

<img width="1536" height="1024" alt="Project flow" src="https://github.com/user-attachments/assets/d2b8323c-cf40-4fa1-b0a1-4b84307fe044" />


The project follows the Medallion Architecture.

```
Google BigQuery
        │
        ▼
Apache Spark Extraction
        │
        ▼
Bronze Layer (Raw)
        │
        ▼
Silver Layer (Cleaned)
        │
        ▼
Gold Layer (Curated)
        │
 ┌──────┼────────────┐
 ▼      ▼            ▼
dbt   Great Expectations
        │
        ▼
Apache Airflow
        │
        ▼
Export Gold Layer
        │
        ▼
Microsoft Power BI
```

---

# 🛠 Technology Stack

| Category | Technology |
|----------|------------|
| Programming | Python 3.11 |
| Processing | Apache Spark (PySpark) |
| Storage | Delta Lake |
| Workflow | Apache Airflow |
| Data Quality | Great Expectations |
| Data Modeling | dbt |
| Source | Google BigQuery |
| Dashboard | Microsoft Power BI |
| Containerization | Docker |
| Version Control | Git & GitHub |

---

# 📂 Project Structure

```
ecommerce-clickstream-pipeline
│
├── airflow/
│
├── configs/
│
├── dashboards/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── dbt_project/
│
├── docs/
│
├── great_expectations/
│
├── notebooks/
│
├── spark_jobs/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── validation/
│   └── common/
│
├── tests/
│
├── Dockerfile
├── docker-compose.spark.yml
├── requirements.txt
└── README.md
```

---

# 🏛 Medallion Architecture

## Bronze Layer

Purpose:

- Store raw clickstream data
- Preserve original schema
- Incremental daily ingestion
- Delta Lake storage

---

## Silver Layer

Purpose:

- Flatten nested JSON
- Remove invalid records
- Standardize schema
- Handle null values
- Data cleansing

---

## Gold Layer

Business-ready analytical datasets.

### Fact Tables

- fact_sessions
- fact_hits
- fact_product_affinity

### Dimension Tables

- dim_date
- dim_device
- dim_product
- dim_user
- dim_traffic_source

---

# 🔄 ETL Pipeline

The ETL pipeline performs:

1. Read data from Google BigQuery
2. Store raw data in Bronze Layer
3. Clean and transform into Silver Layer
4. Build analytical Gold Layer
5. Validate Gold Layer
6. Export datasets for Power BI

---

# ⚡ Incremental Processing

The pipeline implements incremental loading.

Before processing, it compares the latest available dates in:

- Bronze
- Silver
- Gold

If data already exists, the stage is skipped.

Benefits:

- Faster execution
- Reduced compute cost
- No duplicate processing

---

# 🎯 Apache Airflow Workflow

The complete ETL pipeline is orchestrated using Apache Airflow.

Pipeline Flow
<img width="1827" height="162" alt="Screenshot 2026-08-01 222131" src="https://github.com/user-attachments/assets/2d67a3bd-ce87-45a5-93a1-1b838befaf38" />
```
Start
   │
Bronze Extraction
   │
Silver Transformation
   │
Gold Layer Creation
   │
Data Quality Validation
   │
Pipeline Complete
```

---

# ✅ Data Quality Validation

Great Expectations is used to validate Gold Layer datasets.

Implemented validations include:

- Table not empty
- Null checks
- Schema validation
- Unique key validation
- Numeric range validation

---


# 📊 Power BI Dashboard

Three interactive dashboards were developed.

## Executive Dashboard

Features:

- Total Sessions
- Total Users
- Total Revenue
- Transactions
- Bounce Rate
- Revenue Trend
- Conversion Funnel
- Device Analysis
- Country Analysis

---

## Customer Behaviour Dashboard

Features:

- Browser Usage
- Operating System
- Session Duration
- Traffic Medium
- Customer Retention
- Customer Drop-off Analysis

---

## Product & Marketing Dashboard

Features:

- Traffic Source
- Marketing Channel Analysis
- Product Interaction Events
- Device vs Traffic Source
- Product Affinity
- Product Performance

---

# 📈 Business Insights

The dashboard provides insights such as:

- Customer engagement trends
- Revenue trends
- Marketing channel effectiveness
- Device performance
- Customer conversion analysis
- Product interaction analysis
- Geographic revenue distribution

---

# 📁 Gold Layer Data Model

Fact Tables

- fact_sessions
- fact_hits
- fact_product_affinity

Dimension Tables

- dim_date
- dim_user
- dim_device
- dim_product
- dim_traffic_source

---

# 🚀 How to Run

## Clone Repository

```bash
git clone https://github.com/<your-github-username>/ecommerce-clickstream-pipeline.git

cd ecommerce-clickstream-pipeline
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Start Airflow

```bash
cd airflow

docker compose up -d
```

---

## Run Complete Pipeline

```bash
./run_complete_pipeline.sh
```

or

```bash
python spark_jobs/bronze/load_bronze.py

python spark_jobs/silver/load_silver.py

python spark_jobs/gold/load_gold.py
```

---

## Export Gold Layer

```bash
python export_gold_for_powerbi.py
```

---

# 📊 Sample Dashboard

## Executive Dashboard

<img width="1380" height="777" alt="image" src="https://github.com/user-attachments/assets/d671b347-af2f-4dd6-b389-40bdb666172c" />


---

## Customer Behaviour Dashboard

<img width="1376" height="772" alt="Screenshot 2026-08-01 235311" src="https://github.com/user-attachments/assets/2a9c209f-8e52-4477-aa4f-b3d3dae05296" />


---

## Product & Marketing Dashboard

<img width="1370" height="781" alt="Screenshot 2026-08-01 235333" src="https://github.com/user-attachments/assets/2b561ffb-25eb-4c60-a764-0951af57b839" />


---

# 🎓 Learning Outcomes

This project demonstrates practical implementation of:

- Apache Spark
- Delta Lake
- Medallion Architecture
- Apache Airflow
- Great Expectations
- dbt
- Incremental ETL
- Power BI
- Docker
- GitHub

---

# 📌 Future Enhancements

- Kafka Streaming
- Spark Structured Streaming
- Cloud Deployment (Azure/GCP/AWS)
- CI/CD Pipeline
- Grafana Monitoring
- Prometheus Metrics
- Machine Learning Recommendations

---

# 👨‍💻 Author

**Pramod**

---

# ⭐ Acknowledgements

- Google BigQuery Public Dataset
- Apache Spark
- Delta Lake
- Apache Airflow
- Great Expectations
- dbt Labs
- Microsoft Power BI

---
