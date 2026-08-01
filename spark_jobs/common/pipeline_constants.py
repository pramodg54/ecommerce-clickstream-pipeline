"""
===============================================================================
File Name : pipeline_constants.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Global constants used across the data engineering framework.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

# ==============================================================================
# Data Layers
# ==============================================================================

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

LAYERS = (
    BRONZE,
    SILVER,
    GOLD,
)

# ==============================================================================
# File Formats
# ==============================================================================

DELTA = "delta"
PARQUET = "parquet"
CSV = "csv"
JSON = "json"

SUPPORTED_FORMATS = (
    DELTA,
    PARQUET,
    CSV,
    JSON,
)

# ==============================================================================
# Write Modes
# ==============================================================================

OVERWRITE = "overwrite"
APPEND = "append"
IGNORE = "ignore"
ERROR_IF_EXISTS = "errorifexists"

WRITE_MODES = (
    OVERWRITE,
    APPEND,
    IGNORE,
    ERROR_IF_EXISTS,
)

# ==============================================================================
# Metadata Columns
# ==============================================================================

SOURCE_TABLE = "source_table"

LAYER = "layer"

PIPELINE_NAME = "pipeline_name"

JOB_VERSION = "job_version"

CREATED_BY = "created_by"

ETL_LOADED_TIMESTAMP = "etl_loaded_timestamp"

# ==============================================================================
# Common Audit Columns
# ==============================================================================

AUDIT_COLUMNS = [

    SOURCE_TABLE,

    LAYER,

    PIPELINE_NAME,

    JOB_VERSION,

    CREATED_BY,

    ETL_LOADED_TIMESTAMP,
]

# ==============================================================================
# Default Validation Settings
# ==============================================================================

DEFAULT_MIN_ROWS = 1

DEFAULT_PARTITIONS = 8

DEFAULT_JOB_VERSION = "1.0.0"

DEFAULT_OWNER = "Data Engineering"

DEFAULT_CREATED_BY = "Spark Pipeline"

# ==============================================================================
# Boolean Values
# ==============================================================================

SUCCESS = "SUCCESS"

FAILED = "FAILED"

RUNNING = "RUNNING"

PENDING = "PENDING"

# ==============================================================================
# Spark Configuration Defaults
# ==============================================================================

DEFAULT_CACHE = True

DEFAULT_SHUFFLE_PARTITIONS = 8

DEFAULT_PARTITION_OVERWRITE_MODE = "dynamic"

DEFAULT_AUTO_MERGE = True

# ==============================================================================
# Pipeline Stages
# ==============================================================================

EXTRACT = "Extract"

TRANSFORM = "Transform"

VALIDATE = "Validate"

LOAD = "Load"

PIPELINE_STAGES = (

    EXTRACT,

    TRANSFORM,

    VALIDATE,

    LOAD,
)

# ==============================================================================
# Common Date Formats
# ==============================================================================

GA_DATE_FORMAT = "yyyyMMdd"

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# ==============================================================================
# Google Analytics Standard Columns
# ==============================================================================

SESSION_ID = "session_id"

USER_ID = "user_id"

VISIT_DATE = "visit_date"

HIT_NUMBER = "hit_number"

PRODUCT_SKU = "product_sku"

# ==============================================================================
# Standard Validation Messages
# ==============================================================================

VALIDATION_SUCCESS = "Validation completed successfully."

VALIDATION_FAILED = "Validation failed."

WRITE_SUCCESS = "Data written successfully."

WRITE_FAILED = "Failed to write data."

# ==============================================================================
# Logging
# ==============================================================================

LOG_SEPARATOR = "=" * 80

STEP_SEPARATOR = "-" * 80

# ==============================================================================
# Pipeline Names
# ==============================================================================

STG_SESSIONS = "stg_sessions"

STG_HITS = "stg_hits"

STG_PRODUCTS = "stg_products"

STG_PROMOTIONS = "stg_promotions"

STG_TRAFFIC = "stg_traffic"

DIM_PRODUCT = "dim_product"

DIM_USER = "dim_user"

FACT_SESSIONS = "fact_sessions"

FACT_PRODUCT_AFFINITY = "fact_product_affinity"

PIPELINE_NAMES = (

    STG_SESSIONS,

    STG_HITS,

    STG_PRODUCTS,

    STG_PROMOTIONS,

    STG_TRAFFIC,

    DIM_PRODUCT,

    DIM_USER,

    FACT_SESSIONS,

    FACT_PRODUCT_AFFINITY,
)