"""
===============================================================================
Configuration File
===============================================================================
Central location for project-wide configuration values.
"""

from pathlib import Path

# =============================================================================
# Project Paths
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"

RAW_PATH = DATA_DIR / "raw"

BRONZE_PATH = DATA_DIR / "bronze" / "delta"
SILVER_PATH = DATA_DIR / "silver"
GOLD_PATH = DATA_DIR / "gold"

# =============================================================================
# Silver Tables
# =============================================================================

STG_SESSIONS_PATH = SILVER_PATH / "stg_sessions"
STG_HITS_PATH = SILVER_PATH / "stg_hits"
STG_PRODUCTS_PATH = SILVER_PATH / "stg_products"
STG_PROMOTIONS_PATH = SILVER_PATH / "stg_promotions"
STG_TRAFFIC_PATH = SILVER_PATH / "stg_traffic"

# =============================================================================
# Gold Tables
# =============================================================================

FACT_SESSIONS_PATH = GOLD_PATH / "fact_sessions"

FACT_HITS_PATH = GOLD_PATH / "fact_hits"

FACT_PRODUCT_AFFINITY_PATH = GOLD_PATH / "fact_product_affinity"

DIM_USER_PATH = GOLD_PATH / "dim_user"

DIM_PRODUCT_PATH = GOLD_PATH / "dim_product"

DIM_DATE_PATH = GOLD_PATH / "dim_date"

DIM_DEVICE_PATH = GOLD_PATH / "dim_device"

DIM_TRAFFIC_SOURCE_PATH = GOLD_PATH / "dim_traffic_source"
# =============================================================================
# Spark Configuration
# =============================================================================

APP_NAME = "Ecommerce Clickstream Pipeline"

WRITE_MODE = "overwrite"

PARTITION_COLUMN = "visit_date"

# =============================================================================
# Logging
# =============================================================================

LOG_LEVEL = "INFO"