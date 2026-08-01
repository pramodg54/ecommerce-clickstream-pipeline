"""
===============================================================================
File Name : pipeline_config.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Central configuration for all ETL pipelines.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from spark_jobs.common.config import (
    STG_SESSIONS_PATH,
    STG_HITS_PATH,
    STG_PRODUCTS_PATH,
    STG_PROMOTIONS_PATH,
    STG_TRAFFIC_PATH,
    DIM_PRODUCT_PATH,
    DIM_USER_PATH,
    DIM_DATE_PATH,
    DIM_DEVICE_PATH,
    DIM_TRAFFIC_SOURCE_PATH,
    FACT_SESSIONS_PATH,
    FACT_HITS_PATH,
    FACT_PRODUCT_AFFINITY_PATH,
    PARTITION_COLUMN,
)

from spark_jobs.common.pipeline_constants import (
    BRONZE,
    SILVER,
    GOLD,
    DELTA,
    OVERWRITE,
)


# ==============================================================================
# Pipeline Definition
# ==============================================================================

@dataclass(frozen=True)
class PipelineDefinition:

    name: str

    layer: str

    output_path: str

    partition_column: str

    file_format: str

    write_mode: str

    required_columns: List[str]

    key_columns: List[str]

    non_negative_columns: List[str]

    min_rows: int = 1

    version: str = "1.0.0"

    owner: str = "Data Engineering"


# ==============================================================================
# Pipeline Configurations
# ==============================================================================

PIPELINE_CONFIG = {

    "stg_sessions":

        PipelineDefinition(

            name="stg_sessions",

            layer=SILVER,

            output_path=STG_SESSIONS_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "session_id",
                "user_id",
                "visit_date",
                "visit_number",
            ],

            key_columns=[
                "session_id",
            ],

            non_negative_columns=[
                "visit_number",
                "hits",
                "page_views",
                "bounces",
                "transactions",
                "transaction_revenue",
            ],
        ),

    "stg_hits":

        PipelineDefinition(

            name="stg_hits",

            layer=SILVER,

            output_path=STG_HITS_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "session_id",
                "hit_number",
                "hit_type",
            ],

            key_columns=[
                "session_id",
                "hit_number",
            ],

            non_negative_columns=[
                "hit_number",
                "hit_time",
                "transaction_revenue",
            ],
        ),

    "stg_products":

        PipelineDefinition(

            name="stg_products",

            layer=SILVER,

            output_path=STG_PRODUCTS_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "session_id",
                "hit_number",
                "product_sku",
                "product_name",
            ],

            key_columns=[
                "session_id",
                "hit_number",
                "product_sku",
            ],

            non_negative_columns=[
                "quantity",
                "product_price",
                "local_product_price",
                "product_revenue",
                "refund_amount",
            ],
        ),

    "stg_promotions":

        PipelineDefinition(

            name="stg_promotions",

            layer=SILVER,

            output_path=STG_PROMOTIONS_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "session_id",
                "hit_number",
                "promotion_id",
            ],

            key_columns=[
                "session_id",
                "hit_number",
                "promotion_id",
            ],

            non_negative_columns=[],
        ),

    "stg_traffic":

        PipelineDefinition(

            name="stg_traffic",

            layer=SILVER,

            output_path=STG_TRAFFIC_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "session_id",
                "channel_grouping",
                "traffic_source",
                "traffic_medium",
            ],

            key_columns=[
                "session_id",
            ],

            non_negative_columns=[],
        ),

    "dim_product":

        PipelineDefinition(

            name="dim_product",

            layer=GOLD,

            output_path=DIM_PRODUCT_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "product_sku",
                "product_name",
            ],

            key_columns=[
                "product_sku",
            ],

            non_negative_columns=[],
        ),

    "dim_user":

        PipelineDefinition(

            name="dim_user",

            layer=GOLD,

            output_path=DIM_USER_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "user_id",
            ],

            key_columns=[
                "user_id",
            ],

            non_negative_columns=[],
        ),

    "dim_date":

        PipelineDefinition(

            name="dim_date",

            layer=GOLD,

            output_path=DIM_DATE_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "date_key",
                "calendar_date",
            ],

            key_columns=[
                "date_key",
            ],

            non_negative_columns=[],

        ),

    "dim_device":

        PipelineDefinition(

            name="dim_device",

            layer=GOLD,

            output_path=DIM_DEVICE_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "device_key",
                "browser",
                "operating_system",
            ],

            key_columns=[
                "device_key",
            ],

            non_negative_columns=[],

        ),

    "dim_traffic_source":

        PipelineDefinition(

            name="dim_traffic_source",

            layer=GOLD,

            output_path=DIM_TRAFFIC_SOURCE_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "traffic_source_key",
                "traffic_source",
                "traffic_medium",
            ],

            key_columns=[
                "traffic_source_key",
            ],

            non_negative_columns=[],

        ),

    "fact_sessions":

        PipelineDefinition(

            name="fact_sessions",

            layer=GOLD,

            output_path=FACT_SESSIONS_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "session_key",
                "session_id",
                "user_id",
            ],

            key_columns=[
                "session_key",
            ],

            non_negative_columns=[
                "transaction_revenue",
            ],
        ),

    "fact_hits":

        PipelineDefinition(

            name="fact_hits",

            layer=GOLD,

            output_path=FACT_HITS_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[

                "hit_key",
                "session_id",
                "visitor_id",

            ],

            key_columns=[

                "hit_key",

            ],

            non_negative_columns=[

                "hit_number",
                "hit_time",
                "transaction_revenue",

            ],

        ),

    "fact_product_affinity":

        PipelineDefinition(

            name="fact_product_affinity",

            layer=GOLD,

            output_path=FACT_PRODUCT_AFFINITY_PATH,

            partition_column=PARTITION_COLUMN,

            file_format=DELTA,

            write_mode=OVERWRITE,

            required_columns=[
                "product_sku_1",
                "product_sku_2",
                "pair_count",
            ],

            key_columns=[
                "product_sku_1",
                "product_sku_2",
            ],

            non_negative_columns=[
                "pair_count",
            ],
        ),
}


# ==============================================================================
# Configuration Loader
# ==============================================================================

class PipelineConfig:

    def __init__(self, pipeline_name: str):

        if pipeline_name not in PIPELINE_CONFIG:

            raise ValueError(
                f"Unknown pipeline '{pipeline_name}'."
            )

        self.definition = PIPELINE_CONFIG[pipeline_name]

    @property
    def config(self) -> PipelineDefinition:
        return self.definition