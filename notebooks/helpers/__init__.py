"""
Helper functions for Databricks ETL pipeline.
Provides common utilities for database connections, logging, and data loading.
"""

from .audit import ChangeAuditLogger
from .bronze_loader import BronzeLoader
from .database import create_connection, load_bronze_table, safe_count, write_bronze_table, write_gold_table
from .gold_loader import GoldLoader
from .incremental_pipeline import IncrementalPipeline, TableConfig
from .logging_config import setup_logger
from .pipeline_utils import preload_dependencies
from .pipeline_utils import get_latest_batch_id
from .pipeline_utils import generate_batch_id
from .silver_transforms import (
    build_equipment_bridges,
    # Staff hierarchy
    build_staff_hierarchy_bridge,
    # Date dimension functions
    generate_date_dimension,
    generate_role_playing_date_dimensions,
    transform_car_full_pipeline,
    # Full pipeline functions (used by notebooks)
    transform_customer_full_pipeline,
    # Equipment bridges
    transform_equipment_dimension,
    transform_rental_fact,
    # Fact transformations
    transform_service_fact,
    transform_staff_full_pipeline,
    transform_manager_full_pipeline,
    transform_store_full_pipeline,
)
from .watermark import WatermarkManager


__all__ = [
    "create_connection",
    "load_bronze_table",
    "safe_count",
    "write_bronze_table",
    "write_gold_table",
    "setup_logger",
    "preload_dependencies",
    "get_latest_batch_id",
    "generate_batch_id",
    "WatermarkManager",
    "BronzeLoader",
    "GoldLoader",
    "ChangeAuditLogger",
    "IncrementalPipeline",
    "TableConfig",
    # Silver transforms (full pipelines only - cleaning functions are internal)
    "transform_customer_full_pipeline",
    "transform_staff_full_pipeline",
    "transform_manager_full_pipeline",
    "transform_store_full_pipeline",
    "transform_car_full_pipeline",
    # Date dimensions
    "generate_date_dimension",
    "generate_role_playing_date_dimensions",
    # Staff hierarchy
    "build_staff_hierarchy_bridge",
    # Equipment
    "transform_equipment_dimension",
    "build_equipment_bridges",
    # Facts
    "transform_service_fact",
    "transform_rental_fact",
]
