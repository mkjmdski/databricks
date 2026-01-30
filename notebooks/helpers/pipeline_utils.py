"""
Shared helpers for incremental pipelines.
"""

import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


logger = logging.getLogger("wheelie_etl")


def generate_batch_id() -> str:
    """Generate batch_id in yearmonthdayhour format."""
    return datetime.now().strftime("%Y%m%d%H")


def preload_dependencies(
    pipeline,
    dependency_tables: list[str],
    watermark_overrides: dict[str, str | None] | None = None,
    business_key_overrides: dict[str, str] | None = None,
):
    """
    Load dependency tables into bronze and update watermarks when possible.

    Args:
        pipeline: IncrementalPipeline instance
        dependency_tables: List of tables to load into bronze
        watermark_overrides: Map of table -> watermark column name or None (force full)
        business_key_overrides: Map of table -> business key column
    """
    watermark_overrides = watermark_overrides or {}
    business_key_overrides = business_key_overrides or {}

    if not dependency_tables:
        return

    logger.info(f"Pre-loading {len(dependency_tables)} dependency tables to bronze")

    for table in dependency_tables:
        try:
            watermark_column = watermark_overrides.get(table, "last_update")
            force_full = watermark_column is None

            logger.info(f"Loading dependency: {table}")
            df = pipeline.bronze_loader.load_incremental(
                table_name=table,
                watermark_column=watermark_column or "last_update",
                force_full=force_full,
            )
            row_count = df.count()

            business_key = business_key_overrides.get(table, f"{table}_id")
            pipeline.bronze_loader.merge_to_bronze(
                df=df,
                table_name=table,
                business_key=business_key,
            )

            if watermark_column and watermark_column in df.columns:
                load_type = (
                    "FULL"
                    if not pipeline.bronze_loader.watermark_manager.has_watermark(table)
                    else "INCREMENTAL"
                )
                pipeline.bronze_loader.update_watermark(
                    table_name=table,
                    df=df,
                    watermark_column=watermark_column,
                    load_type=load_type,
                )
            else:
                logger.warning(f"No watermark column for {table}; skipping watermark update")

            logger.info(f"✅ {table}: {row_count:,} rows loaded")
        except Exception as e:
            logger.error(f"❌ Failed to load {table}: {str(e)}")
            raise


def get_latest_batch_id(spark: SparkSession, table_name: str) -> str | None:
    """
    Resolve the latest batch_id for a table from the monitoring table.

    This expects updated_by to follow the pattern "batch_{batch_id}".
    """
    watermarks_table = "wheelie.monitoring.watermarks"
    if not spark.catalog.tableExists(watermarks_table):
        return None

    latest = (
        spark.table(watermarks_table)
        .filter(col("table_name") == table_name)
        .orderBy(col("updated_at").desc())
        .limit(1)
        .collect()
    )
    if not latest:
        return None

    updated_by = latest[0].get("updated_by")
    if not updated_by or not updated_by.startswith("batch_"):
        return None

    return updated_by.replace("batch_", "", 1)
