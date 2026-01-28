"""
DRY incremental loading pipeline orchestration.
Configuration-driven approach to run incremental loads for dimensions and facts.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .bronze_loader import BronzeLoader
from .gold_loader import GoldLoader


logger = logging.getLogger("wheelie_etl")


@dataclass
class TableConfig:
    """
    Configuration for a single table's incremental load.

    Attributes:
        table_name: Source table name in MySQL and bronze layer
        business_key: Business key column (e.g., 'customer_id')
        surrogate_key: Surrogate key column for gold (e.g., 'customer_key')
        watermark_column: Column to track for incremental loading
        scd_type: 1 for overwrite, 2 for versioning
        tracking_columns: For SCD2, columns that trigger new versions
        gold_table_name: Gold table name (defaults to dim_{table_name})
        silver_transform: Function to transform bronze→silver→gold DataFrame
    """

    table_name: str
    business_key: str
    surrogate_key: str
    watermark_column: str = "last_update"
    scd_type: int = 1
    tracking_columns: list[str] | None = None
    gold_table_name: str | None = None
    silver_transform: Callable[[DataFrame], DataFrame] | None = None

    def __post_init__(self):
        """Set defaults after initialization."""
        if self.gold_table_name is None:
            self.gold_table_name = f"dim_{self.table_name}"
        if self.scd_type == 2 and not self.tracking_columns:
            raise ValueError(f"tracking_columns required for SCD Type 2: {self.table_name}")


class IncrementalPipeline:
    """
    DRY orchestration for incremental loading.

    Runs complete pipeline:
    1. Load incremental from MySQL → Bronze (with watermark filtering)
    2. Merge to bronze Delta table (upsert)
    3. Apply silver transformations (custom per table)
    4. Upsert to gold (SCD Type 1 or 2)
    5. Update watermark

    Usage:
        pipeline = IncrementalPipeline(spark, dbutils, batch_id="20260128_120000")

        # Define table config
        customer_config = TableConfig(
            table_name="customer",
            business_key="customer_id",
            surrogate_key="customer_key",
            watermark_column="last_update",
            scd_type=1,
            silver_transform=transform_customer_silver
        )

        # Run incremental load
        pipeline.load_table(customer_config)
    """

    def __init__(self, spark: SparkSession, dbutils, batch_id: str | None = None):
        """
        Initialize pipeline.

        Args:
            spark: SparkSession instance
            dbutils: DBUtils for secret management
            batch_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.dbutils = dbutils
        self.batch_id = batch_id
        self.bronze_loader = BronzeLoader(spark, dbutils, batch_id)
        self.gold_loader = GoldLoader(spark, batch_id)
        logger.info(f"IncrementalPipeline initialized (batch_id: {batch_id})")

    def load_table(self, config: TableConfig, force_full: bool = False) -> dict[str, any]:
        """
        Run incremental load for a single table.

        Args:
            config: Table configuration
            force_full: Force full reload even if watermark exists

        Returns:
            Dictionary with load metrics (row_count, load_type, duration, etc.)
        """
        import time

        start_time = time.time()

        logger.info("=" * 70)
        logger.info(f"INCREMENTAL LOAD: {config.table_name} → {config.gold_table_name}")
        logger.info("=" * 70)

        try:
            # Step 1: Load incremental from MySQL
            df_bronze_incremental = self.bronze_loader.load_incremental(
                table_name=config.table_name, watermark_column=config.watermark_column, force_full=force_full
            )

            row_count = df_bronze_incremental.count()
            load_type = (
                "FULL"
                if force_full or not self.bronze_loader.watermark_manager.has_watermark(config.table_name)
                else "INCREMENTAL"
            )

            # Step 2: Merge to bronze Delta table
            self.bronze_loader.merge_to_bronze(
                df=df_bronze_incremental, table_name=config.table_name, business_key=config.business_key
            )

            # Step 3: Apply silver transformations
            if config.silver_transform:
                logger.info(f"Applying silver transformations for {config.table_name}")
                # Load current bronze state for transformation
                df_bronze_current = self.spark.table(f"wheelie.bronze.{config.table_name}")
                df_gold = config.silver_transform(df_bronze_current)
            else:
                logger.info(f"No silver transformation - using bronze as-is for {config.table_name}")
                df_gold = df_bronze_incremental

            # Step 4: Upsert to gold
            self.gold_loader.upsert(
                df=df_gold,
                table_name=config.gold_table_name,
                business_key=config.business_key,
                surrogate_key=config.surrogate_key,
                scd_type=config.scd_type,
                tracking_columns=config.tracking_columns,
            )

            # Step 5: Update watermark
            self.bronze_loader.update_watermark(
                table_name=config.table_name,
                df=df_bronze_incremental,
                watermark_column=config.watermark_column,
                load_type=load_type,
            )

            duration = time.time() - start_time
            logger.info("=" * 70)
            logger.info(f"SUCCESS: {config.table_name} loaded in {duration:.2f}s ({row_count:,} rows)")
            logger.info("=" * 70)

            return {
                "table_name": config.table_name,
                "gold_table_name": config.gold_table_name,
                "row_count": row_count,
                "load_type": load_type,
                "duration_seconds": duration,
                "status": "SUCCESS",
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"FAILED: {config.table_name} after {duration:.2f}s - {str(e)}")
            raise

    def load_tables(self, configs: list[TableConfig], force_full: bool = False) -> list[dict[str, any]]:
        """
        Run incremental load for multiple tables sequentially.

        Args:
            configs: List of table configurations
            force_full: Force full reload for all tables

        Returns:
            List of dictionaries with load metrics per table
        """
        results = []

        logger.info("=" * 70)
        logger.info(f"BATCH INCREMENTAL LOAD: {len(configs)} tables")
        logger.info("=" * 70)

        for config in configs:
            try:
                result = self.load_table(config, force_full=force_full)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to load {config.table_name}: {str(e)}")
                results.append(
                    {
                        "table_name": config.table_name,
                        "gold_table_name": config.gold_table_name,
                        "status": "FAILED",
                        "error": str(e),
                    }
                )
                # Continue with next table instead of failing entire batch
                continue

        # Summary
        success_count = sum(1 for r in results if r.get("status") == "SUCCESS")
        failed_count = len(results) - success_count
        total_rows = sum(r.get("row_count", 0) for r in results)

        logger.info("=" * 70)
        logger.info("BATCH LOAD COMPLETE")
        logger.info(f"  Success: {success_count}/{len(configs)} tables")
        logger.info(f"  Failed: {failed_count}/{len(configs)} tables")
        logger.info(f"  Total rows: {total_rows:,}")
        logger.info("=" * 70)

        return results
