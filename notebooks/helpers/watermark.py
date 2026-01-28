"""
Watermark management for incremental data loading.
Tracks last successfully loaded timestamp per source table.
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


logger = logging.getLogger("wheelie_etl")


class WatermarkManager:
    """
    Manages watermarks for incremental data loading.

    Watermarks track the last successfully loaded timestamp for each source table,
    enabling incremental loading by only processing new/modified records.
    """

    def __init__(self, spark: SparkSession, schema: str = "wheelie.monitoring"):
        """
        Initialize watermark manager.

        Args:
            spark: SparkSession instance
            schema: Schema containing watermarks table. Default: 'wheelie.monitoring'
        """
        self.spark = spark
        self.watermark_table = f"{schema}.watermarks"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Verify watermarks table exists. Raises error if not found."""
        if not self.spark.catalog.tableExists(self.watermark_table):
            raise RuntimeError(
                f"Watermarks table {self.watermark_table} does not exist. "
                f"Please run setup_db.sql to create monitoring infrastructure."
            )

    def get_watermark(self, table_name: str) -> Optional[datetime]:
        """
        Get last successful watermark for a table.

        Args:
            table_name: Source table name (e.g., 'customer', 'rental')

        Returns:
            Watermark timestamp if exists, None for first load
        """
        logger.info(f"Checking watermark for {table_name}")

        result = (
            self.spark.table(self.watermark_table)
            .filter(col("table_name") == table_name)
            .select("watermark_timestamp")
            .collect()
        )

        if result:
            watermark = result[0][0]
            logger.info(f"Watermark found for {table_name}: {watermark}")
            return watermark
        else:
            logger.info(f"No watermark found for {table_name} - first load")
            return None

    def update_watermark(
        self,
        table_name: str,
        watermark_value: datetime,
        watermark_column: str,
        row_count: int = 0,
        load_type: str = "INCREMENTAL",
        updated_by: str = "etl_pipeline",
    ):
        """
        Update watermark after successful load.

        Args:
            table_name: Source table name
            watermark_value: New watermark timestamp (max value from loaded data)
            watermark_column: Column used for watermark (e.g., 'last_update')
            row_count: Number of rows loaded
            load_type: 'FULL' or 'INCREMENTAL'
            updated_by: Job/user identifier
        """
        logger.info(f"Updating watermark for {table_name}: {watermark_value}")

        # Prepare update DataFrame
        update_df = self.spark.createDataFrame(
            [
                (
                    table_name,
                    watermark_value,
                    watermark_column,
                    row_count,
                    load_type,
                    datetime.now(),
                    updated_by,
                )
            ],
            [
                "table_name",
                "watermark_timestamp",
                "watermark_column",
                "row_count",
                "load_type",
                "updated_at",
                "updated_by",
            ],
        )

        # MERGE: Update if exists, insert if new
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(self.spark, self.watermark_table)

        delta_table.alias("target").merge(
            update_df.alias("source"), "target.table_name = source.table_name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Watermark updated for {table_name}")

    def reset_watermark(self, table_name: str):
        """
        Reset watermark for a table (forces full reload next time).

        Args:
            table_name: Source table name
        """
        logger.warning(f"Resetting watermark for {table_name}")

        self.spark.sql(
            f"""
            DELETE FROM {self.watermark_table}
            WHERE table_name = '{table_name}'
        """
        )

        logger.info(f"Watermark reset for {table_name} - next load will be full")

    def get_all_watermarks(self):
        """
        Get all watermarks for monitoring/debugging.

        Returns:
            DataFrame with all watermark records
        """
        return self.spark.table(self.watermark_table).orderBy("table_name")

    def has_watermark(self, table_name: str) -> bool:
        """
        Check if watermark exists for a table.

        Args:
            table_name: Source table name

        Returns:
            True if watermark exists, False otherwise
        """
        return self.get_watermark(table_name) is not None
