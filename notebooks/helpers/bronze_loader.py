"""
Bronze layer incremental loading.
Handles extraction from MySQL and merging to bronze Delta tables.
"""

import logging
from datetime import datetime
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

from .database import create_connection, write_bronze_table
from .watermark import WatermarkManager
from .audit import ChangeAuditLogger


logger = logging.getLogger("wheelie_etl")


class BronzeLoader:
    """
    Handles bronze layer incremental data loading.

    Workflow:
    1. Load incremental data from source (WHERE last_update > watermark)
    2. Merge into bronze Delta table (upsert by business key)
    """

    def __init__(self, spark: SparkSession, dbutils, batch_id: Optional[str] = None):
        """
        Initialize bronze loader.

        Args:
            spark: SparkSession instance
            dbutils: DBUtils for secret management
            batch_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.dbutils = dbutils
        self.batch_id = batch_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.watermark_manager = WatermarkManager(spark)
        self.audit_logger = ChangeAuditLogger(spark, batch_id=self.batch_id)
        logger.info(f"BronzeLoader initialized (batch_id: {self.batch_id})")

    def load_incremental(
        self, table_name: str, watermark_column: str = "last_update", force_full: bool = False
    ) -> DataFrame:
        """
        Load incremental data from MySQL source.

        Args:
            table_name: Source table name in MySQL
            watermark_column: Column to use for incremental filtering
            force_full: Force full load even if watermark exists

        Returns:
            DataFrame with incremental data (or full data if first load)
        """
        logger.info(f"Loading incremental bronze for {table_name} (column: {watermark_column})")

        # Get watermark
        watermark = self.watermark_manager.get_watermark(table_name) if not force_full else None

        # Create JDBC connection
        conn = create_connection(self.spark, self.dbutils)

        # Build query
        if watermark:
            # Incremental load
            query = f"(SELECT * FROM {table_name} WHERE {watermark_column} > '{watermark}') AS subset"
            logger.info(f"Incremental query: {watermark_column} > {watermark}")
        else:
            # Full load (first time)
            query = table_name
            logger.info("First load - fetching all records")

        # Load data
        df = conn.option("dbtable", query).load()

        # Add metadata
        df_bronze = df.withColumn("_ingestion_ts", current_timestamp()).withColumn("_source", lit("mysql_wheelie"))

        row_count = df_bronze.count()
        load_type = "FULL" if watermark is None else "INCREMENTAL"
        logger.info(f"Bronze loaded: {table_name} ({row_count:,} rows, {load_type})")

        return df_bronze

    def merge_to_bronze(self, df: DataFrame, table_name: str, business_key: str):
        """
        Merge incremental data into bronze Delta table.

        Uses Delta Lake MERGE to upsert records:
        - Update existing records (matched on business key)
        - Insert new records

        Args:
            df: Incremental DataFrame to merge
            table_name: Bronze table name (without schema)
            business_key: Column name for business key (e.g., 'customer_id')
        """
        logger.info(f"Merging to bronze table: {table_name}")

        bronze_table_path = f"wheelie.bronze.{table_name}"

        # Check if table exists
        if not self.spark.catalog.tableExists(bronze_table_path):
            # First load - just write
            logger.info(f"Bronze table {bronze_table_path} doesn't exist - creating with initial data")
            write_bronze_table(df, table_name, mode="overwrite")
            return

        # Merge into existing table
        delta_table = DeltaTable.forName(self.spark, bronze_table_path)

        delta_table.alias("target").merge(
            df.alias("source"), f"target.{business_key} = source.{business_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Bronze merge complete for {table_name}")

    def update_watermark(self, table_name: str, df: DataFrame, watermark_column: str, load_type: str):
        """
        Update watermark after successful load.

        Args:
            table_name: Source table name
            df: Loaded DataFrame
            watermark_column: Column used for watermark
            load_type: 'FULL' or 'INCREMENTAL'
        """
        # Get max watermark value from loaded data
        max_watermark_row = df.agg({watermark_column: "max"}).collect()
        max_watermark = max_watermark_row[0][0] if max_watermark_row else None

        if max_watermark:
            row_count = df.count()
            self.watermark_manager.update_watermark(
                table_name=table_name,
                watermark_value=max_watermark,
                watermark_column=watermark_column,
                row_count=row_count,
                load_type=load_type,
                updated_by=f"batch_{self.batch_id}",
            )
            logger.info(f"Watermark updated for {table_name}: {max_watermark}")
        else:
            logger.warning(f"No watermark value found for {table_name} - skipping update")
