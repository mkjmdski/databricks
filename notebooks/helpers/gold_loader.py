"""
Gold layer loading with SCD Type 1 and Type 2 support.
Handles dimension table updates with appropriate versioning strategies.
"""

import logging
from datetime import datetime
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from .audit import ChangeAuditLogger
from .database import write_gold_table


logger = logging.getLogger("wheelie_etl")


class GoldLoader:
    """
    Handles gold layer upserts with SCD Type 1 and Type 2 support.

    SCD Type 1: Overwrite existing records (simple update)
    SCD Type 2: Create new versions for changed records (track history)
    """

    def __init__(self, spark: SparkSession, batch_id: str | None = None):
        """
        Initialize gold loader.

        Args:
            spark: SparkSession instance
            batch_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.batch_id = batch_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.audit_logger = ChangeAuditLogger(spark, batch_id=self.batch_id)
        logger.info(f"GoldLoader initialized (batch_id: {self.batch_id})")

    def upsert(
        self,
        df: DataFrame,
        table_name: str,
        business_key: str,
        surrogate_key: str,
        scd_type: int = 1,
        tracking_columns: list[str] | None = None,
    ):
        """
        Upsert DataFrame to gold layer with SCD logic.

        Args:
            df: DataFrame to upsert (should already have surrogate keys)
            table_name: Gold table name (without schema)
            business_key: Business key column (e.g., 'customer_id')
            surrogate_key: Surrogate key column (e.g., 'customer_key')
            scd_type: 1 = Overwrite, 2 = Versioning
            tracking_columns: For SCD Type 2, columns that trigger new versions
        """
        logger.info(f"Upserting to gold: {table_name} (SCD Type {scd_type})")

        gold_table_path = f"wheelie.gold.{table_name}"

        # Check if table exists
        if not self.spark.catalog.tableExists(gold_table_path):
            # First load - just write
            logger.info(f"Gold table {gold_table_path} doesn't exist - creating")

            if scd_type == 2:
                # Add SCD2 columns for first load
                df = (
                    df.withColumn("effective_date", current_timestamp())
                    .withColumn("end_date", lit(None).cast("timestamp"))
                    .withColumn("is_current", lit(True))
                )

            write_gold_table(df, table_name, mode="overwrite", schema="gold")
            return

        # Merge into existing table
        if scd_type == 1:
            self._upsert_scd1(df, gold_table_path, business_key, surrogate_key, table_name)
        elif scd_type == 2:
            if not tracking_columns:
                raise ValueError("tracking_columns required for SCD Type 2")
            self._upsert_scd2(df, gold_table_path, business_key, surrogate_key, tracking_columns, table_name)
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")

    def _upsert_scd1(
        self, df_new: DataFrame, gold_table_path: str, business_key: str, surrogate_key: str, table_name: str
    ):
        """
        SCD Type 1: Overwrite existing records.

        Uses MERGE:
        - WHEN MATCHED: Update all columns
        - WHEN NOT MATCHED: Insert new record
        """
        delta_table = DeltaTable.forName(self.spark, gold_table_path)

        row_count = df_new.count()
        logger.info(f"SCD1: Processing {row_count:,} records for {table_name}")

        # Perform MERGE
        delta_table.alias("target").merge(
            df_new.alias("source"), f"target.{business_key} = source.{business_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Gold SCD1 upsert complete: {table_name}")

    def _upsert_scd2(
        self,
        df_new: DataFrame,
        gold_table_path: str,
        business_key: str,
        surrogate_key: str,
        tracking_columns: list[str],
        table_name: str,
    ):
        """
        SCD Type 2: Create new versions for changed records.

        Process:
        1. Find records where tracking columns changed
        2. Close old versions (set is_current=FALSE, end_date=now)
        3. Insert new versions (with new effective_date)
        4. Insert truly new records
        """
        delta_table = DeltaTable.forName(self.spark, gold_table_path)

        # Add SCD2 metadata to incoming data
        df_new = (
            df_new.withColumn("effective_date", current_timestamp())
            .withColumn("end_date", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        # Build change detection condition (null-safe)
        change_conditions = [f"NOT (target.{col} <=> source.{col})" for col in tracking_columns]
        change_condition_str = " OR ".join(change_conditions)

        # Step 1: Close expired records where tracking columns changed
        logger.info(f"SCD2: Closing old versions where {tracking_columns} changed")

        delta_table.alias("target").merge(
            df_new.alias("source"),
            f"target.{business_key} = source.{business_key} AND target.is_current = TRUE AND ({change_condition_str})",
        ).whenMatchedUpdate(set={"is_current": lit(False), "end_date": current_timestamp()}).execute()

        # Step 2: Insert only new or changed records
        current_df = (
            delta_table.toDF()
            .filter(col("is_current") == True)  # noqa: E712 - Spark column comparison
            .select(business_key, *tracking_columns)
            .alias("target")
        )

        source_df = df_new.alias("source")
        change_expr = reduce(
            lambda left, right: left | right,
            [~col(f"source.{c}").eqNullSafe(col(f"target.{c}")) for c in tracking_columns],
        )

        df_to_insert = (
            source_df.join(current_df, on=business_key, how="left")
            .filter(col(f"target.{business_key}").isNull() | change_expr)
            .select("source.*")
        )

        insert_count = df_to_insert.count()
        logger.info(f"SCD2: Inserting {insert_count:,} records (new + changed versions)")
        if insert_count > 0:
            write_gold_table(df_to_insert, table_name, mode="append", schema="gold")

        logger.info(f"Gold SCD2 upsert complete: {table_name}")
