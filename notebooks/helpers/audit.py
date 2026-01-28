"""
Change audit logging for tracking data modifications.
Records INSERT, UPDATE, and SCD2 operations for compliance and debugging.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


logger = logging.getLogger("wheelie_etl")


class ChangeAuditLogger:
    """
    Logs data changes to audit table for compliance and debugging.

    Tracks:
    - INSERT: New records added
    - UPDATE: Existing records modified (SCD Type 1)
    - SCD2_NEW_VERSION: New version created (SCD Type 2)
    - SCD2_CLOSE_OLD: Old version closed (SCD Type 2)
    """

    def __init__(self, spark: SparkSession, batch_id: Optional[str] = None, schema: str = "wheelie.monitoring"):
        """
        Initialize audit logger.

        Args:
            spark: SparkSession instance
            batch_id: Unique identifier for ETL batch (e.g., timestamp)
            schema: Schema containing change_audit table
        """
        self.spark = spark
        self.audit_table = f"{schema}.change_audit"
        self.batch_id = batch_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Verify audit table exists."""
        if not self.spark.catalog.tableExists(self.audit_table):
            raise RuntimeError(
                f"Audit table {self.audit_table} does not exist. "
                f"Please run setup_db.sql to create monitoring infrastructure."
            )

    def log_insert(
        self,
        table_name: str,
        business_key: str,
        surrogate_key: str,
        new_values: Dict[str, str],
    ):
        """
        Log INSERT operation (new record).

        Args:
            table_name: Target table name (e.g., 'dim_customer')
            business_key: Business key value (e.g., 'customer_id=123')
            surrogate_key: Surrogate key value (e.g., 'customer_key=456')
            new_values: Dictionary of column values
        """
        self._log_change(
            table_name=table_name,
            operation="INSERT",
            business_key=business_key,
            surrogate_key=surrogate_key,
            changed_columns=list(new_values.keys()),
            old_values={},
            new_values=new_values,
        )

    def log_update(
        self,
        table_name: str,
        business_key: str,
        surrogate_key: str,
        changed_columns: List[str],
        old_values: Dict[str, str],
        new_values: Dict[str, str],
    ):
        """
        Log UPDATE operation (SCD Type 1 overwrite).

        Args:
            table_name: Target table name
            business_key: Business key value
            surrogate_key: Surrogate key value
            changed_columns: List of columns that changed
            old_values: Old values for changed columns
            new_values: New values for changed columns
        """
        self._log_change(
            table_name=table_name,
            operation="UPDATE",
            business_key=business_key,
            surrogate_key=surrogate_key,
            changed_columns=changed_columns,
            old_values=old_values,
            new_values=new_values,
        )

    def log_scd2_new_version(
        self,
        table_name: str,
        business_key: str,
        surrogate_key: str,
        changed_columns: List[str],
        old_values: Dict[str, str],
        new_values: Dict[str, str],
    ):
        """
        Log SCD Type 2 new version creation.

        Args:
            table_name: Target table name
            business_key: Business key value
            surrogate_key: Surrogate key value (NEW version)
            changed_columns: List of tracking columns that changed
            old_values: Old values from previous version
            new_values: New values in new version
        """
        self._log_change(
            table_name=table_name,
            operation="SCD2_NEW_VERSION",
            business_key=business_key,
            surrogate_key=surrogate_key,
            changed_columns=changed_columns,
            old_values=old_values,
            new_values=new_values,
        )

    def log_scd2_close_old(
        self,
        table_name: str,
        business_key: str,
        surrogate_key: str,
    ):
        """
        Log SCD Type 2 old version closure.

        Args:
            table_name: Target table name
            business_key: Business key value
            surrogate_key: Surrogate key value (OLD version being closed)
        """
        self._log_change(
            table_name=table_name,
            operation="SCD2_CLOSE_OLD",
            business_key=business_key,
            surrogate_key=surrogate_key,
            changed_columns=["is_current", "end_date"],
            old_values={"is_current": "TRUE", "end_date": "NULL"},
            new_values={"is_current": "FALSE", "end_date": str(datetime.now())},
        )

    def _log_change(
        self,
        table_name: str,
        operation: str,
        business_key: str,
        surrogate_key: str,
        changed_columns: List[str],
        old_values: Dict[str, str],
        new_values: Dict[str, str],
    ):
        """
        Internal method to write audit record.

        Args:
            table_name: Target table name
            operation: Operation type (INSERT, UPDATE, SCD2_NEW_VERSION, SCD2_CLOSE_OLD)
            business_key: Business key value
            surrogate_key: Surrogate key value
            changed_columns: List of changed column names
            old_values: Old values dictionary
            new_values: New values dictionary
        """
        try:
            # Create audit record
            audit_record = self.spark.createDataFrame(
                [
                    (
                        table_name,
                        operation,
                        business_key,
                        surrogate_key,
                        changed_columns,
                        old_values,
                        new_values,
                        datetime.now(),
                        self.batch_id,
                    )
                ],
                [
                    "table_name",
                    "operation",
                    "business_key",
                    "surrogate_key",
                    "changed_columns",
                    "old_values",
                    "new_values",
                    "audit_timestamp",
                    "batch_id",
                ],
            )

            # Append to audit table
            audit_record.write.format("delta").mode("append").saveAsTable(self.audit_table)

            logger.debug(f"Audit logged: {operation} on {table_name} - {business_key}")

        except Exception as e:
            # Log error but don't fail the ETL job
            logger.error(f"Failed to write audit log: {str(e)}")

    def log_bulk_changes(self, changes_df: DataFrame, table_name: str, operation: str):
        """
        Log multiple changes efficiently (bulk logging).

        Args:
            changes_df: DataFrame with columns: business_key, surrogate_key, changed_columns, old_values, new_values
            table_name: Target table name
            operation: Operation type
        """
        logger.info(f"Logging {changes_df.count()} {operation} operations for {table_name}")

        # Add audit metadata
        audit_df = changes_df.withColumn("table_name", lit(table_name)).withColumn(
            "operation", lit(operation)
        ).withColumn("audit_timestamp", current_timestamp()).withColumn("batch_id", lit(self.batch_id))

        # Append to audit table
        audit_df.write.format("delta").mode("append").saveAsTable(self.audit_table)

    def get_audit_summary(self, table_name: Optional[str] = None, batch_id: Optional[str] = None) -> DataFrame:
        """
        Get audit log summary for monitoring.

        Args:
            table_name: Optional filter by table name
            batch_id: Optional filter by batch ID

        Returns:
            DataFrame with audit records
        """
        df = self.spark.table(self.audit_table)

        if table_name:
            df = df.filter(col("table_name") == table_name)

        if batch_id:
            df = df.filter(col("batch_id") == batch_id)

        return df.groupBy("table_name", "operation", "batch_id").count().orderBy("table_name", "operation")
