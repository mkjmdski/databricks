"""
Database connection and data loading utilities.
Implements bronze→silver→gold medallion pattern helpers.
"""

import logging
from contextlib import suppress

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


# Get logger
logger = logging.getLogger("wheelie_etl")


def create_connection(spark, dbutils):
    """
    Create JDBC connection to MySQL source database.
    Uses dbutils.secrets for secure credential management.

    Args:
        spark: SparkSession from notebook context
        dbutils: DBUtils from notebook context

    Returns:
        DataFrameReader configured for JDBC connection

    Raises:
        Exception: If connection creation fails
    """
    try:
        return (
            spark.read.format("jdbc")
            .option(
                "url",
                f"jdbc:mysql://{dbutils.secrets.get('wheelie', 'MYSQL_HOST')}/{dbutils.secrets.get('wheelie', 'MYSQL_DB')}",
            )
            .option("user", dbutils.secrets.get("wheelie", "MYSQL_USERNAME"))
            .option("password", dbutils.secrets.get("wheelie", "MYSQL_PASSWORD"))
        )
    except Exception as e:
        logger.error(f"Failed to create connection: {str(e)}")
        raise


def load_bronze_table(conn, table_name: str) -> DataFrame:
    """
    BRONZE LAYER: Load raw table from MySQL as in-memory DataFrame.

    - No transformations applied
    - Adds ingestion metadata (_ingestion_ts, _source)
    - Preserves all source columns

    Args:
        conn: JDBC connection reader (from create_connection())
        table_name: Source table name in MySQL

    Returns:
        DataFrame with raw data + metadata columns

    Raises:
        Exception: If table load fails
    """
    logger.info(f"BRONZE: Loading {table_name}")
    try:
        df = conn.option("dbtable", table_name).load()
        df_bronze = df.withColumn("_ingestion_ts", current_timestamp()).withColumn("_source", lit("mysql_wheelie"))
        row_count = df_bronze.count()
        logger.info(f"BRONZE: {table_name} loaded ({row_count:,} rows)")
        return df_bronze
    except Exception as e:
        logger.error(f"Failed to load bronze table {table_name}: {str(e)}")
        raise


def write_gold_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    display_data: bool = False,
    partition_by: list | None = None,
    schema: str = "gold",
):
    """
    GOLD LAYER: Write DataFrame to Delta table in data warehouse.

    Args:
        df: DataFrame to persist
        table_name: Target table name (without schema prefix)
        mode: Write mode ('overwrite', 'append', 'merge'). Default: 'overwrite'
        display_data: Whether to display DataFrame before writing. Default: False
        partition_by: DEPRECATED - No longer used. Small dataset (<1Tb data) does not benefit from partitioning.
        schema: Target schema name. Default: 'gold'

    Raises:
        Exception: If write operation fails

    Note:
        Partitioning is not used for this university project due to small data volumes (<1Tb of rows).
    """
    logger.info(f"GOLD: Writing {table_name}")
    try:
        row_count = df.count()

        if display_data:
            with suppress(NameError):
                display(df)  # Databricks-specific function, ignore in linting

        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")

        # Partitioning removed - not beneficial for small datasets
        if partition_by:
            logger.warning(f"GOLD: partition_by parameter ignored for {table_name} (not beneficial for <3k rows)")

        full_table_name = f"wheelie.{schema}.{table_name}"
        writer.saveAsTable(full_table_name)
        logger.info(f"GOLD: {table_name} written to {full_table_name} ({row_count:,} rows)")
    except Exception as e:
        logger.error(f"Failed to write gold table {table_name}: {str(e)}")
        raise


def write_bronze_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
):
    """
    BRONZE LAYER: Write DataFrame to Delta table in bronze schema.

    Used for incremental loading - persists raw data from source.

    Args:
        df: DataFrame to persist
        table_name: Target table name (without schema prefix)
        mode: Write mode ('overwrite', 'append', 'merge'). Default: 'overwrite'

    Raises:
        Exception: If write operation fails
    """
    logger.info(f"BRONZE: Writing {table_name}")
    try:
        row_count = df.count()
        full_table_name = f"wheelie.bronze.{table_name}"

        df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full_table_name)

        logger.info(f"BRONZE: {table_name} written to {full_table_name} ({row_count:,} rows)")
    except Exception as e:
        logger.error(f"Failed to write bronze table {table_name}: {str(e)}")
        raise


def safe_count(spark, table_name: str, schema: str = "gold", catalog: str = "wheelie") -> str:
    """
    Return a formatted row count string for a table if it exists.

    Args:
        spark: SparkSession instance
        table_name: Table name without schema/catalog
        schema: Schema name (default: "gold")
        catalog: Catalog name (default: "wheelie")

    Returns:
        String with formatted count or an N/A message if table is missing.
    """
    full_name = f"{catalog}.{schema}.{table_name}"
    if not spark.catalog.tableExists(full_name):
        return "N/A (table not found)"
    return f"{spark.table(full_name).count():,}"
