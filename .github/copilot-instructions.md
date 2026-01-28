# Databricks ETL Pipeline Refactoring Plan - Copilot Instructions

## Executive Summary
This document provides a comprehensive review and refactoring plan for the Wheelie car rental data warehouse ETL pipeline (university project). The current implementation in `load_all_dim.ipynb` and `load_all_facts.ipynb` requires refactoring to improve code quality, implement in-memory medallion architecture, support incremental loading, and follow ETL best practices.

**Project Scope:** University project with simplified requirements:
- **Data Volume:** Hundreds to ~3,000 rows maximum
- **Medallion Architecture:** In-memory DataFrames (not separate Delta tables)
- **Environment:** Single environment (no dev/staging/prod)
- **Data Quality:** Log failures only; assume source DB is clean
- **SCD Type 2:** Only for `dim_customer` as currently designed

---

## Current State Analysis

### 1. **Code Structure Issues**

#### Problem Areas:
- **Monolithic notebook design**: All dimension loads are in a single notebook with 7 cells
- **Mixed responsibilities**: Helper functions, data loading, transformations, and writes all intermixed
- **No separation of concerns**: Business logic, data access, and transformations are tightly coupled
- **Hard to test**: No modularity makes unit testing impossible
- **Poor reusability**: Code is duplicated across dimensions

#### Current Cell Structure:
```
Cell 1: Connection setup + helper functions
Cell 2: Load all source tables (11+ tables loaded at once)
Cell 3: DIM_DATE generation (500+ lines)
Cell 4: DIM_STAFF + STAFF_HIERARCHY (complex hierarchy logic)
Cell 5: DIM_STORE (joins across 3 tables)
Cell 6: DIM_CAR + Equipment bridges (complex many-to-many)
Cell 7: DIM_CUSTOMER (simple transformation)
```

---

### 2. **DRY/KISS/SOLID Violations**

#### **DRY (Don't Repeat Yourself) Violations:**

1. **Connection Pattern Repeated:**
   ```python
   # Repeated in every notebook
   c = create_connection()
   ```

2. **Join Pattern Duplication:**
   ```python
   # Similar join pattern repeated 5+ times
   address_with_city_df = address_df.join(city_df, address_df.city_id == city_df.city_id, "left")
   address_with_location_df = address_df.join(city_with_country_df, "city_id")
   store_with_address_df = store_df.join(address_df, ...)
   ```

3. **Hash Key Generation Repeated:**
   ```python
   # Pattern repeated for every dimension
   .withColumn("staff_key", xxhash64(col("staff_id")))
   .withColumn("customer_key", xxhash64(col("customer_id")))
   .withColumn("car_key", xxhash64(col("inventory_id")))
   ```

4. **Write Pattern Duplicated:**
   ```python
   # Called 10+ times with same options
   write_dim(df, "dim_staff")
   write_dim(df, "dim_store")
   # ... repeated
   ```

#### **KISS (Keep It Simple, Stupid) Violations:**

1. **Overly Complex Staff Hierarchy:**
   - 80+ lines for iterative hierarchy building
   - Manual depth management (`max_depth = 10`)
   - Complex array manipulation that could use recursive CTEs
   - Difficult to understand logic flow

2. **Equipment Bridge Complexity:**
   - Multiple transformations for equipment grouping
   - `array_sort(collect_set(...))` followed by `concat_ws` followed by hash
   - Could be simplified with proper bridge table design

3. **Date Dimension Generation:**
   - 200+ lines with verbose `when().when().when()...` chains
   - Hardcoded date logic that could use lookup tables
   - COVID flags manually defined (not configurable)

#### **SOLID Violations:**

1. **Single Responsibility Principle (SRP):**
   - `create_connection()` function mixes secret retrieval and connection creation
   - `write_dim()` function handles display, write, and schema management
   - Cells mix data loading, transformation, and persistence

2. **Open/Closed Principle (OCP):**
   - Cannot extend dimension loading without modifying core code
   - Adding new dimensions requires copy-paste of entire patterns
   - No abstraction layer for dimension types

3. **Dependency Inversion Principle (DIP):**
   - Hard dependency on `dbutils.secrets` (cannot test without Databricks)
   - Direct JDBC connection (no abstraction layer)
   - Tightly coupled to Delta Lake format

---

### 3. **Typos and Code Quality Issues**

#### **Typos Found:**

1. **Cell 3 - Polish Comments:**
   ```python
   # Line 55: "PEŁNE ROZWIĄZANIE" (Polish: Full Solution)
   # Line 57: "NAJLEPSZA, NAJNIŻSZA, NAJSTABLINIEJSZA" (Polish: Best, Lowest, Most Stable)
   # Line 61: "KONFIGURACJA" (Polish: Configuration)
   # Line 66: "GENEROWANIE SEKWENCJI DAT" (Polish: Date Sequence Generation)
   ```
   **Action:** Translate all comments to English for international team collaboration

2. **Cell 4 - Variable Naming:**
   ```python
   # Inconsistent naming
   staff_hierarchy  # snake_case
   bridge_df       # abbreviated
   dim_staff       # clear
   ```

3. **Cell 5 - Comment Quality:**
   ```python
   # "7. TRZECI JOIN" (Polish: Third Join) - but it's labeled as 7th?
   ```

#### **Data Quality Issues:**

1. **Fuel Type Data Correction (Cell 6):**
   ```python
   inventory_df = inventory_df.withColumn(
       "fuel_type",
       F.when(F.lower(col("fuel_type")) == "diesle", "Diesel")  # Typo in source data
        .when(F.lower(col("fuel_type")) == "petol", "Petrol")  # Typo in source data
        .otherwise(col("fuel_type"))
   )
   ```
   **Issue:** Data quality fixes should be in Bronze → Silver transformation, not in Gold layer

#### **Missing Documentation:**

1. No docstrings for functions
2. No cell-level descriptions (except dim_date)
3. No schema validation comments
4. No data lineage documentation

---

### 4. **ETL Best Practices - Major Gaps**

#### **A. No Medallion Architecture (Bronze/Silver/Gold)**

**Current Implementation:**
```
Source MySQL → Direct to Gold (Delta Tables)
```

**Problem:** No intermediate layers for:
- Raw data preservation (bronze)
- Data quality checks (silver)
- Business logic separation (gold)

**Expected Architecture (Simplified for University Project):**
```
Source MySQL → Bronze DataFrame (in-memory, raw)
            ↓
         Silver DataFrame (in-memory, cleaned)
            ↓
         Gold Delta Tables (persisted, business logic)
```

**Note:** For this project, we'll use in-memory DataFrames for bronze/silver layers and only persist the final gold layer to Delta tables. This is acceptable given the small data volumes (<3k rows).

#### **B. No Incremental Loading**

**Current Code:**
```python
# Cell 1
df.write.format("delta") \
    .mode("overwrite") \      # ← Full overwrite every time!
    .option("overwriteSchema", "true") \
    .saveAsTable(...)
```

**Problems:**
1. **Inefficient**: Reprocesses all data every run
2. **No CDC**: Cannot track changes
3. **SCD Type 2 Not Implemented**: `dim_customer` spec requires SCD2, but code does full overwrite
4. **Loss of History**: Overwrite mode destroys historical data
5. **Performance**: Scans entire source table every time

**What's Needed:**
- Watermark-based incremental loading
- Change Data Capture (CDC) for SCD Type 2
- Merge/Upsert operations instead of overwrite
- Last updated timestamp tracking

#### **C. No Data Quality Checks**

Missing:
- Schema validation
- Null checks on required fields
- Data type validation
- Referential integrity checks
- Business rule validation (e.g., `rental_rate > 0`)

**Simplified Approach:** For this university project, implement basic validation with logging. Failed validations should log warnings but not block processing (assume source DB is mostly clean).

#### **D. No Error Handling**

```python
# Current code has zero try-catch blocks
c = create_connection()  # What if connection fails?
df = c.option("dbtable", "inventory").load()  # What if table doesn't exist?
write_dim(df, "dim_store")  # What if write fails?
```

**Simplified Approach:** Add try-catch blocks with logging. On failure, log error details and raise exception to fail the job.

#### **E. No Logging**

- No start/end timestamps
- No row count tracking
- No error logging
- No audit trail
- Cannot debug failures

#### **F. No Idempotency**

- Running twice produces different results (surrogate keys)
- No transaction management
- Cannot safely retry failed jobs

#### **G. No Testing**

- No unit tests
- No integration tests
- No data validation tests
- Cannot verify transformations are correct

---

### 5. **Schema Mapping Issues (vs. warehouse.dbml)**

#### **Missing Columns:**

1. **dim_customer (SCD Type 2 not implemented):**
   ```python
   # Current code missing:
   - effective_date
   - end_date
   - is_current
   ```

2. **dim_staff:**
   ```python
   # Current code missing:
   - manager_staff_key (using staff_manager_key instead)
   - manager_name (using first+last separately)
   ```

3. **dim_store:**
   ```python
   # Current code has: store_manager_first_name, store_manager_last_name
   # Schema expects: store_manager_id only
   ```

4. **dim_date:**
   ```python
   # Current code uses: date_key (hash)
   # Schema expects: ID (auto-increment surrogate key)
   ```

5. **dim_car:**
   ```python
   # Current code missing:
   - create_date
   # Has car_key (hash), schema expects ID (auto-increment)
   ```

#### **Incorrect Key Generation:**

**Current Implementation:**
```python
.withColumn("staff_key", xxhash64(col("staff_id")))
.withColumn("customer_key", xxhash64(col("customer_id")))
```

**Problem:**
- Using xxhash64 produces non-deterministic keys across runs
- SCD Type 2 requires separate surrogate key (auto-increment ID) from business key
- Hash collisions possible (though rare with 64-bit)

**Schema Expectation:**
```sql
-- Separate surrogate key (PK) and business key
ID int(10) [pk, not null, increment]  -- Surrogate key
customer_id int(10) [not null]         -- Business key
```

#### **Denormalization Mismatches:**

1. **dim_staff location denormalization:**
   - Code: Joins `address_with_location_df` to get `staff_city`, `staff_country`
   - Schema: Expects `city`, `country` (without "staff_" prefix)

2. **dim_store denormalization:**
   - Code: Denormalizes manager first/last name
   - Schema: Only expects `store_manager_id` FK

---

### 6. **Performance Issues**

1. **Loading All Tables Upfront (Cell 2):**
   ```python
   # Loads 11+ tables into memory even if not all needed
   inventory_df = c.option("dbtable", "inventory").load()
   car_df = c.option("dbtable", "car").load()
   # ... 9 more tables
   ```

2. **Cartesian Product Risk:**
   ```python
   # No explicit join conditions in some cases
   hierarchy.join(staff_hierarchy.select(...), ..., "left")
   ```

3. **Multiple Passes Over Data:**
   - Staff hierarchy iterates 10 times over same data
   - Equipment grouping does multiple aggregations

4. **No Partitioning Strategy:**
   - No partition columns defined
   - No Z-ordering hints
   - No optimize commands

---

### 7. **Missing Infrastructure for Databricks Pipeline**

#### **Current Terraform (infra/main.tf):**
```terraform
# Only has: databricks_repo resource
# Missing: databricks_pipeline, databricks_job, logging, monitoring
```

#### **Needed Resources:**

1. **databricks_pipeline** resource for DLT (Delta Live Tables)
2. **databricks_job** for orchestration
3. **databricks_cluster** for compute
4. **databricks_sql_endpoint** for querying
5. **databricks_notebook** for transformed scripts
6. **databricks_secret** management (already using secrets, but not defined in TF)

---

## Refactoring Plan - Simplified Phased Approach

### **Phase 1: Fix Current Issues & Code Quality (Week 1)**

#### 1.1 Immediate Fixes
- [ ] Remove all Polish comments, translate to English
- [ ] Fix typos and naming inconsistencies
- [ ] Add basic error handling (try-catch blocks with logging)
- [ ] Add cell-level documentation
- [ ] Fix schema mismatches (align with warehouse.dbml)

#### 1.2 Code Organization
- [ ] Separate helper functions into dedicated cell at top
- [ ] One dimension per cell (clearly labeled)
- [ ] Consistent column ordering in selects
- [ ] Standardize join patterns (reusable helper functions)
- [ ] Add simple logging (start/end timestamps, row counts)

---

### **Phase 2: Implement In-Memory Medallion Architecture (Week 2)**

#### 2.1 Bronze Layer (In-Memory Raw DataFrames)
```python
def load_bronze_table(table_name: str, logger) -> DataFrame:
    """
    Load raw table from MySQL to in-memory Bronze DataFrame.
    - No transformations
    - Add ingestion metadata for tracking
    - Preserve all source columns
    """
    logger.info(f"Loading bronze: {table_name}")

    df = c.option("dbtable", table_name).load()

    # Add metadata columns (in-memory only, not persisted)
    df_bronze = df.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                  .withColumn("_source_system", F.lit("mysql_wheelie"))

    logger.info(f"Bronze loaded: {table_name} ({df_bronze.count()} rows)")
    return df_bronze

# Usage in notebook:
inventory_bronze = load_bronze_table("inventory", logger)
car_bronze = load_bronze_table("car", logger)
customer_bronze = load_bronze_table("customer", logger)
# ... etc
```

#### 2.2 Silver Layer (In-Memory Cleaned DataFrames)
```python
def clean_to_silver(df_bronze: DataFrame, table_name: str,
                    cleaning_rules: dict, logger) -> DataFrame:
    """
    Apply data quality rules and cleaning transformations.
    - Fix data quality issues (e.g., fuel_type typos)
    - Log validation warnings
    - Return cleaned DataFrame
    """
    logger.info(f"Cleaning silver: {table_name}")

    df_silver = df_bronze

    # Apply cleaning rules
    for col_name, clean_fn in cleaning_rules.items():
        df_silver = clean_fn(df_silver)

    # Basic validation (log warnings only)
    null_counts = {col: df_silver.filter(F.col(col).isNull()).count()
                   for col in df_silver.columns if not col.startswith('_')}
    for col, count in null_counts.items():
        if count > 0:
            logger.warning(f"Silver {table_name}.{col} has {count} null values")

    logger.info(f"Silver cleaned: {table_name} ({df_silver.count()} rows)")
    return df_silver

# Example cleaning rules for inventory
inventory_rules = {
    "fuel_type": lambda df: df.withColumn(
        "fuel_type",
        F.when(F.lower(F.col("fuel_type")) == "diesle", "Diesel")
         .when(F.lower(F.col("fuel_type")) == "petol", "Petrol")
         .otherwise(F.col("fuel_type"))
    )
}

inventory_silver = clean_to_silver(inventory_bronze, "inventory", inventory_rules, logger)
```

#### 2.3 Gold Layer (Business Logic → Delta Tables)
```python
def build_gold_dimension(df_silver: DataFrame, dimension_name: str,
                         transform_fn, logger) -> DataFrame:
    """
    Apply business logic and persist to Gold Delta table.
    - Join multiple silver DataFrames
    - Apply business transformations
    - Add surrogate keys
    - Write to Delta table
    """
    logger.info(f"Building gold: {dimension_name}")

    # Apply transformation function
    df_gold = transform_fn(df_silver)

    # Persist to Delta
    write_dim(df_gold, dimension_name, logger)

    return df_gold
```

**Key Simplification:** No separate bronze/silver Delta tables. All transformations happen in memory, only gold is persisted.

---

### **Phase 3: Refactor to Modular Python Files (Week 3) - OPTIONAL**

**Note:** For a university project, this phase is optional. You can keep everything in notebooks but organized into separate notebooks per layer.

#### Option A: Keep Notebooks (Simpler for Uni Project)
```
notebooks/
├── 00_config_and_helpers.py     # Shared functions
├── 01_load_all_dimensions.py    # All dimensions (refactored)
├── 02_load_all_facts.py         # All facts (refactored)
└── utils/
    ├── connection_utils.py      # Connection helpers
    ├── logging_utils.py         # Simple logging
    └── transformation_utils.py  # Reusable transformations
```

#### Option B: Full Modular Structure (If Time Permits)
```
src/
├── common/
│   ├── __init__.py
│   ├── connection.py       # Database connections
│   ├── logging_utils.py    # Logging utilities
│   └── spark_utils.py      # Spark helpers
├── etl/
│   ├── __init__.py
│   ├── bronze_loader.py    # Bronze loading functions
│   ├── silver_cleaner.py   # Silver cleaning functions
│   └── gold_builder.py     # Gold dimension builders
└── notebooks/
    ├── load_dimensions.py  # Orchestrates dimension loading
    └── load_facts.py       # Orchestrates fact loading
```

**Recommendation:** Start with Option A (organized notebooks). Only move to Option B if you have extra time.

#### 3.2 Common Helper Functions (Simplified)

**A. Connection Helpers (connection_utils.py or in notebook)**
```python
from pyspark.sql import SparkSession
from typing import Optional

class DatabaseConnection:
    """Manages database connections with connection pooling and retry logic."""

    def __init__(self, spark: SparkSession, secret_scope: str = "wheelie"):
        self.spark = spark
        self.secret_scope = secret_scope

    def get_jdbc_reader(self, table: str, predicates: Optional[list] = None):
        """
        Get JDBC reader with connection parameters from secrets.

        Args:
            table: Table name to read
            predicates: List of predicates for parallel reading

        Returns:
            DataFrameReader configured for JDBC
        """
        reader = self.spark.read.format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("user", self._get_secret("MYSQL_USERNAME")) \
            .option("password", self._get_secret("MYSQL_PASSWORD")) \
            .option("dbtable", table)

        if predicates:
            reader = reader.option("predicates", predicates)

        return reader

    def _get_secret(self, key: str) -> str:
        """Retrieve secret with fallback and validation."""
        try:
            return dbutils.secrets.get(self.secret_scope, key)
        except Exception as e:
            raise ValueError(f"Failed to retrieve secret {key}: {str(e)}")

    def _get_jdbc_url(self) -> str:
        """Construct JDBC URL from secrets."""
        host = self._get_secret("MYSQL_HOST")
        db = self._get_secret("MYSQL_DB")
        return f"jdbc:mysql://{host}/{db}?useSSL=false&allowPublicKeyRetrieval=true"
```

**B. spark_utils.py**
```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import xxhash64, col, current_timestamp, lit
from typing import List, Optional

class SparkUtils:
    """Common Spark DataFrame utilities."""

    @staticmethod
    def add_surrogate_key(df: DataFrame, key_name: str, source_columns: List[str]) -> DataFrame:
        """
        Add surrogate key column using hash of source columns.

        Args:
            df: Input DataFrame
            key_name: Name of surrogate key column
            source_columns: Columns to hash for key generation

        Returns:
            DataFrame with surrogate key added
        """
        # Concatenate columns and hash
        key_expr = xxhash64(
            *[col(c) for c in source_columns]
        )
        return df.withColumn(key_name, key_expr)

    @staticmethod
    def add_audit_columns(df: DataFrame, batch_id: Optional[str] = None) -> DataFrame:
        """Add standard audit columns to DataFrame."""
        df = df.withColumn("_load_timestamp", current_timestamp())
        if batch_id:
            df = df.withColumn("_batch_id", lit(batch_id))
        return df

    @staticmethod
    def validate_required_columns(df: DataFrame, required_cols: List[str]) -> DataFrame:
        """Validate that required columns exist and are not null."""
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Check for nulls in required columns
        for col_name in required_cols:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                raise ValueError(f"Column {col_name} has {null_count} null values")

        return df
```

**C. Simplified Logging (logging_utils.py or in notebook)**
```python
import logging
from datetime import datetime
from typing import Optional

class SimpleLogger:
    """Simplified logging for university project."""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.logger = logging.getLogger(job_name)
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Configure console logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s'
        )

    def info(self, message: str):
        """Log info message."""
        self.logger.info(f"[{self.run_id}] {message}")

    def warning(self, message: str):
        """Log warning message."""
        self.logger.warning(f"[{self.run_id}] {message}")

    def error(self, message: str, exception: Optional[Exception] = None):
        """Log error message."""
        error_msg = f"[{self.run_id}] {message}"
        if exception:
            error_msg += f" - {str(exception)}"
        self.logger.error(error_msg)

    def log_table_load(self, stage: str, table: str, row_count: int, duration_seconds: float):
        """Log table load completion with metrics."""
        self.info(f"{stage} - {table}: {row_count:,} rows in {duration_seconds:.2f}s")

# Simple usage:
logger = SimpleLogger("load_dimensions")
logger.info("Starting ETL job")
logger.log_table_load("GOLD", "dim_customer", 1500, 2.5)
```

#### 3.3 Dimension Classes (Example: dim_customer.py)

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when
from typing import Optional
from gold.dimension_base import DimensionBase

class CustomerDimension(DimensionBase):
    """
    Customer dimension with SCD Type 2 implementation.

    Business Key: customer_id
    SCD Type: 2 (tracks address changes)
    Source: silver.customer_clean + silver.address_clean + silver.city_clean + silver.country_clean
    """

    def __init__(self, spark, logger):
        super().__init__(
            spark=spark,
            logger=logger,
            dimension_name="dim_customer",
            business_key="customer_id",
            scd_type=2
        )

    def extract(self) -> DataFrame:
        """Load source data from silver layer."""
        customer_df = self.spark.table("wheelie.silver.customer_clean")
        address_df = self.spark.table("wheelie.silver.address_clean")
        city_df = self.spark.table("wheelie.silver.city_clean")
        country_df = self.spark.table("wheelie.silver.country_clean")

        # Denormalize location hierarchy
        address_with_location = address_df \
            .join(city_df, "city_id", "left") \
            .join(country_df, "country_id", "left")

        # Join customer with location
        df = customer_df.join(
            address_with_location,
            "address_id",
            "left"
        )

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply business logic transformations."""
        return df.select(
            col("customer_id"),
            col("first_name").alias("customer_first_name"),
            col("last_name").alias("customer_last_name"),
            col("email").alias("customer_email"),
            col("birth_date"),
            col("city").alias("customer_city"),
            col("country").alias("customer_country"),
            col("create_date"),
            col("last_update")
        )

    def get_scd2_tracking_columns(self) -> list:
        """Define which columns trigger SCD Type 2 versioning."""
        return ["customer_city", "customer_country"]  # Track address changes
```

#### 3.4 Dimension Base Class

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, row_number, xxhash64, concat_ws
from pyspark.sql.window import Window
from typing import Optional, List
import time

class DimensionBase(ABC):
    """
    Abstract base class for dimension table loading.
    Implements common patterns for SCD Type 1 and Type 2.
    """

    def __init__(self, spark, logger, dimension_name: str,
                 business_key: str, scd_type: int = 1):
        self.spark = spark
        self.logger = logger
        self.dimension_name = dimension_name
        self.business_key = business_key
        self.scd_type = scd_type
        self.table_name = f"wheelie.gold.{dimension_name}"

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract source data from silver layer."""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply business logic transformations."""
        pass

    def load(self):
        """Main entry point for dimension loading."""
        start_time = time.time()

        try:
            self.logger.log_start("GOLD", self.dimension_name)

            # Extract
            df_source = self.extract()

            # Transform
            df_transformed = self.transform(df_source)

            # Add surrogate key
            df_with_key = self._add_surrogate_key(df_transformed)

            # Load based on SCD type
            if self.scd_type == 1:
                self._load_scd1(df_with_key)
            elif self.scd_type == 2:
                self._load_scd2(df_with_key)
            else:
                raise ValueError(f"Unsupported SCD type: {self.scd_type}")

            # Log completion
            row_count = df_with_key.count()
            duration = time.time() - start_time
            self.logger.log_completion("GOLD", self.dimension_name, row_count, duration)

        except Exception as e:
            self.logger.log_error("GOLD", self.dimension_name, e)
            raise

    def _add_surrogate_key(self, df: DataFrame) -> DataFrame:
        """Add surrogate key column."""
        key_name = f"{self.dimension_name.replace('dim_', '')}_key"
        return df.withColumn(key_name, xxhash64(col(self.business_key)))

    def _load_scd1(self, df: DataFrame):
        """Load dimension with SCD Type 1 (overwrite)."""
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(self.table_name)

    def _load_scd2(self, df: DataFrame):
        """
        Load dimension with SCD Type 2 (merge with versioning).

        Logic:
        1. Compare incoming records with existing dimension
        2. If tracked columns changed, close old record and insert new
        3. If no change, update last_update timestamp
        4. If new record, insert with is_current=TRUE
        """
        from delta.tables import DeltaTable

        # Add SCD2 columns to incoming data
        df_new = df.withColumn("effective_date", current_timestamp()) \
                   .withColumn("end_date", lit(None).cast("timestamp")) \
                   .withColumn("is_current", lit(True))

        # Check if dimension table exists
        if self.spark.catalog.tableExists(self.table_name):
            # Get existing dimension
            delta_table = DeltaTable.forName(self.spark, self.table_name)

            # Merge logic for SCD Type 2
            # This is simplified - full implementation needs complex MERGE statement
            merge_condition = f"target.{self.business_key} = source.{self.business_key} AND target.is_current = TRUE"

            delta_table.alias("target").merge(
                df_new.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                condition=self._get_change_condition("source", "target"),
                set={
                    "is_current": lit(False),
                    "end_date": current_timestamp()
                }
            ).whenNotMatchedInsertAll() \
            .execute()

            # Insert new versions for changed records (requires second pass)
            # Full implementation would handle this in single pass

        else:
            # First load - just write
            df_new.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(self.table_name)

    @abstractmethod
    def get_scd2_tracking_columns(self) -> List[str]:
        """Define which columns trigger SCD Type 2 versioning."""
        pass

    def _get_change_condition(self, source_alias: str, target_alias: str) -> str:
        """Build condition to detect changes in SCD2 tracking columns."""
        tracking_cols = self.get_scd2_tracking_columns()
        conditions = [
            f"{source_alias}.{col} != {target_alias}.{col}"
            for col in tracking_cols
        ]
        return " OR ".join(conditions)
```

---

### **Phase 4: Implement Incremental Loading (Week 4)**

#### 4.1 Simplified Watermark Strategy

**Concept:**
- Track last successful load timestamp per table
- Only load records modified after watermark
- Update watermark on successful completion

**Simplified Implementation (for small data volumes):**

```python
# New file: common/watermark.py
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional

class WatermarkManager:
    """Manages watermarks for incremental loading."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.watermark_table = "wheelie.monitoring.watermarks"
        self._ensure_watermark_table()

    def _ensure_watermark_table(self):
        """Create watermark tracking table if not exists."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.watermark_table} (
                table_name STRING,
                watermark_value TIMESTAMP,
                updated_at TIMESTAMP
            )
            USING DELTA
        """)

    def get_watermark(self, table_name: str) -> Optional[datetime]:
        """Get last successful watermark for table."""
        result = self.spark.sql(f"""
            SELECT watermark_value
            FROM {self.watermark_table}
            WHERE table_name = '{table_name}'
        """).collect()

        return result[0][0] if result else None

    def update_watermark(self, table_name: str, watermark_value: datetime):
        """Update watermark after successful load."""
        self.spark.sql(f"""
            MERGE INTO {self.watermark_table} target
            USING (
                SELECT '{table_name}' as table_name,
                       '{watermark_value}' as watermark_value,
                       current_timestamp() as updated_at
            ) source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
```

**Usage in Bronze Layer:**

```python
# Modified bronze/ingest_tables.py
def ingest_incremental(table_name: str, watermark_column: str = "last_update"):
    """Ingest only new/modified records."""

    wm = WatermarkManager(spark)
    last_watermark = wm.get_watermark(table_name)

    # Build incremental query
    if last_watermark:
        query = f"(SELECT * FROM {table_name} WHERE {watermark_column} > '{last_watermark}') as subset"
    else:
        query = table_name  # First load - get all

    # Load incremental data
    df = conn.get_jdbc_reader(query).load()

    # Write to bronze (append mode)
    df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"wheelie.bronze.{table_name}")

    # Update watermark
    max_watermark = df.agg({watermark_column: "max"}).collect()[0][0]
    if max_watermark:
        wm.update_watermark(table_name, max_watermark)
```

#### 4.2 Change Data Capture (CDC) for SCD Type 2

**Strategy:**
1. Compare current dimension with incoming data
2. Detect changes in tracked columns
3. Close expired records (set `end_date`, `is_current=FALSE`)
4. Insert new versions for changed records

**Implementation in dimension_base.py:**

```python
def _load_scd2_incremental(self, df_new: DataFrame):
    """
    Incremental SCD Type 2 merge using Delta Lake MERGE.

    Handles:
    - New records: Insert with is_current=TRUE
    - Changed records: Close old version, insert new version
    - Unchanged records: No action (or update last_update)
    """
    from delta.tables import DeltaTable

    # Add SCD2 metadata
    df_new = df_new.withColumn("effective_date", current_timestamp()) \
                   .withColumn("end_date", lit(None).cast("timestamp")) \
                   .withColumn("is_current", lit(True))

    if not self.spark.catalog.tableExists(self.table_name):
        # First load
        df_new.write.format("delta").mode("overwrite").saveAsTable(self.table_name)
        return

    delta_table = DeltaTable.forName(self.spark, self.table_name)

    # Step 1: Close expired records (where tracked columns changed)
    tracking_cols = self.get_scd2_tracking_columns()
    change_conditions = [
        f"target.{col} != source.{col}" for col in tracking_cols
    ]
    change_condition_str = " OR ".join(change_conditions)

    delta_table.alias("target").merge(
        df_new.alias("source"),
        f"target.{self.business_key} = source.{self.business_key} AND target.is_current = TRUE"
    ).whenMatchedUpdate(
        condition=change_condition_str,
        set={
            "is_current": lit(False),
            "end_date": current_timestamp()
        }
    ).execute()

    # Step 2: Insert new versions (for both new records and changed records)
    # This requires a second merge or separate insert
    delta_table.alias("target").merge(
        df_new.alias("source"),
        f"target.{self.business_key} = source.{self.business_key} AND target.is_current = TRUE"
    ).whenNotMatchedInsertAll().execute()
```

---

### **Phase 5: Databricks Job Automation with Terraform (Week 5) - OPTIONAL**

**Note:** For a university project, this phase is optional. You can manually run notebooks or use simple Databricks Jobs.

#### 5.1 Simplified Job Architecture

**Why Databricks Jobs (not DLT)?**
- Simpler setup for university project
- Sufficient for small data volumes
- Easier to debug and modify
- No need for streaming complexity

#### 5.2 Organized Notebook Structure

**Keep it simple with organized notebooks:**

**notebooks/00_config.py:**
```python
# Configuration and helper functions
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, xxhash64, col
import logging

# Initialize logger
logger = logging.getLogger("wheelie_etl")
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def create_connection():
    """Create JDBC connection to MySQL."""
    return spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{dbutils.secrets.get('wheelie', 'MYSQL_HOST')}/{dbutils.secrets.get('wheelie', 'MYSQL_DB')}") \
        .option("user", dbutils.secrets.get('wheelie', 'MYSQL_USERNAME')) \
        .option("password", dbutils.secrets.get('wheelie', 'MYSQL_PASSWORD'))

def load_bronze_table(table_name: str) -> DataFrame:
    """Load table from MySQL (bronze layer - in memory)."""
    logger.info(f"Loading bronze: {table_name}")
    try:
        df = c.option("dbtable", table_name).load()
        logger.info(f"Bronze loaded: {table_name} ({df.count()} rows)")
        return df
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {str(e)}")
        raise

def write_dim(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Write DataFrame to Gold Delta table."""
    logger.info(f"Writing gold: {table_name} ({df.count()} rows)")
    try:
        df.write.format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"wheelie.data_warehouse.{table_name}")
        logger.info(f"Gold written: {table_name}")
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {str(e)}")
        raise

# Initialize connection
c = create_connection()
```

**notebooks/01_load_dimensions.py:**
```python
# %run ./00_config

import time
from pyspark.sql.functions import *

# Track job execution
start_time = time.time()
logger.info("=" * 50)
logger.info("Starting Dimension Load Job")
logger.info("=" * 50)

try:
    # Load all bronze tables (in-memory)
    logger.info("PHASE 1: Loading Bronze DataFrames")
    inventory_bronze = load_bronze_table("inventory")
    car_bronze = load_bronze_table("car")
    customer_bronze = load_bronze_table("customer")
    # ... other tables

    # Clean to silver (in-memory transformations)
    logger.info("PHASE 2: Cleaning to Silver DataFrames")

    # Fix fuel type (silver transformation)
    inventory_silver = inventory_bronze.withColumn(
        "fuel_type",
        when(lower(col("fuel_type")) == "diesle", "Diesel")
         .when(lower(col("fuel_type")) == "petol", "Petrol")
         .otherwise(col("fuel_type"))
    )

    # Build gold dimensions
    logger.info("PHASE 3: Building Gold Dimensions")

    # DIM_DATE
    logger.info("Building dim_date...")
    # ... (existing dim_date logic)
    write_dim(dim_date, "dim_date")

    # DIM_CUSTOMER
    logger.info("Building dim_customer...")
    # ... (refactored customer logic)
    write_dim(dim_customer, "dim_customer")

    # ... other dimensions

    duration = time.time() - start_time
    logger.info("=" * 50)
    logger.info(f"Dimension Load Complete in {duration:.2f}s")
    logger.info("=" * 50)

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    raise
```

#### 5.3 Optional: Simple Terraform Job Resource

**If you want to automate notebook execution, add this to infra/jobs.tf:**

```terraform
resource "databricks_job" "wheelie_dimensions_load" {
  name = "wheelie-load-dimensions"

  task {
    task_key = "load_dimensions"

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/load_all_dim"
      source        = "GIT"
    }

    new_cluster {
      num_workers   = 1  # Small cluster for university project
      spark_version = data.databricks_spark_version.latest.id
      node_type_id  = "i3.xlarge"
    }
  }

  max_concurrent_runs = 1
  timeout_seconds     = 1800  # 30 minutes (plenty for small data)
}

resource "databricks_job" "wheelie_facts_load" {
  name = "wheelie-load-facts"

  task {
    task_key = "load_facts"

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/load_all_facts"
      source        = "GIT"
    }

    new_cluster {
      num_workers   = 1
      spark_version = data.databricks_spark_version.latest.id
      node_type_id  = "i3.xlarge"
    }
  }

  depends_on = [databricks_job.wheelie_dimensions_load]
}

data "databricks_spark_version" "latest" {
  long_term_support = true
}
```

**For a university project, you can also just run notebooks manually - no automation needed!

---

### **Phase 6: Apply to load_all_facts.ipynb (Week 6)**

Once dimension loading is refactored, apply same patterns to facts:

1. **Fact Tables to Refactor:**
   - `fact_rental`
   - `fact_service`

2. **Follow Same Structure:**
   ```
   src/gold/fact_rental.py
   src/gold/fact_service.py
   ```

3. **Key Differences:**
   - Facts use `append` mode (not overwrite)
   - No SCD logic (facts are immutable)
   - Watermark based on transaction date
   - Need to handle late-arriving facts

4. **Example fact_rental.py:**
```python
from gold.fact_base import FactBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, xxhash64, datediff

class RentalFact(FactBase):
    """
    Rental fact table implementation.

    Grain: One row per rental transaction
    Source: silver.rental + silver.payment + dimension lookups
    """

    def __init__(self, spark, logger):
        super().__init__(
            spark=spark,
            logger=logger,
            fact_name="fact_rental",
            business_key="rental_id",
            transaction_date_column="rental_date"
        )

    def extract(self) -> DataFrame:
        """Load source data from silver layer."""
        rental_df = self.spark.table("wheelie.silver.rental_clean")
        payment_df = self.spark.table("wheelie.silver.payment_clean")
        staff_df = self.spark.table("wheelie.silver.staff_clean")
        inventory_df = self.spark.table("wheelie.silver.inventory_clean")

        # Join rental with payment (left join - payment may not exist yet)
        rental_with_payment = rental_df.join(
            payment_df,
            rental_df.rental_id == payment_df.rental_id,
            "left"
        )

        # Join with staff for store_id
        rental_with_staff = rental_with_payment.join(
            staff_df,
            rental_with_payment.staff_id == staff_df.staff_id,
            "left"
        )

        # Join with inventory for car_id
        rental_complete = rental_with_staff.join(
            inventory_df.select("inventory_id", "car_id"),
            rental_with_staff.inventory_id == inventory_df.inventory_id,
            "left"
        )

        return rental_complete

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply business logic and add surrogate keys."""
        return df.select(
            col("rental_id"),
            col("rental_rate"),
            col("payment_amount"),
            col("customer_id"),
            col("car_id"),
            col("staff_id"),
            col("store_id"),
            col("rental_date"),
            col("return_date"),
            col("payment_date"),
            col("payment_deadline")
        ).withColumn("rental_key", xxhash64(col("rental_id"))) \
         .withColumn("customer_key", xxhash64(col("customer_id"))) \
         .withColumn("car_key", xxhash64(col("car_id"))) \
         .withColumn("staff_key", xxhash64(col("staff_id"))) \
         .withColumn("store_key", xxhash64(col("store_id"))) \
         .withColumn("rental_date_key", xxhash64(col("rental_date"))) \
         .withColumn("return_date_key", xxhash64(col("return_date"))) \
         .withColumn("payment_date_key", xxhash64(col("payment_date"))) \
         .withColumn("payment_deadline_date_key", xxhash64(col("payment_deadline"))) \
         .withColumn(
             "rental_amount",
             col("rental_rate") * datediff(col("return_date"), col("rental_date"))
         ) \
         .withColumn("rental_duration", datediff(col("return_date"), col("rental_date"))) \
         .withColumn(
             "payment_delay_duration",
             datediff(col("payment_date"), col("payment_deadline"))
         )
```

---

## Implementation Checklist (Simplified for University Project)

### **Phase 1: Code Quality & Cleanup (Priority: HIGH)**
- [ ] Translate all Polish comments to English
- [ ] Fix typos and inconsistent naming
- [ ] Add error handling with try-catch blocks
- [ ] Add cell documentation headers
- [ ] Fix schema mismatches vs warehouse.dbml
- [ ] Add simple logging (console output with timestamps)

### **Phase 2: In-Memory Medallion Architecture (Priority: HIGH)**
- [ ] Create helper function `load_bronze_table()` for raw loading
- [ ] Create helper function `clean_to_silver()` for data cleaning
- [ ] Move fuel_type corrections to silver transformation
- [ ] Refactor each dimension to use bronze→silver→gold pattern
- [ ] Add row count logging for each stage

### **Phase 3: Code Organization (Priority: MEDIUM)**
- [ ] Extract common functions to Cell 1 (config & helpers)
- [ ] One dimension per cell with clear labels
- [ ] Standardize join patterns (reusable helper if needed)
- [ ] Add function docstrings
- [ ] Clean up variable naming

### **Phase 4: Incremental Loading (Priority: MEDIUM - Optional)**
- [ ] Add simple watermark tracking (last_update column)
- [ ] Modify load_bronze_table() to support incremental
- [ ] Test with small incremental updates
- [ ] Handle first load vs incremental load

### **Phase 5: Facts Refactoring (Priority: HIGH)**
- [ ] Apply same bronze→silver→gold pattern to fact_rental
- [ ] Apply same pattern to fact_service
- [ ] Add proper error handling
- [ ] Add logging

### **Phase 6: Terraform Automation (Priority: LOW - Optional)**
- [ ] Add simple Databricks job resource (optional)
- [ ] Test automated notebook execution (optional)

**Total Estimated Time: 2-3 weeks** (vs 6 weeks for production-grade)

---

## Testing Strategy

### **Unit Tests**
```python
# tests/test_customer_dimension.py
import pytest
from gold.dim_customer import CustomerDimension

def test_customer_transform(spark_session):
    # Arrange
    input_data = [
        (1, "John", "Doe", "Warsaw", "Poland"),
        (2, "Jane", "Smith", "Krakow", "Poland")
    ]
    df = spark_session.createDataFrame(input_data, ["customer_id", "first_name", "last_name", "city", "country"])

    dim = CustomerDimension(spark_session, mock_logger)

    # Act
    result = dim.transform(df)

    # Assert
    assert result.count() == 2
    assert "customer_key" in result.columns
    assert "customer_first_name" in result.columns
```

### **Integration Tests**
- Test bronze → silver → gold flow
- Test incremental loading
- Test SCD Type 2 versioning
- Test data quality failures

### **Data Quality Tests**
- Validate all required columns present
- Check for null values in non-nullable columns
- Verify referential integrity
- Test business rule validations

---

## Remaining Questions (for University Project)

1. **SCD Type 2 Implementation:**
   - For dim_customer SCD2, what triggers a new version?
   - Option A: Only address changes (city, country)
   - Option B: Any column change
   - **Recommendation:** Option A (simpler)

2. **Incremental Loading Priority:**
   - Is incremental loading required for the project grade?
   - Or is full refresh acceptable given small data volumes?
   - **Recommendation:** Implement simple incremental if time permits

3. **Date Dimension Range:**
   - What date range should dim_date cover?
   - Current code: 2000-01-01 to 2027-12-31
   - **Recommendation:** Keep current range (sufficient for project)

4. **Delivery Format:**
   - Should final deliverable be notebooks or Python scripts?
   - **Recommendation:** Refactored notebooks (easier to demo/grade)

5. **Documentation Requirements:**
   - What level of documentation is required?
   - (README, code comments, architecture diagram?)
   - **Recommendation:** Add README with architecture overview

---

## Success Criteria (University Project)

### **Code Quality (Required)**
- ✅ All code in English (no Polish comments)
- ✅ Consistent naming conventions
- ✅ Basic error handling with try-catch
- ✅ Logging for major operations (load start/end, row counts)
- ✅ Code organized and commented
- ✅ Functions have docstrings

### **Architecture (Required)**
- ✅ Bronze→Silver→Gold pattern implemented (in-memory)
- ✅ Data cleaning separated from business logic
- ✅ Schema matches warehouse.dbml specification
- ✅ All dimensions load successfully
- ✅ All facts load successfully

### **Functionality (Required)**
- ✅ Full refresh works end-to-end
- ✅ dim_customer with SCD Type 2 (basic implementation)
- ✅ Staff hierarchy bridge table
- ✅ Equipment bridge tables
- ✅ dim_date with COVID period flags

### **Optional Enhancements (Bonus Points)**
- ⭐ Incremental loading with watermarks
- ⭐ Modular helper functions
- ⭐ Terraform job automation
- ⭐ Comprehensive README documentation

### **Performance (Nice to Have)**
- ✅ Full load completes in < 5 minutes (should be easy with <3k rows)
- ✅ No unnecessary full table scans

---

## Next Steps

1. **Review this document** and clarify open questions
2. **Prioritize phases** based on business needs
3. **Set up development environment** (dev Databricks workspace)
4. **Begin Phase 1** with immediate fixes
5. **Iterate through phases** with regular reviews

---

## References

- [Databricks Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Slowly Changing Dimensions Best Practices](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Terraform Databricks Provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/pipeline)
