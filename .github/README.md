# Wheelie Data Warehouse ETL Pipeline

## Overview
This directory contains Databricks notebooks for the Wheelie car rental data warehouse ETL pipeline. The implementation follows a simplified **medallion architecture** (bronze→silver→gold) optimized for university project requirements.

## Architecture

### Medallion Pattern (Simplified)
```
┌──────────────┐
│ MySQL Source │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────┐
│ BRONZE Layer (In-Memory)     │
│ - Raw data from source       │
│ - No transformations         │
│ - Metadata added             │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│ SILVER Layer (In-Memory)     │
│ - Data cleaning              │
│ - Type corrections           │
│ - Business rules             │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│ GOLD Layer (Delta Tables)    │
│ - Star schema dimensions     │
│ - Fact tables                │
│ - Persisted to disk          │
└──────────────────────────────┘
```

**Note:** Bronze and silver layers are **in-memory DataFrames** (not persisted). Only the gold layer is written to Delta tables. This is acceptable for the project's small data volumes (<3,000 rows).

## Project Structure

```
notebooks/
├── helpers/                    # Shared utility functions (DRY principle)
│   ├── __init__.py            # Module initialization
│   ├── database.py            # Connection & data loading utilities
│   └── logging_config.py      # Logging setup
│
├── load_all_dim.ipynb         # Dimension table ETL pipeline
├── load_all_facts.ipynb       # Fact table ETL pipeline
├── test_data_quality.ipynb    # Data quality tests with pytest
├── drop_all_tables.sql        # Utility to reset warehouse
└── README.md                  # This file
```

## Notebooks

### 1. load_all_dim.ipynb
**Purpose:** Load all dimension tables from MySQL to data warehouse

**Cell Structure:**
1. **Configuration & Helpers** - Import shared utilities
2. **Bronze Layer** - Load 10 source tables from MySQL
3. **Silver Layer** - Data cleaning (fuel type corrections, denormalization)
4. **DIM_DATE** - Generate date dimension (2000-2027)
5. **DIM_SERVICE_DATE** - Copy of dim_date for BI separation
6. **DIM_STAFF** - Staff dimension + hierarchy bridge
7. **DIM_STORE** - Store dimension with location denormalization
8. **DIM_CAR** - Car dimension + equipment bridges
9. **DIM_CUSTOMER** - Customer dimension (SCD Type 2 ready)

**Output Tables:**
- `dim_date` (7 columns, ~10,000 rows)
- `dim_service_date` (7 columns, ~10,000 rows) - For BI purposes
- `dim_staff` (10 columns)
- `dim_store` (9 columns)
- `dim_car` (12 columns)
- `dim_customer` (9 columns)
- `dim_equipment` (3 columns)
- `bridge_staff_hierarchy` (3 columns) - Staff reporting paths
- `bridge_car_equipment` (2 columns) - Car to equipment group
- `bridge_equipment_group_equipment` (2 columns) - Equipment group to equipment

### 2. load_all_facts.ipynb
**Purpose:** Load all fact tables from MySQL to data warehouse

**Cell Structure:**
1. **Configuration & Helpers** - Import shared utilities
2. **Bronze Layer** - Load source tables (rental, service, payment, etc.)
3. **Silver Layer** - Data preparation
4. **FACT_SERVICE** - Service transactions with service_date_key → dim_service_date
5. **FACT_RENTAL** - Rental transactions with multiple date keys → dim_date

**Output Tables:**
- `fact_service` (5 columns) - Service transaction grain
- `fact_rental` (13 columns) - Rental transaction grain

**Key Design:** 
- `fact_service.service_date_key` → `dim_service_date.service_date_key`
- `fact_rental.rental_date_key` (and other date keys) → `dim_date.date_key`
- Separate date dimensions for BI clarity

### 3. test_data_quality.ipynb
**Purpose:** Validate data quality with pytest assertions

**Test Coverage:**
- ✅ Uniqueness tests for all surrogate keys
- ✅ Referential integrity between facts and dimensions
- ✅ Referential integrity for bridge tables
- ✅ Equipment bridge type consistency validation

**Total Tests:** 20+ assertions across 7 dimensions, 3 bridges, 2 facts

## Helper Functions (DRY Architecture)

### helpers/database.py

**`create_connection()`**
- Creates JDBC connection to MySQL
- Uses `dbutils.secrets` for secure credentials
- Returns configured DataFrameReader

**`load_bronze_table(conn, table_name)`**
- Loads raw table from MySQL (bronze layer)
- Adds metadata: `_ingestion_ts`, `_source`
- Logs row counts
- Returns in-memory DataFrame

**`write_gold_table(df, table_name, mode='overwrite')`**
- Writes DataFrame to Delta table (gold layer)
- Schema: `wheelie.data_warehouse.{table_name}`
- Supports overwrite/append modes
- Logs completion with row counts

### helpers/logging_config.py

**`setup_logger(name)`**
- Configures consistent logging format
- Returns logger instance for job tracking

## Usage

### Running the ETL Pipeline

1. **Load Dimensions First:**
   ```python
   # Run all cells in load_all_dim.ipynb
   # Expected duration: 2-5 minutes
   ```

2. **Load Facts Second:**
   ```python
   # Run all cells in load_all_facts.ipynb
   # Expected duration: 1-2 minutes
   ```

3. **Validate Data Quality:**
   ```python
   # Run all cells in test_data_quality.ipynb
   # All tests should pass ✅
   ```

### Importing Helpers in New Notebooks

```python
import sys
sys.path.append("/Workspace/Repos/nutter/databricks/notebooks")

from helpers import create_connection, load_bronze_table, write_gold_table, setup_logger

# Initialize
logger = setup_logger("my_job")
c = create_connection()

# Use helpers
df = load_bronze_table(c, "my_table")
write_gold_table(df, "dim_my_table")
```

## Key Features

### ✅ Code Quality
- All helper functions extracted to reusable modules (DRY principle)
- Consistent naming conventions (snake_case)
- English comments and documentation
- Error handling with try-catch blocks
- Comprehensive logging (start/end, row counts, timing)

### ✅ Medallion Architecture
- Bronze: Raw MySQL data (in-memory)
- Silver: Cleaned and prepared (in-memory)
- Gold: Business logic, persisted to Delta tables

### ✅ Data Quality
- Pytest-based validation framework
- Uniqueness constraints validated
- Referential integrity verified
- Type consistency checks (fixed equipment bridge bug)

### ✅ BI-Friendly Design
- Separate date dimensions (dim_date vs dim_service_date)
- Clear key naming conventions
- Bridge tables for many-to-many relationships
- Staff hierarchy for organizational reporting

## Data Model Summary

### Dimension Tables
| Table | Grain | Rows | SCD Type |
|-------|-------|------|----------|
| dim_date | One row per day | ~10,000 | Type 1 |
| dim_service_date | One row per day (copy of dim_date) | ~10,000 | Type 1 |
| dim_staff | One row per staff member | ~10 | Type 1 |
| dim_store | One row per store | ~2 | Type 1 |
| dim_car | One row per inventory item | ~1,000 | Type 1 |
| dim_customer | One row per customer | ~600 | Type 2* |
| dim_equipment | One row per equipment type | ~10 | Type 1 |

*SCD Type 2 structure present, but currently loading as Type 1 (full overwrite)

### Bridge Tables
| Table | Purpose | Relationship |
|-------|---------|--------------|
| bridge_staff_hierarchy | Staff reporting paths | Staff → Manager (transitive) |
| bridge_car_equipment | Car to equipment group | 1:1 |
| bridge_equipment_group_equipment | Equipment group to equipment | M:N |

### Fact Tables
| Table | Grain | Rows | Update Mode |
|-------|-------|------|-------------|
| fact_service | One row per service transaction | ~500 | Overwrite |
| fact_rental | One row per rental transaction | ~1,500 | Overwrite |

## Known Limitations

1. **No Incremental Loading:** Currently full overwrite on each run (acceptable for small data)
2. **SCD Type 2 Not Implemented:** dim_customer has structure but uses overwrite mode
3. **No Automated Scheduling:** Manual notebook execution required
4. **No Data Validation Rules:** Basic logging only, no data rejection

## Future Enhancements (Optional)

- [ ] Implement watermark-based incremental loading
- [ ] Complete SCD Type 2 for dim_customer with MERGE
- [ ] Add Terraform Databricks job automation
- [ ] Implement data quality rules with rejection thresholds
- [ ] Add performance optimizations (partitioning, Z-ordering)

## References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Data Warehouse Schema](../data-structure/warehouse.dbml)
- [Refactoring Plan](../.github/copilot-instructions.md)

---

**Last Updated:** January 28, 2026  
**Project:** Wheelie Car Rental Data Warehouse (University Project)
