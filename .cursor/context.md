# Wheelie Data Warehouse - Context Documentation

## Project Overview

This project contains a data warehouse schema for Wheelie car rental business, designed as a star schema with bridge tables for analytical queries.

## Database Structure

### Source Database (Transactional)
- **Location**: `data-structure/wheelie.dbml`
- **Type**: Normalized transactional database (3NF)
- **Purpose**: Operational system, source for ETL
- **Cannot be modified**: No ALTER TABLE allowed

### Data Warehouse
- **Location**: `data-structure/warehouse.dbml`
- **Type**: Denormalized star schema with bridge tables
- **Purpose**: Analytical queries and reporting

## Key Design Decisions

### 1. Naming Conventions
- **Primary Keys**: All PKs are named `ID` (int(10) auto-increment)
- **Foreign Keys**: All FKs are named `<dim>_key` (e.g., `customer_key`, `car_key`, `staff_key`, `store_key`)
- **Business Keys**: Preserved from source system with original format (e.g., `customer_id`, `rental_id`, `inventory_id`)

### 2. Schema Structure
- **1 Fact Table**: `fact_rental` (grain: one row per rental)
- **8 Dimension Tables**:
  - `dim_customer` (SCD Type 2)
  - `dim_car`
  - `dim_staff`
  - `dim_store`
  - `dim_date`
  - `dim_eq` (equipment)
  - `dim_eq_group` (equipment groups)
- **2 Bridge Tables**:
  - `bridge_staff_hierarchy` (closure table for staff hierarchy)
  - `bridge_eq_group` (car to equipment group)

### 3. Removed Components
- ❌ `fact_service` - removed (not needed)
- ❌ `dim_payment` - removed (data moved to `fact_rental`)
- ❌ FK relationships from `dim_store` to `dim_car` and `dim_staff` - removed (store_id is denormalized, no FK)

### 4. Equipment Hierarchy
- **Structure**: `dim_car` → `bridge_eq_group` → `dim_eq_group` → `dim_eq`
- **Design**: Flat structure - all equipment types for a car instance are in one group
- **No separation by equipment.type** - all equipment in one group per car

### 5. Staff Hierarchy
- **Direct manager**: `dim_staff.manager_staff_key` (self-referencing FK)
- **Full hierarchy**: `bridge_staff_hierarchy` (closure table with all levels)
- **Pattern**: Kimball bridge pattern for hierarchical queries

## Fact Table Structure

### fact_rental
**Measures**:
- `rental_amount` - Total rental amount
- `payment_amount` - Payment amount (from payment table)
- `rental_rate` - Rental rate charged
- `rental_duration` - Days between rental_date and return_date
- `rental_delay_days` - Delay in return (calculated)

**Date Fields**:
- Both `*_date_key` (FK to dim_date) and `*_date` (direct date) for each date
- `rental_date`, `return_date`, `payment_date`, `payment_deadline_date`
- Keys used for temporal analysis, dates for direct operations

**Payment Information**:
- All payment data embedded in `fact_rental` (no separate `dim_payment`)
- `payment_amount`, `payment_date`, `payment_deadline_date` are nullable

## Incremental Loading Strategy

### Constraints
- **Source database cannot be modified** - must work with existing columns
- **Missing timestamps**: Some tables lack `create_date` (staff, store, payment, address)
- **Static tables**: city, country have no timestamps (treat as static)

### Loading Strategies by Table Type

#### A. Static Dimensions (dim_date, dim_eq)
- Full load on first run
- Incremental: `WHERE create_date > @watermark OR last_update > @watermark`
- Rarely changes, can be loaded weekly

#### B. SCD Type 2 Dimensions (dim_customer)
- Incremental: `WHERE last_update > @watermark`
- Compare current values with warehouse
- If changed: close old version (set `is_current = FALSE`, `end_date = CURRENT_TIMESTAMP`)
- Insert new version with `is_current = TRUE`, `effective_date = CURRENT_TIMESTAMP`
- Temporal join when loading facts: `rental_date BETWEEN effective_date AND COALESCE(end_date, '9999-12-31')`

#### C. Type 1 Dimensions (dim_car, dim_staff, dim_store)
- **dim_car**: `WHERE create_date > @watermark OR last_update > @watermark` (from inventory table)
- **dim_staff**: `WHERE last_update > @watermark` (no create_date available)
- **dim_store**: `WHERE last_update > @watermark` (no create_date available)
- **MERGE strategy**: UPDATE if exists (by business key), INSERT if new
- For tables without create_date: use `last_update` to detect new records

#### D. Bridge Tables
- **Rebuild strategy**: TRUNCATE + INSERT when dependencies change
- **bridge_staff_hierarchy**: Rebuild when `dim_staff.manager_id` changes
- **bridge_eq_group**: Rebuild when `inventory_equipment` changes

#### E. Fact Tables (fact_rental)
- Incremental: `WHERE create_date > @watermark OR technical_timestamp > @watermark`
- Also check payment table: `WHERE payment.last_update > @watermark`
- Handle late-arriving data:
  - Update existing rentals when `return_date` is added later
  - Update existing rentals when `payment` is made after rental creation
- Use `rental_id` as business key to detect duplicates

### ETL Infrastructure Tables

#### etl_watermarks
Tracks last processed timestamps for each source table:
- `table_name` (PK)
- `source_table`
- `last_processed_timestamp`
- `last_processed_id`
- `last_run_timestamp`
- `records_processed`
- `status` ('SUCCESS', 'FAILED', 'IN_PROGRESS')
- `error_message`

#### etl_audit_log
Logs all ETL operations for monitoring:
- `audit_log_id` (PK)
- `table_name`
- `source_table`
- `run_timestamp`
- `operation_type` ('FULL_LOAD', 'INCREMENTAL_LOAD', 'SCD_TYPE2_UPDATE')
- `records_inserted`, `records_updated`, `records_deleted`, `records_failed`
- `start_timestamp`, `end_timestamp`, `duration_seconds`
- `status` ('SUCCESS', 'FAILED', 'PARTIAL')
- `error_message`
- `source_watermark_timestamp`, `target_watermark_timestamp`

## SQL Files

### load-db.sql
Contains complete incremental loading queries for all tables:
- Dimensions (static, Type 1, SCD Type 2)
- Bridge tables (rebuild logic)
- Fact tables (with late-arriving data handling)
- Watermark updates

**Usage**: Execute queries in dependency order within a transaction.

## Important Notes

1. **Source Database Schema**: Cannot be modified - all incremental loading must work with existing columns
2. **Timestamp Handling**:
   - Tables without `create_date` use `last_update` for new record detection
   - Static tables (city, country) require full reload if needed
3. **SCD Type 2**: Only `dim_customer` uses SCD Type 2 (tracks address changes)
4. **Denormalization**: Store information is denormalized in `dim_car` and `dim_staff` (no FK to `dim_store`)
5. **Equipment Groups**: Created during ETL based on `inventory_equipment` combinations, reused if multiple cars have same equipment

## Business Questions Supported

The warehouse supports analysis for:
- Customer profiling (age, location, returning customers)
- Payment analysis (late payments, payment amounts)
- Rental analysis (duration, revenue, delays)
- Staff performance (by salesperson, hierarchy)
- Store performance (geographic analysis, rankings)
- Car analysis (profitability, equipment preferences)
- COVID impact analysis (pre/during/post pandemic periods)

## File Locations

- **Source Schema**: `data-structure/wheelie.dbml`
- **Warehouse Schema**: `data-structure/warehouse.dbml`
- **Documentation**: `data-structure/README.md`
- **ETL Queries**: `sql/load-db.sql`
- **Context**: `.cursor/context.md` (this file)

## Last Updated
2024-12-10

