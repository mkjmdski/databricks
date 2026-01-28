%sql
DROP CATALOG IF EXISTS wheelie CASCADE;
CREATE CATALOG IF NOT EXISTS wheelie;
GRANT ALL PRIVILEGES ON CATALOG wheelie TO `mm151@st.amu.edu.pl`;
GRANT ALL PRIVILEGES ON CATALOG wheelie TO `sebpie2@st.amu.edu.pl`;
GRANT ALL PRIVILEGES ON CATALOG wheelie TO `tr32342@st.amu.edu.pl`;
GRANT ALL PRIVILEGES ON CATALOG wheelie TO `mikmlo@st.amu.edu.pl`;
GRANT ALL PRIVILEGES ON CATALOG wheelie TO `a662d958-d69f-42df-b30c-66cb1c96944e`;

-- ==============================================================================
-- LAYER ARCHITECTURE: Bronze → Silver (in-memory) → Gold
-- ==============================================================================

-- Bronze layer: Raw data from source systems (persisted)
CREATE SCHEMA IF NOT EXISTS wheelie.bronze
COMMENT 'Raw data from source MySQL database (upsert for incremental loading)';

-- Gold layer: Business-ready dimensional model (persisted)
CREATE SCHEMA IF NOT EXISTS wheelie.gold
COMMENT 'Star schema data warehouse - dimensions and facts';

-- Monitoring schema: ETL metadata and audit logs
CREATE SCHEMA IF NOT EXISTS wheelie.monitoring
COMMENT 'Watermarks, audit logs, and ETL monitoring';
-- ==============================================================================
-- MONITORING TABLES FOR INCREMENTAL LOADING
-- ==============================================================================

-- Watermark tracking table for incremental loading
CREATE TABLE IF NOT EXISTS wheelie.monitoring.watermarks (
    table_name STRING NOT NULL COMMENT 'Source table name (e.g., customer, rental)',
    watermark_timestamp TIMESTAMP NOT NULL COMMENT 'Last successfully loaded timestamp',
    watermark_column STRING NOT NULL COMMENT 'Column used for watermark (e.g., last_update, rental_date)',
    row_count BIGINT COMMENT 'Number of rows loaded in last incremental run',
    load_type STRING COMMENT 'FULL or INCREMENTAL',
    updated_at TIMESTAMP NOT NULL COMMENT 'When watermark was last updated',
    updated_by STRING COMMENT 'Job/user that updated the watermark',
    CONSTRAINT pk_watermarks PRIMARY KEY (table_name)
) USING DELTA
COMMENT 'Tracks watermarks for incremental data loading';

-- Change audit table for tracking updates/inserts
CREATE TABLE IF NOT EXISTS wheelie.monitoring.change_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    table_name STRING NOT NULL COMMENT 'Target table name (e.g., dim_customer)',
    operation STRING NOT NULL COMMENT 'INSERT, UPDATE, SCD2_NEW_VERSION, SCD2_CLOSE_OLD',
    business_key STRING NOT NULL COMMENT 'Business key value (e.g., customer_id=123)',
    surrogate_key STRING COMMENT 'Surrogate key value (e.g., customer_key=456)',
    changed_columns ARRAY<STRING> COMMENT 'List of columns that changed',
    old_values MAP<STRING, STRING> COMMENT 'Old values for changed columns',
    new_values MAP<STRING, STRING> COMMENT 'New values for changed columns',
    audit_timestamp TIMESTAMP NOT NULL COMMENT 'When the change was recorded',
    batch_id STRING COMMENT 'Unique identifier for the ETL batch',
    CONSTRAINT pk_change_audit PRIMARY KEY (audit_id)
) USING DELTA
PARTITIONED BY (table_name)
COMMENT 'Audit log for all data changes in gold layer';
