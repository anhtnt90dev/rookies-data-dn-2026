%%sql
-- =====================================================================
-- Project: CarPro Insurance Analytics
-- Layer: Gold (Star Schema / Dimensional Model)
-- Platform: Microsoft Fabric Lakehouse (lh_insurance_dev)
--
-- References:
--   - Documentation: docs/source-to-target-mapping/silver-to-gold-mapping.md
--   - Design Docs: docs/data-modeling/dimensional-design/02-dimensional-table-structures-design.md
--   - Configurations: docs/source-to-target-mapping/jsons/silver-to-gold/*.json
--
-- Purpose:
-- This script creates the Gold schema, Dimension and Fact tables.
-- Aligns with star schema specs (SCD Type 1 & 2 dimensions simplified;
-- fact tables updated with converted_flag and technical soft-delete/lineage).
--
-- File Location: sql/lakehouse/create_gold_tables.sql
-- =====================================================================

-- ---------------------------------------------------------------------
-- CREATE SCHEMA
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS gold;

-- =====================================================================
-- 1. DIMENSION TABLES
-- =====================================================================

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_date
-- Source: Generated Calendar
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_date;
CREATE TABLE gold.dim_date (
    date_key       INT,
    full_date      DATE,
    day_number     INT,
    day_name       STRING,
    week_number    INT,
    month_number   INT,
    month_name     STRING,
    quarter_number INT,
    year_number    INT,
    year_month     STRING,
    is_weekend     BOOLEAN
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_customer
-- Source Table: silver.customer (SCD Type 2)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_customer;
CREATE TABLE gold.dim_customer (
    customer_key    BIGINT,
    customer_id     STRING,
    full_name       STRING,
    gender          STRING,
    dob             DATE,
    phone_number    STRING,
    email           STRING,
    city            STRING,
    district        STRING,
    effective_from  TIMESTAMP,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_agent
-- Source Table: silver.agent (SCD Type 2)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_agent;
CREATE TABLE gold.dim_agent (
    agent_key       BIGINT,
    agent_id        STRING,
    agent_name      STRING,
    region          STRING,
    branch          STRING,
    manager_name    STRING,
    effective_from  TIMESTAMP,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_provider
-- Source Table: silver.provider (SCD Type 2)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_provider;
CREATE TABLE gold.dim_provider (
    provider_key    BIGINT,
    provider_code   STRING,
    provider_name   STRING,
    provider_group  STRING,
    is_active       BOOLEAN,
    effective_from  TIMESTAMP,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_package
-- Source Table: silver.quotation (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_package;
CREATE TABLE gold.dim_package (
    package_key     BIGINT,
    package_code    STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_coverage
-- Source Table: silver.quotation_item (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_coverage;
CREATE TABLE gold.dim_coverage (
    coverage_key    BIGINT,
    coverage_type   STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_quotation
-- Source Table: silver.quotation (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_quotation;
CREATE TABLE gold.dim_quotation (
    quotation_key        BIGINT,
    quotation_id         STRING,
    quotation_expiry_date DATE,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id            STRING,
    _loaded_at           TIMESTAMP,
    _source_system       STRING,
    _source_name         STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_policy
-- Source Table: silver.policy (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_policy;
CREATE TABLE gold.dim_policy (
    policy_key         BIGINT,
    policy_id          STRING,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id          STRING,
    _loaded_at         TIMESTAMP,
    _source_system     STRING,
    _source_name       STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_quotation_status
-- Source Table: silver.quotation (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_quotation_status;
CREATE TABLE gold.dim_quotation_status (
    quotation_status_key  BIGINT,
    quotation_status_code STRING,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id             STRING,
    _loaded_at            TIMESTAMP,
    _source_system        STRING,
    _source_name          STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_policy_status
-- Source Table: silver.policy (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_policy_status;
CREATE TABLE gold.dim_policy_status (
    policy_status_key   BIGINT,
    policy_status_code  STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_payment_status
-- Source Table: silver.payment (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_payment_status;
CREATE TABLE gold.dim_payment_status (
    payment_status_key    BIGINT,
    payment_status_code   STRING,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id             STRING,
    _loaded_at            TIMESTAMP,
    _source_system        STRING,
    _source_name          STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_payment_method
-- Source Table: silver.payment (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_payment_method;
CREATE TABLE gold.dim_payment_method (
    payment_method_key   BIGINT,
    payment_method_code  STRING,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id            STRING,
    _loaded_at           TIMESTAMP,
    _source_system       STRING,
    _source_name         STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_cancellation_reason
-- Source Table: silver.cancellation (SCD Type 1)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_cancellation_reason;
CREATE TABLE gold.dim_cancellation_reason (
    cancellation_reason_key BIGINT,
    cancellation_reason     STRING,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id               STRING,
    _loaded_at              TIMESTAMP,
    _source_system          STRING,
    _source_name            STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.dim_vehicle
-- Source Table: silver.vehicle (SCD Type 2)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_vehicle;
CREATE TABLE gold.dim_vehicle (
    vehicle_key       BIGINT,
    vehicle_id        STRING,
    customer_id       STRING,
    plate_number      STRING,
    vehicle_brand     STRING,
    vehicle_model     STRING,
    manufacture_year  INT,
    vehicle_value     DECIMAL(18,2),
    effective_from    TIMESTAMP,
    effective_to      TIMESTAMP,
    is_current        BOOLEAN,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id         STRING,
    _loaded_at        TIMESTAMP,
    _source_system    STRING,
    _source_name      STRING
) USING DELTA;


-- =====================================================================
-- 2. FACT TABLES
-- =====================================================================

-- ---------------------------------------------------------------------
-- TABLE: gold.fact_quotation
-- Source Table: silver.quotation
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_quotation;
CREATE TABLE gold.fact_quotation (
    -- Degenerate Dimensions / Business Keys
    quotation_id              STRING,
    customer_id               STRING,
    agent_id                  STRING,
    provider_code             STRING,
    
    -- Dimension Foreign Keys
    customer_key              BIGINT,
    agent_key                 BIGINT,
    provider_key              BIGINT,
    package_key               BIGINT,
    quotation_status_key      BIGINT,
    quotation_date_key        INT,
    quotation_expiry_date_key INT,
    vehicle_key               BIGINT, -- Resolved via customer_id context
    
    -- Measures
    premium_amount            DECIMAL(18,2),
    converted_flag            BOOLEAN,
    
    -- Metadata / Audit columns
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    _batch_id                 STRING,
    _source_system            STRING,
    pipeline_run_id           STRING,
    is_deleted                BOOLEAN,
    deleted_at                TIMESTAMP,
    delete_batch_id           STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.fact_quotation_item
-- Source Tables: silver.quotation_item (primary), silver.quotation (header)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_quotation_item;
CREATE TABLE gold.fact_quotation_item (
    -- Degenerate Dimensions / Business Keys
    quotation_item_id         STRING,
    quotation_id              STRING,
    
    -- Dimension Foreign Keys
    quotation_key             BIGINT,
    quotation_date_key        INT,
    customer_key              BIGINT,
    agent_key                 BIGINT,
    provider_key              BIGINT,
    package_key               BIGINT,
    quotation_status_key      BIGINT,
    coverage_key              BIGINT,
    vehicle_key               BIGINT, -- Resolved via customer_id context
    
    -- Measures
    coverage_amount           DECIMAL(18,2),
    deductible_amount         DECIMAL(18,2),
    
    -- Metadata / Audit columns
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    _batch_id                 STRING,
    _source_system            STRING,
    pipeline_run_id           STRING,
    is_deleted                BOOLEAN,
    deleted_at                TIMESTAMP,
    delete_batch_id           STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.fact_policy
-- Source Tables: silver.policy (primary), silver.quotation (context)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_policy;
CREATE TABLE gold.fact_policy (
    -- Degenerate Dimensions / Business Keys
    policy_id                 STRING,
    policy_number             STRING,
    quotation_id              STRING,
    customer_id               STRING,
    provider_code             STRING,
    
    -- Dimension Foreign Keys
    policy_key                BIGINT,
    quotation_key             BIGINT,
    customer_key              BIGINT,
    provider_key              BIGINT,
    agent_key                 BIGINT,
    package_key               BIGINT,
    policy_status_key         BIGINT,
    issued_date_key           INT,
    policy_start_date_key     INT,
    policy_end_date_key       INT,
    vehicle_key               BIGINT, -- Resolved via customer_id context
    
    -- Measures
    premium_amount            DECIMAL(18,2),
    
    -- Metadata / Audit columns
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    _batch_id                 STRING,
    _source_system            STRING,
    pipeline_run_id           STRING,
    is_deleted                BOOLEAN,
    deleted_at                TIMESTAMP,
    delete_batch_id           STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.fact_payment
-- Source Tables: silver.payment (primary), silver.policy (context)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_payment;
CREATE TABLE gold.fact_payment (
    -- Degenerate Dimensions / Business Keys
    payment_id                STRING,
    policy_id                 STRING,
    transaction_reference     STRING,
    
    -- Dimension Foreign Keys
    policy_key                BIGINT,
    payment_status_key        BIGINT,
    payment_method_key        BIGINT,
    payment_date_key          INT,
    customer_key              BIGINT,
    provider_key              BIGINT,
    vehicle_key               BIGINT, -- Resolved via customer_id context
    
    -- Measures
    payment_amount            DECIMAL(18,2),
    
    -- Metadata / Audit columns
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    _batch_id                 STRING,
    _source_system            STRING,
    pipeline_run_id           STRING,
    is_deleted                BOOLEAN,
    deleted_at                TIMESTAMP,
    delete_batch_id           STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: gold.fact_cancellation
-- Source Tables: silver.cancellation (primary), silver.policy (context)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_cancellation;
CREATE TABLE gold.fact_cancellation (
    -- Degenerate Dimensions / Business Keys
    cancellation_id           STRING,
    policy_id                 STRING,
    
    -- Dimension Foreign Keys
    policy_key                BIGINT,
    cancellation_reason_key   BIGINT,
    cancellation_date_key     INT,
    customer_key              BIGINT,
    provider_key              BIGINT,
    vehicle_key               BIGINT, -- Resolved via customer_id context
    
    -- Measures
    refund_amount             DECIMAL(18,2),
    
    -- Metadata / Audit columns
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    _batch_id                 STRING,
    _source_system            STRING,
    pipeline_run_id           STRING,
    is_deleted                BOOLEAN,
    deleted_at                TIMESTAMP,
    delete_batch_id           STRING
) USING DELTA;