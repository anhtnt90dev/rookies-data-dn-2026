%%sql
-- =====================================================================
-- Project: CarPro Insurance Analytics
-- Layer: Bronze (Delta Tables)
-- Platform: Microsoft Fabric Lakehouse (lh_insurance_dev)
--
-- References:
--   - Documentation: docs/source-to-target-mapping/source-to-bronze-mapping.md
--   - Configurations: docs/source-to-target-mapping/jsons/source-to-bronze/*.json
--   - Naming Conventions: docs/standards/naming_convention.md
--
-- Purpose:
-- This script creates the Bronze schema and all Bronze Delta Lake tables.
-- Column definitions match target specifications in the mapping files
-- (e.g., last_updated_at and _source_file mapped for JSON sources).
--
-- File Location: sql/lakehouse/create_bronze_tables.sql
-- =====================================================================

-- ---------------------------------------------------------------------
-- CREATE SCHEMA
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;

-- =====================================================================
-- 1. JSON SOURCES (Append-Only Ingestion)
-- =====================================================================

-- ---------------------------------------------------------------------
-- TABLE: bronze.cancellation
-- Source System: policy_system
-- Source File: cancellation_*.json
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.cancellation;
CREATE TABLE bronze.cancellation (
    -- Business Columns (Raw JSON values read as STRING)
    cancellation_id     STRING,
    policy_id           STRING,
    cancellation_date   STRING,
    cancellation_reason STRING,
    refund_amount       STRING,
    last_updated_at     TIMESTAMP,
    source_system       STRING,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _operation_type     STRING,
    _batch_date         DATE,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING,
    _source_file        STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.payment
-- Source System: payment_system
-- Source File: payment_*.json
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.payment;
CREATE TABLE bronze.payment (
    -- Business Columns (Raw JSON values read as STRING)
    payment_id              STRING,
    policy_id               STRING,
    payment_date            STRING,
    payment_method          STRING,
    payment_status          STRING,
    payment_amount          STRING,
    transaction_reference   STRING,
    last_updated_at         TIMESTAMP,
    source_system           STRING,
    
    -- Technical Metadata Columns
    _batch_id               STRING,
    _operation_type         STRING,
    _batch_date             DATE,
    _loaded_at              TIMESTAMP,
    _source_system          STRING,
    _source_name            STRING,
    _source_file            STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.policy
-- Source System: policy_system
-- Source File: policy_*.json
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.policy;
CREATE TABLE bronze.policy (
    -- Business Columns (Raw JSON values read as STRING)
    policy_id           STRING,
    quotation_id        STRING,
    customer_id         STRING,
    provider_code       STRING,
    policy_number       STRING,
    policy_start_date   STRING,
    policy_end_date     STRING,
    policy_status       STRING,
    premium_amount      STRING,
    issued_date         STRING,
    last_updated_at     TIMESTAMP,
    source_system       STRING,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _operation_type     STRING,
    _batch_date         DATE,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING,
    _source_file        STRING
) USING DELTA;


-- =====================================================================
-- 2. CRM DATABASE SOURCES
-- =====================================================================

-- ---------------------------------------------------------------------
-- TABLE: bronze.customer
-- Source System: crm_system
-- Source Table: dbo.customers
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.customer;
CREATE TABLE bronze.customer (
    -- Business Columns
    customer_id     STRING,
    full_name       STRING,
    gender          STRING,
    dob             DATE,
    phone_number    STRING,
    email           STRING,
    city            STRING,
    district        STRING,
    created_date    TIMESTAMP,
    updated_date    TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _operation_type STRING,
    _batch_date     DATE,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.agent
-- Source System: crm_system
-- Source Table: dbo.agents
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.agent;
CREATE TABLE bronze.agent (
    -- Business Columns
    agent_id        STRING,
    agent_name      STRING,
    region          STRING,
    branch          STRING,
    manager_name    STRING,
    created_date    TIMESTAMP,
    updated_date    TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _operation_type STRING,
    _batch_date     DATE,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.insurance_provider
-- Source System: crm_system
-- Source Table: dbo.insurance_providers
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.insurance_provider;
CREATE TABLE bronze.insurance_provider (
    -- Business Columns
    provider_code   STRING,
    provider_name   STRING,
    provider_group  STRING,
    active_flag     INT,
    created_date    TIMESTAMP,
    updated_date    TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _operation_type STRING,
    _batch_date     DATE,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.vehicle
-- Source System: crm_system
-- Source Table: dbo.vehicle
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.vehicle;
CREATE TABLE bronze.vehicle (
    -- Business Columns
    vehicle_id        STRING,
    customer_id       STRING,
    plate_number      STRING,
    vehicle_brand     STRING,
    vehicle_model     STRING,
    manufacture_year  INT,
    vehicle_value     DECIMAL(18,2),
    created_date      TIMESTAMP,
    updated_date      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id         STRING,
    _operation_type   STRING,
    _batch_date       DATE,
    _loaded_at        TIMESTAMP,
    _source_system    STRING,
    _source_name      STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.quotation
-- Source System: crm_system
-- Source Table: dbo.quotation
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.quotation;
CREATE TABLE bronze.quotation (
    -- Business Columns
    quotation_id            STRING,
    customer_id             STRING,
    agent_id                STRING,
    provider_code           STRING,
    quotation_date          TIMESTAMP,
    quotation_status        STRING,
    package_code            STRING,
    premium_amount          DECIMAL(18,2),
    quotation_expiry_date   TIMESTAMP,
    created_date            TIMESTAMP,
    updated_date            TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id               STRING,
    _operation_type         STRING,
    _batch_date             DATE,
    _loaded_at              TIMESTAMP,
    _source_system          STRING,
    _source_name            STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: bronze.quotation_item
-- Source System: crm_system
-- Source Table: dbo.quotation_item
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.quotation_item;
CREATE TABLE bronze.quotation_item (
    -- Business Columns
    quotation_item_id   STRING,
    quotation_id        STRING,
    coverage_type       STRING,
    coverage_amount     DECIMAL(18,2),
    deductible_amount   DECIMAL(18,2),
    created_date        TIMESTAMP,
    updated_date        TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _operation_type     STRING,
    _batch_date         DATE,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING
) USING DELTA;
