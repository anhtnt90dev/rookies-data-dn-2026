%%sql
-- =====================================================================
-- Project: CarPro Insurance Analytics
-- Layer: Silver (Delta Tables)
-- Platform: Microsoft Fabric Lakehouse (lh_insurance_dev)
--
-- References:
--   - Documentation: docs/source-to-target-mapping/bronze-to-silver-mapping.md
--   - Configurations: docs/source-to-target-mapping/jsons/bronze-to-silver/*.json
--   - Naming Conventions: docs/standards/naming_convention.md
--
-- Purpose:
-- This script creates the Silver schema and all Silver Delta Lake tables.
-- Standardizes naming (e.g., last_updated_at and updated_at added to tables),
-- performs basic cleansing, and prepares data for Gold layer lookup.
--
-- File Location: sql/lakehouse/create_silver_tables.sql
-- =====================================================================

-- ---------------------------------------------------------------------
-- CREATE SCHEMA
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS silver;

-- =====================================================================
-- 1. JSON SOURCES mapping to Silver
-- =====================================================================

-- ---------------------------------------------------------------------
-- TABLE: silver.cancellation
-- Source Table: bronze.cancellation
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.cancellation;
CREATE TABLE silver.cancellation (
    cancellation_id     STRING,
    policy_id           STRING,
    cancellation_at     TIMESTAMP,
    cancellation_reason STRING,
    refund_amount       DECIMAL(18,2),
    last_updated_at     TIMESTAMP,
    updated_at          TIMESTAMP,
    operation_type      STRING,
    is_deleted          BOOLEAN,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.payment
-- Source Table: bronze.payment
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.payment;
CREATE TABLE silver.payment (
    payment_id              STRING,
    policy_id               STRING,
    payment_at              TIMESTAMP,
    payment_method          STRING,
    payment_status          STRING,
    payment_amount          DECIMAL(18,2),
    transaction_reference   STRING,
    last_updated_at         TIMESTAMP,
    updated_at              TIMESTAMP,
    operation_type          STRING,
    is_deleted              BOOLEAN, 
    
    -- Technical Metadata Columns
    _batch_id               STRING,
    _loaded_at              TIMESTAMP,
    _source_system          STRING,
    _source_name            STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.policy
-- Source Table: bronze.policy
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.policy;
CREATE TABLE silver.policy (
    policy_id           STRING,
    quotation_id        STRING,
    customer_id         STRING,
    provider_code       STRING,
    policy_number       STRING,
    policy_start_date   DATE,
    policy_end_date     DATE,
    policy_status       STRING,
    premium_amount      DECIMAL(18,2),
    operation_type      STRING,
    is_deleted          BOOLEAN, 
    issued_at           TIMESTAMP,
    last_updated_at     TIMESTAMP,
    updated_at          TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING
) USING DELTA;


-- =====================================================================
-- 2. CRM DATABASE SOURCES mapping to Silver
-- =====================================================================

-- ---------------------------------------------------------------------
-- TABLE: silver.customer
-- Source Table: bronze.customer
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.customer;
CREATE TABLE silver.customer (
    customer_id     STRING,
    full_name       STRING,
    gender          STRING,
    dob             DATE,
    phone_number    STRING,
    email           STRING,
    city            STRING,
    district        STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.agent
-- Source Table: bronze.agent
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.agent;
CREATE TABLE silver.agent (
    agent_id        STRING,
    agent_name      STRING,
    region          STRING,
    branch          STRING,
    manager_name    STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.provider
-- Source Table: bronze.insurance_provider
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.provider;
CREATE TABLE silver.provider (
    provider_code   STRING,
    provider_name   STRING,
    provider_group  STRING,
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id       STRING,
    _loaded_at      TIMESTAMP,
    _source_system  STRING,
    _source_name    STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.vehicle
-- Source Table: bronze.vehicle
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.vehicle;
CREATE TABLE silver.vehicle (
    vehicle_id        STRING,
    customer_id       STRING,
    plate_number      STRING,
    vehicle_brand     STRING,
    vehicle_model     STRING,
    manufacture_year  INT,
    vehicle_value     DECIMAL(18,2),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id         STRING,
    _loaded_at        TIMESTAMP,
    _source_system    STRING,
    _source_name      STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.quotation
-- Source Table: bronze.quotation
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.quotation;
CREATE TABLE silver.quotation (
    quotation_id            STRING,
    customer_id             STRING,
    agent_id                STRING,
    provider_code           STRING,
    quotation_at            TIMESTAMP,
    quotation_status        STRING,
    package_code            STRING,
    premium_amount          DECIMAL(18,2),
    quotation_expiry_at     TIMESTAMP,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id               STRING,
    _loaded_at              TIMESTAMP,
    _source_system          STRING,
    _source_name            STRING
) USING DELTA;

-- ---------------------------------------------------------------------
-- TABLE: silver.quotation_item
-- Source Table: bronze.quotation_item
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS silver.quotation_item;
CREATE TABLE silver.quotation_item (
    quotation_item_id   STRING,
    quotation_id        STRING,
    coverage_type       STRING,
    coverage_amount     DECIMAL(18,2),
    deductible_amount   DECIMAL(18,2),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    
    -- Technical Metadata Columns
    _batch_id           STRING,
    _loaded_at          TIMESTAMP,
    _source_system      STRING,
    _source_name        STRING
) USING DELTA;
