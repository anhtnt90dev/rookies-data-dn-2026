# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b883e6d2-ee4b-4338-a694-4b81d338dd49",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "ddc0f61e-f221-421b-a87b-f80ffce2c8df",
# META       "known_lakehouses": [
# META         {
# META           "id": "b883e6d2-ee4b-4338-a694-4b81d338dd49"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Project: CarPro Insurance Analytics
# MAGIC -- Layer: Silver (Delta Tables)
# MAGIC -- Platform: Microsoft Fabric Lakehouse (lh_insurance_dev)
# MAGIC --
# MAGIC -- Purpose:
# MAGIC -- This script creates the Silver schema and all Silver Delta Lake tables.
# MAGIC -- The Silver layer stores cleansed, typed, and standardized data.
# MAGIC --
# MAGIC -- File Location: sql/lakehouse/create_silver_tables.sql
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- CREATE SCHEMA
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- 1. JSON SOURCES mapping to Silver
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.cancellation
# MAGIC -- Source Table: bronze.cancellation
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.cancellation;
# MAGIC CREATE TABLE silver.cancellation (
# MAGIC     cancellation_id     STRING,
# MAGIC     policy_id           STRING,
# MAGIC     cancellation_at     TIMESTAMP,
# MAGIC     cancellation_reason STRING,
# MAGIC     refund_amount       DECIMAL(18,2),
# MAGIC     last_updated        TIMESTAMP,
# MAGIC     operation_type      STRING,
# MAGIC     is_deleted          BOOLEAN,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id           STRING,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.payment
# MAGIC -- Source Table: bronze.payment
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.payment;
# MAGIC CREATE TABLE silver.payment (
# MAGIC     payment_id              STRING,
# MAGIC     policy_id               STRING,
# MAGIC     payment_at              TIMESTAMP,
# MAGIC     payment_method          STRING,
# MAGIC     payment_status          STRING,
# MAGIC     payment_amount          DECIMAL(18,2),
# MAGIC     transaction_reference   STRING,
# MAGIC     last_updated            TIMESTAMP,
# MAGIC     operation_type          STRING,
# MAGIC     is_deleted              BOOLEAN, 
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id               STRING,
# MAGIC     _loaded_at              TIMESTAMP,
# MAGIC     _source_system          STRING,
# MAGIC     _source_name            STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.policy
# MAGIC -- Source Table: bronze.policy
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.policy;
# MAGIC CREATE TABLE silver.policy (
# MAGIC     policy_id           STRING,
# MAGIC     quotation_id        STRING,
# MAGIC     customer_id         STRING,
# MAGIC     provider_code       STRING,
# MAGIC     policy_number       STRING,
# MAGIC     policy_start_date   DATE,
# MAGIC     policy_end_date     DATE,
# MAGIC     policy_status       STRING,
# MAGIC     premium_amount      DECIMAL(18,2),
# MAGIC     operation_type      STRING,
# MAGIC     is_deleted          BOOLEAN, 
# MAGIC     issued_at           TIMESTAMP,
# MAGIC     last_updated_at     TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id           STRING,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- 2. CRM DATABASE SOURCES mapping to Silver
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.customer
# MAGIC -- Source Table: bronze.customer
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.customer;
# MAGIC CREATE TABLE silver.customer (
# MAGIC     customer_id     STRING,
# MAGIC     full_name       STRING,
# MAGIC     gender          STRING,
# MAGIC     dob             DATE,
# MAGIC     phone_number    STRING,
# MAGIC     email           STRING,
# MAGIC     city            STRING,
# MAGIC     district        STRING,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _loaded_at      TIMESTAMP,
# MAGIC     _source_system  STRING,
# MAGIC     _source_name    STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.agent
# MAGIC -- Source Table: bronze.agent
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.agent;
# MAGIC CREATE TABLE silver.agent (
# MAGIC     agent_id        STRING,
# MAGIC     agent_name      STRING,
# MAGIC     region          STRING,
# MAGIC     branch          STRING,
# MAGIC     manager_name    STRING,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _loaded_at      TIMESTAMP,
# MAGIC     _source_system  STRING,
# MAGIC     _source_name    STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.provider
# MAGIC -- Source Table: bronze.insurance_provider
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.provider;
# MAGIC CREATE TABLE silver.provider (
# MAGIC     provider_code   STRING,
# MAGIC     provider_name   STRING,
# MAGIC     provider_group  STRING,
# MAGIC     is_active       BOOLEAN,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _loaded_at      TIMESTAMP,
# MAGIC     _source_system  STRING,
# MAGIC     _source_name    STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.vehicle
# MAGIC -- Source Table: bronze.vehicle
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.vehicle;
# MAGIC CREATE TABLE silver.vehicle (
# MAGIC     vehicle_id        STRING,
# MAGIC     customer_id       STRING,
# MAGIC     plate_number      STRING,
# MAGIC     vehicle_brand     STRING,
# MAGIC     vehicle_model     STRING,
# MAGIC     manufacture_year  INT,
# MAGIC     vehicle_value     DECIMAL(18,2),
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id         STRING,
# MAGIC     _loaded_at        TIMESTAMP,
# MAGIC     _source_system    STRING,
# MAGIC     _source_name      STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.quotation
# MAGIC -- Source Table: bronze.quotation
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.quotation;
# MAGIC CREATE TABLE silver.quotation (
# MAGIC     quotation_id            STRING,
# MAGIC     customer_id             STRING,
# MAGIC     agent_id                STRING,
# MAGIC     provider_code           STRING,
# MAGIC     quotation_at            TIMESTAMP,
# MAGIC     quotation_status        STRING,
# MAGIC     package_code            STRING,
# MAGIC     premium_amount          DECIMAL(18,2),
# MAGIC     quotation_expiry_at     TIMESTAMP,
# MAGIC     created_at              TIMESTAMP,
# MAGIC     updated_at              TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id               STRING,
# MAGIC     _loaded_at              TIMESTAMP,
# MAGIC     _source_system          STRING,
# MAGIC     _source_name            STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: silver.quotation_item
# MAGIC -- Source Table: bronze.quotation_item
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS silver.quotation_item;
# MAGIC CREATE TABLE silver.quotation_item (
# MAGIC     quotation_item_id   STRING,
# MAGIC     quotation_id        STRING,
# MAGIC     coverage_type       STRING,
# MAGIC     coverage_amount     DECIMAL(18,2),
# MAGIC     deductible_amount   DECIMAL(18,2),
# MAGIC     created_at          TIMESTAMP,
# MAGIC     updated_at          TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id           STRING,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
