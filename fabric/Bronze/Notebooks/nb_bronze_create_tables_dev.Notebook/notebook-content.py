# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59e55d5a-c0cc-429c-8dcb-068cfbec22d2",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "d7a45747-6b09-483f-b813-8aee84a3afc6",
# META       "known_lakehouses": [
# META         {
# META           "id": "59e55d5a-c0cc-429c-8dcb-068cfbec22d2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Project: CarPro Insurance Analytics
# MAGIC -- Layer: Bronze (Delta Tables)
# MAGIC -- Platform: Microsoft Fabric Lakehouse (lh_insurance_dev)
# MAGIC --
# MAGIC -- Purpose:
# MAGIC -- This script creates the Bronze schema and all Bronze Delta Lake tables.
# MAGIC -- The Bronze layer stores raw data from source systems with minimal
# MAGIC -- transformation, preserving history and including pipeline metadata.
# MAGIC --
# MAGIC -- File Location: sql/lakehouse/create_bronze_tables.sql
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- CREATE SCHEMA
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- 1. JSON SOURCES (Append-Only Ingestion)
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.cancellation
# MAGIC -- Source System: policy_system
# MAGIC -- Source File: cancellation_full_<yyyy-MM-dd>.json
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.cancellation (
# MAGIC     cancellation_id     STRING,
# MAGIC     policy_id           STRING,
# MAGIC     cancellation_date   STRING,
# MAGIC     cancellation_reason STRING,
# MAGIC     refund_amount       STRING,
# MAGIC     last_updated        STRING,
# MAGIC     operation_type      STRING,
# MAGIC     batch_date          STRING,
# MAGIC     source_system       STRING,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id           STRING,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.payment
# MAGIC -- Source System: payment_system
# MAGIC -- Source File: payment_full_<yyyy-MM-dd>.json
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.payment (
# MAGIC     payment_id              STRING,
# MAGIC     policy_id               STRING,
# MAGIC     payment_date            STRING,
# MAGIC     payment_method          STRING,
# MAGIC     payment_status          STRING,
# MAGIC     payment_amount          STRING,
# MAGIC     transaction_reference   STRING,
# MAGIC     last_updated            STRING,
# MAGIC     operation_type          STRING,
# MAGIC     batch_date              STRING,
# MAGIC     source_system           STRING,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id               STRING,
# MAGIC     _loaded_at              TIMESTAMP,
# MAGIC     _source_system          STRING,
# MAGIC     _source_name            STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.policy
# MAGIC -- Source System: policy_system
# MAGIC -- Source File: policy_full_<yyyy-MM-dd>.json
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.policy (
# MAGIC     policy_id           STRING,
# MAGIC     quotation_id        STRING,
# MAGIC     customer_id         STRING,
# MAGIC     provider_code       STRING,
# MAGIC     policy_number       STRING,
# MAGIC     policy_start_date   STRING,
# MAGIC     policy_end_date     STRING,
# MAGIC     policy_status       STRING,
# MAGIC     premium_amount      STRING,
# MAGIC     issued_date         STRING,
# MAGIC     last_updated        STRING,
# MAGIC     operation_type      STRING,
# MAGIC     batch_date          STRING,
# MAGIC     source_system       STRING,
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
# MAGIC -- 2. CRM DATABASE SOURCES
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.customer
# MAGIC -- Source System: crm_system
# MAGIC -- Source Table: dbo.customers
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customer (
# MAGIC     customer_id     STRING,
# MAGIC     full_name       STRING,
# MAGIC     gender          STRING,
# MAGIC     dob             DATE,
# MAGIC     phone_number    STRING,
# MAGIC     email           STRING,
# MAGIC     city            STRING,
# MAGIC     district        STRING,
# MAGIC     created_date    TIMESTAMP,
# MAGIC     updated_date    TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _loaded_at      TIMESTAMP,
# MAGIC     _source_system  STRING,
# MAGIC     _source_name    STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.agent
# MAGIC -- Source System: crm_system
# MAGIC -- Source Table: dbo.agents
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.agent (
# MAGIC     agent_id        STRING,
# MAGIC     agent_name      STRING,
# MAGIC     region          STRING,
# MAGIC     branch          STRING,
# MAGIC     manager_name    STRING,
# MAGIC     created_date    TIMESTAMP,
# MAGIC     updated_date    TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _loaded_at      TIMESTAMP,
# MAGIC     _source_system  STRING,
# MAGIC     _source_name    STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.insurance_provider
# MAGIC -- Source System: crm_system
# MAGIC -- Source Table: dbo.insurance_providers
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.insurance_provider (
# MAGIC     provider_code   STRING,
# MAGIC     provider_name   STRING,
# MAGIC     provider_group  STRING,
# MAGIC     active_flag     INT,
# MAGIC     created_date    TIMESTAMP,
# MAGIC     updated_date    TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _loaded_at      TIMESTAMP,
# MAGIC     _source_system  STRING,
# MAGIC     _source_name    STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.vehicle
# MAGIC -- Source System: crm_system
# MAGIC -- Source Table: dbo.vehicle
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.vehicle (
# MAGIC     vehicle_id        STRING,
# MAGIC     customer_id       STRING,
# MAGIC     plate_number      STRING,
# MAGIC     vehicle_brand     STRING,
# MAGIC     vehicle_model     STRING,
# MAGIC     manufacture_year  INT,
# MAGIC     vehicle_value     DECIMAL(18,2),
# MAGIC     created_date      TIMESTAMP,
# MAGIC     updated_date      TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id         STRING,
# MAGIC     _loaded_at        TIMESTAMP,
# MAGIC     _source_system    STRING,
# MAGIC     _source_name      STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.quotation
# MAGIC -- Source System: crm_system
# MAGIC -- Source Table: dbo.quotation
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.quotation (
# MAGIC     quotation_id            STRING,
# MAGIC     customer_id             STRING,
# MAGIC     agent_id                STRING,
# MAGIC     provider_code           STRING,
# MAGIC     quotation_date          TIMESTAMP,
# MAGIC     quotation_status        STRING,
# MAGIC     package_code            STRING,
# MAGIC     premium_amount          DECIMAL(18,2),
# MAGIC     quotation_expiry_date   TIMESTAMP,
# MAGIC     created_date            TIMESTAMP,
# MAGIC     updated_date            TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id               STRING,
# MAGIC     _loaded_at              TIMESTAMP,
# MAGIC     _source_system          STRING,
# MAGIC     _source_name            STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.quotation_item
# MAGIC -- Source System: crm_system
# MAGIC -- Source Table: dbo.quotation_item
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze.quotation_item (
# MAGIC     quotation_item_id   STRING,
# MAGIC     quotation_id        STRING,
# MAGIC     coverage_type       STRING,
# MAGIC     coverage_amount     DECIMAL(18,2),
# MAGIC     deductible_amount   DECIMAL(18,2),
# MAGIC     created_date        TIMESTAMP,
# MAGIC     updated_date        TIMESTAMP,
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
