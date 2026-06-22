# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f6154ec7-4dbf-44f7-a335-159149f2ae56",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "c86fdecc-7ed1-42f4-9ec0-4b0274a76958",
# META       "known_lakehouses": [
# META         {
# META           "id": "f6154ec7-4dbf-44f7-a335-159149f2ae56"
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
# MAGIC -- References:
# MAGIC --   - Documentation: docs/source-to-target-mapping/source-to-bronze-mapping.md
# MAGIC --   - Configurations: docs/source-to-target-mapping/jsons/source-to-bronze/*.json
# MAGIC --   - Naming Conventions: docs/standards/naming_convention.md
# MAGIC --
# MAGIC -- Purpose:
# MAGIC -- This script creates the Bronze schema and all Bronze Delta Lake tables.
# MAGIC -- Column definitions match target specifications in the mapping files
# MAGIC -- (e.g., last_updated_at and _source_file mapped for JSON sources).
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
# MAGIC -- Source File: cancellation_*.json
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS bronze.cancellation;
# MAGIC CREATE TABLE bronze.cancellation (
# MAGIC     -- Business Columns (Raw JSON values read as STRING)
# MAGIC     cancellation_id     STRING,
# MAGIC     policy_id           STRING,
# MAGIC     cancellation_date   STRING,
# MAGIC     cancellation_reason STRING,
# MAGIC     refund_amount       STRING,
# MAGIC     last_updated_at     TIMESTAMP,
# MAGIC     source_system       STRING,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id           STRING,
# MAGIC     _operation_type     STRING,
# MAGIC     _batch_date         DATE,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING,
# MAGIC     _source_file        STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.payment
# MAGIC -- Source System: payment_system
# MAGIC -- Source File: payment_*.json
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS bronze.payment;
# MAGIC CREATE TABLE bronze.payment (
# MAGIC     -- Business Columns (Raw JSON values read as STRING)
# MAGIC     payment_id              STRING,
# MAGIC     policy_id               STRING,
# MAGIC     payment_date            STRING,
# MAGIC     payment_method          STRING,
# MAGIC     payment_status          STRING,
# MAGIC     payment_amount          STRING,
# MAGIC     transaction_reference   STRING,
# MAGIC     last_updated_at         TIMESTAMP,
# MAGIC     source_system           STRING,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id               STRING,
# MAGIC     _operation_type         STRING,
# MAGIC     _batch_date             DATE,
# MAGIC     _loaded_at              TIMESTAMP,
# MAGIC     _source_system          STRING,
# MAGIC     _source_name            STRING,
# MAGIC     _source_file            STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: bronze.policy
# MAGIC -- Source System: policy_system
# MAGIC -- Source File: policy_*.json
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS bronze.policy;
# MAGIC CREATE TABLE bronze.policy (
# MAGIC     -- Business Columns (Raw JSON values read as STRING)
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
# MAGIC     last_updated_at     TIMESTAMP,
# MAGIC     source_system       STRING,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id           STRING,
# MAGIC     _operation_type     STRING,
# MAGIC     _batch_date         DATE,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING,
# MAGIC     _source_file        STRING
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
# MAGIC DROP TABLE IF EXISTS bronze.customer;
# MAGIC CREATE TABLE bronze.customer (
# MAGIC     -- Business Columns
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
# MAGIC     _operation_type STRING,
# MAGIC     _batch_date     DATE,
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
# MAGIC DROP TABLE IF EXISTS bronze.agent;
# MAGIC CREATE TABLE bronze.agent (
# MAGIC     -- Business Columns
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
# MAGIC     _operation_type STRING,
# MAGIC     _batch_date     DATE,
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
# MAGIC DROP TABLE IF EXISTS bronze.insurance_provider;
# MAGIC CREATE TABLE bronze.insurance_provider (
# MAGIC     -- Business Columns
# MAGIC     provider_code   STRING,
# MAGIC     provider_name   STRING,
# MAGIC     provider_group  STRING,
# MAGIC     active_flag     INT,
# MAGIC     created_date    TIMESTAMP,
# MAGIC     updated_date    TIMESTAMP,
# MAGIC     
# MAGIC     -- Technical Metadata Columns
# MAGIC     _batch_id       STRING,
# MAGIC     _operation_type STRING,
# MAGIC     _batch_date     DATE,
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
# MAGIC DROP TABLE IF EXISTS bronze.vehicle;
# MAGIC CREATE TABLE bronze.vehicle (
# MAGIC     -- Business Columns
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
# MAGIC     _operation_type   STRING,
# MAGIC     _batch_date       DATE,
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
# MAGIC DROP TABLE IF EXISTS bronze.quotation;
# MAGIC CREATE TABLE bronze.quotation (
# MAGIC     -- Business Columns
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
# MAGIC     _operation_type         STRING,
# MAGIC     _batch_date             DATE,
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
# MAGIC DROP TABLE IF EXISTS bronze.quotation_item;
# MAGIC CREATE TABLE bronze.quotation_item (
# MAGIC     -- Business Columns
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
# MAGIC     _operation_type     STRING,
# MAGIC     _batch_date         DATE,
# MAGIC     _loaded_at          TIMESTAMP,
# MAGIC     _source_system      STRING,
# MAGIC     _source_name        STRING
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""TRUNCATE TABLE bronze.cancellation""")

spark.sql("""TRUNCATE TABLE bronze.payment""")

spark.sql("""TRUNCATE TABLE bronze.policy""")

spark.sql("""TRUNCATE TABLE bronze.customer""")

spark.sql("""TRUNCATE TABLE bronze.agent""")

spark.sql("""TRUNCATE TABLE bronze.insurance_provider""")

spark.sql("""TRUNCATE TABLE bronze.vehicle""")

spark.sql("""TRUNCATE TABLE bronze.quotation""")

spark.sql("""TRUNCATE TABLE bronze.quotation_item""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
