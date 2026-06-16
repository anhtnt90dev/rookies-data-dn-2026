# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "126c09a8-79bf-4e16-9e56-5e7c93311e29",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "6358469d-5cd2-48a3-8d0f-c9583b40d1fa",
# META       "known_lakehouses": [
# META         {
# META           "id": "126c09a8-79bf-4e16-9e56-5e7c93311e29"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Project: CarPro Insurance Analytics
# MAGIC -- Layer: Gold (Star Schema / Dimensional Model)
# MAGIC -- Platform: Microsoft Fabric Lakehouse (lh_insurance_dev)
# MAGIC --
# MAGIC -- References:
# MAGIC --   - Documentation: docs/source-to-target-mapping/silver-to-gold-mapping.md
# MAGIC --   - Design Docs: docs/data-modeling/dimensional-design/02-dimensional-table-structures-design.md
# MAGIC --   - Configurations: docs/source-to-target-mapping/jsons/silver-to-gold/*.json
# MAGIC --
# MAGIC -- Purpose:
# MAGIC -- This script creates the Gold schema, Dimension and Fact tables.
# MAGIC -- Aligns with star schema specs (SCD Type 1 & 2 dimensions simplified;
# MAGIC -- fact tables updated with converted_flag and technical soft-delete/lineage).
# MAGIC --
# MAGIC -- File Location: sql/lakehouse/create_gold_tables.sql
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- CREATE SCHEMA
# MAGIC -- ---------------------------------------------------------------------
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- 1. DIMENSION TABLES
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_date
# MAGIC -- Source: Generated Calendar
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_date;
# MAGIC CREATE TABLE gold.dim_date (
# MAGIC     date_key       INT,
# MAGIC     full_date      DATE,
# MAGIC     day_number     INT,
# MAGIC     day_name       STRING,
# MAGIC     week_number    INT,
# MAGIC     month_number   INT,
# MAGIC     month_name     STRING,
# MAGIC     quarter_number INT,
# MAGIC     year_number    INT,
# MAGIC     year_month     STRING,
# MAGIC     is_weekend     BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_customer
# MAGIC -- Source Table: silver.customer (SCD Type 2)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_customer;
# MAGIC CREATE TABLE gold.dim_customer (
# MAGIC     customer_key    BIGINT,
# MAGIC     customer_id     STRING,
# MAGIC     full_name       STRING,
# MAGIC     gender          STRING,
# MAGIC     dob             DATE,
# MAGIC     phone_number    STRING,
# MAGIC     email           STRING,
# MAGIC     city            STRING,
# MAGIC     district        STRING,
# MAGIC     effective_from  TIMESTAMP,
# MAGIC     effective_to    TIMESTAMP,
# MAGIC     is_current      BOOLEAN,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_agent
# MAGIC -- Source Table: silver.agent (SCD Type 2)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_agent;
# MAGIC CREATE TABLE gold.dim_agent (
# MAGIC     agent_key       BIGINT,
# MAGIC     agent_id        STRING,
# MAGIC     agent_name      STRING,
# MAGIC     region          STRING,
# MAGIC     branch          STRING,
# MAGIC     manager_name    STRING,
# MAGIC     effective_from  TIMESTAMP,
# MAGIC     effective_to    TIMESTAMP,
# MAGIC     is_current      BOOLEAN,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_provider
# MAGIC -- Source Table: silver.provider (SCD Type 2)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_provider;
# MAGIC CREATE TABLE gold.dim_provider (
# MAGIC     provider_key    BIGINT,
# MAGIC     provider_code   STRING,
# MAGIC     provider_name   STRING,
# MAGIC     provider_group  STRING,
# MAGIC     active_flag     INT,
# MAGIC     effective_from  TIMESTAMP,
# MAGIC     effective_to    TIMESTAMP,
# MAGIC     is_current      BOOLEAN,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_package
# MAGIC -- Source Table: silver.quotation (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_package;
# MAGIC CREATE TABLE gold.dim_package (
# MAGIC     package_key     BIGINT,
# MAGIC     package_code    STRING,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_coverage
# MAGIC -- Source Table: silver.quotation_item (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_coverage;
# MAGIC CREATE TABLE gold.dim_coverage (
# MAGIC     coverage_key    BIGINT,
# MAGIC     coverage_type   STRING,
# MAGIC     created_at      TIMESTAMP,
# MAGIC     updated_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_quotation
# MAGIC -- Source Table: silver.quotation (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_quotation;
# MAGIC CREATE TABLE gold.dim_quotation (
# MAGIC     quotation_key        BIGINT,
# MAGIC     quotation_id         STRING,
# MAGIC     quotation_expiry_date DATE,
# MAGIC     created_at           TIMESTAMP,
# MAGIC     updated_at           TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_policy
# MAGIC -- Source Table: silver.policy (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_policy;
# MAGIC CREATE TABLE gold.dim_policy (
# MAGIC     policy_key         BIGINT,
# MAGIC     policy_id          STRING,
# MAGIC     created_at         TIMESTAMP,
# MAGIC     updated_at         TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_quotation_status
# MAGIC -- Source Table: silver.quotation (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_quotation_status;
# MAGIC CREATE TABLE gold.dim_quotation_status (
# MAGIC     quotation_status_key  BIGINT,
# MAGIC     quotation_status_code STRING,
# MAGIC     created_at            TIMESTAMP,
# MAGIC     updated_at            TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_policy_status
# MAGIC -- Source Table: silver.policy (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_policy_status;
# MAGIC CREATE TABLE gold.dim_policy_status (
# MAGIC     policy_status_key   BIGINT,
# MAGIC     policy_status_code  STRING,
# MAGIC     created_at          TIMESTAMP,
# MAGIC     updated_at          TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_payment_status
# MAGIC -- Source Table: silver.payment (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_payment_status;
# MAGIC CREATE TABLE gold.dim_payment_status (
# MAGIC     payment_status_key    BIGINT,
# MAGIC     payment_status_code   STRING,
# MAGIC     created_at            TIMESTAMP,
# MAGIC     updated_at            TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_payment_method
# MAGIC -- Source Table: silver.payment (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_payment_method;
# MAGIC CREATE TABLE gold.dim_payment_method (
# MAGIC     payment_method_key   BIGINT,
# MAGIC     payment_method_code  STRING,
# MAGIC     created_at           TIMESTAMP,
# MAGIC     updated_at           TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_cancellation_reason
# MAGIC -- Source Table: silver.cancellation (SCD Type 1)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_cancellation_reason;
# MAGIC CREATE TABLE gold.dim_cancellation_reason (
# MAGIC     cancellation_reason_key BIGINT,
# MAGIC     cancellation_reason     STRING,
# MAGIC     created_at              TIMESTAMP,
# MAGIC     updated_at              TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.dim_vehicle
# MAGIC -- Source Table: silver.vehicle (SCD Type 2)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.dim_vehicle;
# MAGIC CREATE TABLE gold.dim_vehicle (
# MAGIC     vehicle_key       BIGINT,
# MAGIC     vehicle_id        STRING,
# MAGIC     customer_id       STRING,
# MAGIC     plate_number      STRING,
# MAGIC     vehicle_brand     STRING,
# MAGIC     vehicle_model     STRING,
# MAGIC     manufacture_year  INT,
# MAGIC     vehicle_value     DECIMAL(18,2),
# MAGIC     effective_from    TIMESTAMP,
# MAGIC     effective_to      TIMESTAMP,
# MAGIC     is_current        BOOLEAN,
# MAGIC     created_at        TIMESTAMP,
# MAGIC     updated_at        TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- 2. FACT TABLES
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.fact_quotation
# MAGIC -- Source Table: silver.quotation
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.fact_quotation;
# MAGIC CREATE TABLE gold.fact_quotation (
# MAGIC     -- Degenerate Dimensions / Business Keys
# MAGIC     quotation_id              STRING,
# MAGIC     customer_id               STRING,
# MAGIC     agent_id                  STRING,
# MAGIC     provider_code             STRING,
# MAGIC     
# MAGIC     -- Dimension Foreign Keys
# MAGIC     quotation_key             BIGINT,
# MAGIC     customer_key              BIGINT,
# MAGIC     agent_key                 BIGINT,
# MAGIC     provider_key              BIGINT,
# MAGIC     package_key               BIGINT,
# MAGIC     quotation_status_key      BIGINT,
# MAGIC     quotation_date_key        INT,
# MAGIC     quotation_expiry_date_key INT,
# MAGIC     vehicle_key               BIGINT, -- Resolved via customer_id context
# MAGIC     
# MAGIC     -- Measures
# MAGIC     premium_amount            DECIMAL(18,2),
# MAGIC     converted_flag            BOOLEAN,
# MAGIC     
# MAGIC     -- Metadata / Audit columns
# MAGIC     created_at                TIMESTAMP,
# MAGIC     updated_at                TIMESTAMP,
# MAGIC     _batch_id                 STRING,
# MAGIC     _source_system            STRING,
# MAGIC     pipeline_run_id           STRING,
# MAGIC     is_deleted                BOOLEAN,
# MAGIC     deleted_at                TIMESTAMP,
# MAGIC     delete_batch_id           STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.fact_quotation_item
# MAGIC -- Source Tables: silver.quotation_item (primary), silver.quotation (header)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.fact_quotation_item;
# MAGIC CREATE TABLE gold.fact_quotation_item (
# MAGIC     -- Degenerate Dimensions / Business Keys
# MAGIC     quotation_item_id         STRING,
# MAGIC     quotation_id              STRING,
# MAGIC     
# MAGIC     -- Dimension Foreign Keys
# MAGIC     quotation_key             BIGINT,
# MAGIC     quotation_date_key        INT,
# MAGIC     customer_key              BIGINT,
# MAGIC     agent_key                 BIGINT,
# MAGIC     provider_key              BIGINT,
# MAGIC     package_key               BIGINT,
# MAGIC     quotation_status_key      BIGINT,
# MAGIC     coverage_key              BIGINT,
# MAGIC     vehicle_key               BIGINT, -- Resolved via customer_id context
# MAGIC     
# MAGIC     -- Measures
# MAGIC     coverage_amount           DECIMAL(18,2),
# MAGIC     deductible_amount         DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Metadata / Audit columns
# MAGIC     created_at                TIMESTAMP,
# MAGIC     updated_at                TIMESTAMP,
# MAGIC     _batch_id                 STRING,
# MAGIC     _source_system            STRING,
# MAGIC     pipeline_run_id           STRING,
# MAGIC     is_deleted                BOOLEAN,
# MAGIC     deleted_at                TIMESTAMP,
# MAGIC     delete_batch_id           STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.fact_policy
# MAGIC -- Source Tables: silver.policy (primary), silver.quotation (context)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.fact_policy;
# MAGIC CREATE TABLE gold.fact_policy (
# MAGIC     -- Degenerate Dimensions / Business Keys
# MAGIC     policy_id                 STRING,
# MAGIC     policy_number             STRING,
# MAGIC     quotation_id              STRING,
# MAGIC     customer_id               STRING,
# MAGIC     provider_code             STRING,
# MAGIC     
# MAGIC     -- Dimension Foreign Keys
# MAGIC     policy_key                BIGINT,
# MAGIC     quotation_key             BIGINT,
# MAGIC     customer_key              BIGINT,
# MAGIC     provider_key              BIGINT,
# MAGIC     agent_key                 BIGINT,
# MAGIC     package_key               BIGINT,
# MAGIC     policy_status_key         BIGINT,
# MAGIC     issued_date_key           INT,
# MAGIC     policy_start_date_key     INT,
# MAGIC     policy_end_date_key       INT,
# MAGIC     vehicle_key               BIGINT, -- Resolved via customer_id context
# MAGIC     
# MAGIC     -- Measures
# MAGIC     premium_amount            DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Metadata / Audit columns
# MAGIC     created_at                TIMESTAMP,
# MAGIC     updated_at                TIMESTAMP,
# MAGIC     _batch_id                 STRING,
# MAGIC     _source_system            STRING,
# MAGIC     pipeline_run_id           STRING,
# MAGIC     is_deleted                BOOLEAN,
# MAGIC     deleted_at                TIMESTAMP,
# MAGIC     delete_batch_id           STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.fact_payment
# MAGIC -- Source Tables: silver.payment (primary), silver.policy (context)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.fact_payment;
# MAGIC CREATE TABLE gold.fact_payment (
# MAGIC     -- Degenerate Dimensions / Business Keys
# MAGIC     payment_id                STRING,
# MAGIC     policy_id                 STRING,
# MAGIC     transaction_reference     STRING,
# MAGIC     
# MAGIC     -- Dimension Foreign Keys
# MAGIC     policy_key                BIGINT,
# MAGIC     payment_status_key        BIGINT,
# MAGIC     payment_method_key        BIGINT,
# MAGIC     payment_date_key          INT,
# MAGIC     issued_date_key           INT,
# MAGIC     customer_key              BIGINT,
# MAGIC     provider_key              BIGINT,
# MAGIC     vehicle_key               BIGINT, -- Resolved via customer_id context
# MAGIC     
# MAGIC     -- Measures
# MAGIC     payment_amount            DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Metadata / Audit columns
# MAGIC     created_at                TIMESTAMP,
# MAGIC     updated_at                TIMESTAMP,
# MAGIC     _batch_id                 STRING,
# MAGIC     _source_system            STRING,
# MAGIC     pipeline_run_id           STRING,
# MAGIC     is_deleted                BOOLEAN,
# MAGIC     deleted_at                TIMESTAMP,
# MAGIC     delete_batch_id           STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- TABLE: gold.fact_cancellation
# MAGIC -- Source Tables: silver.cancellation (primary), silver.policy (context)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC DROP TABLE IF EXISTS gold.fact_cancellation;
# MAGIC CREATE TABLE gold.fact_cancellation (
# MAGIC     -- Degenerate Dimensions / Business Keys
# MAGIC     cancellation_id           STRING,
# MAGIC     policy_id                 STRING,
# MAGIC     
# MAGIC     -- Dimension Foreign Keys
# MAGIC     policy_key                BIGINT,
# MAGIC     cancellation_reason_key   BIGINT,
# MAGIC     cancellation_date_key     INT,
# MAGIC     customer_key              BIGINT,
# MAGIC     provider_key              BIGINT,
# MAGIC     vehicle_key               BIGINT, -- Resolved via customer_id context
# MAGIC     
# MAGIC     -- Measures
# MAGIC     refund_amount             DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Metadata / Audit columns
# MAGIC     created_at                TIMESTAMP,
# MAGIC     updated_at                TIMESTAMP,
# MAGIC     _batch_id                 STRING,
# MAGIC     _source_system            STRING,
# MAGIC     pipeline_run_id           STRING,
# MAGIC     is_deleted                BOOLEAN,
# MAGIC     deleted_at                TIMESTAMP,
# MAGIC     delete_batch_id           STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC TRUNCATE TABLE gold.dim_date;
# MAGIC TRUNCATE TABLE gold.dim_customer;
# MAGIC TRUNCATE TABLE gold.dim_agent;
# MAGIC TRUNCATE TABLE gold.dim_provider;
# MAGIC TRUNCATE TABLE gold.dim_package;
# MAGIC TRUNCATE TABLE gold.dim_coverage;
# MAGIC TRUNCATE TABLE gold.dim_quotation;
# MAGIC TRUNCATE TABLE gold.dim_policy;
# MAGIC TRUNCATE TABLE gold.dim_quotation_status;
# MAGIC TRUNCATE TABLE gold.dim_policy_status;
# MAGIC TRUNCATE TABLE gold.dim_payment_status;
# MAGIC TRUNCATE TABLE gold.dim_payment_method;
# MAGIC TRUNCATE TABLE gold.dim_cancellation_reason;
# MAGIC TRUNCATE TABLE gold.dim_vehicle;
# MAGIC 
# MAGIC TRUNCATE TABLE gold.fact_quotation;
# MAGIC TRUNCATE TABLE gold.fact_quotation_item;
# MAGIC TRUNCATE TABLE gold.fact_policy;
# MAGIC TRUNCATE TABLE gold.fact_payment;
# MAGIC TRUNCATE TABLE gold.fact_cancellation;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
