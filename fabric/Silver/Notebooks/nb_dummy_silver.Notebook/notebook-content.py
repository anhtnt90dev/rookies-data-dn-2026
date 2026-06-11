# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c8fc794-e72d-4c37-8b73-1adf7e8c1529",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "a562f741-0da9-4508-be62-0c9caf763e5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "2c8fc794-e72d-4c37-8b73-1adf7e8c1529"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.customer (
# MAGIC     customer_id STRING,
# MAGIC     full_name STRING,
# MAGIC     gender STRING,
# MAGIC     dob DATE,
# MAGIC     phone_number STRING,
# MAGIC     email STRING,
# MAGIC     city STRING,
# MAGIC     district STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.agent (
# MAGIC     agent_id STRING,
# MAGIC     agent_name STRING,
# MAGIC     region STRING,
# MAGIC     branch STRING,
# MAGIC     manager_name STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.provider (
# MAGIC     provider_code STRING,
# MAGIC     provider_name STRING,
# MAGIC     provider_group STRING,
# MAGIC     is_active BOOLEAN,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.vehicle (
# MAGIC     vehicle_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     plate_number STRING,
# MAGIC     vehicle_brand STRING,
# MAGIC     vehicle_model STRING,
# MAGIC     manufacture_year INT,
# MAGIC     vehicle_value DECIMAL(18,2),
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.quotation (
# MAGIC     quotation_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     agent_id STRING,
# MAGIC     provider_code STRING,
# MAGIC     quotation_at TIMESTAMP,
# MAGIC     quotation_status STRING,
# MAGIC     package_code STRING,
# MAGIC     premium_amount DECIMAL(18,2),
# MAGIC     quotation_expiry_at TIMESTAMP,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.quotation_item (
# MAGIC     quotation_item_id STRING,
# MAGIC     quotation_id STRING,
# MAGIC     coverage_type STRING,
# MAGIC     coverage_amount DECIMAL(18,2),
# MAGIC     deductible_amount DECIMAL(18,2),
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.policy (
# MAGIC     policy_id STRING,
# MAGIC     quotation_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     provider_code STRING,
# MAGIC     policy_number STRING,
# MAGIC     policy_start_date DATE,
# MAGIC     policy_end_date DATE,
# MAGIC     policy_status STRING,
# MAGIC     premium_amount DECIMAL(18,2),
# MAGIC     operation_type STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     issued_at TIMESTAMP,
# MAGIC     last_updated_at TIMESTAMP,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.payment (
# MAGIC     payment_id STRING,
# MAGIC     policy_id STRING,
# MAGIC     payment_at TIMESTAMP,
# MAGIC     payment_method STRING,
# MAGIC     payment_status STRING,
# MAGIC     payment_amount DECIMAL(18,2),
# MAGIC     transaction_reference STRING,
# MAGIC     last_updated_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     operation_type STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.cancellation (
# MAGIC     cancellation_id STRING,
# MAGIC     policy_id STRING,
# MAGIC     cancellation_at TIMESTAMP,
# MAGIC     cancellation_reason STRING,
# MAGIC     refund_amount DECIMAL(18,2),
# MAGIC     last_updated_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     operation_type STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     _batch_id STRING,
# MAGIC     _loaded_at TIMESTAMP,
# MAGIC     _source_system STRING,
# MAGIC     _source_name STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM silver.cancellation
# MAGIC WHERE cancellation_id LIKE 'SCD_TEST_%'
# MAGIC    OR policy_id LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.payment
# MAGIC WHERE payment_id LIKE 'SCD_TEST_%'
# MAGIC    OR policy_id LIKE 'SCD_TEST_%'
# MAGIC    OR transaction_reference LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.policy
# MAGIC WHERE policy_id LIKE 'SCD_TEST_%'
# MAGIC    OR quotation_id LIKE 'SCD_TEST_%'
# MAGIC    OR customer_id LIKE 'SCD_TEST_%'
# MAGIC    OR provider_code LIKE 'SCD_TEST_%'
# MAGIC    OR policy_number LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.quotation_item
# MAGIC WHERE quotation_item_id LIKE 'SCD_TEST_%'
# MAGIC    OR quotation_id LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.quotation
# MAGIC WHERE quotation_id LIKE 'SCD_TEST_%'
# MAGIC    OR customer_id LIKE 'SCD_TEST_%'
# MAGIC    OR agent_id LIKE 'SCD_TEST_%'
# MAGIC    OR provider_code LIKE 'SCD_TEST_%'
# MAGIC    OR package_code LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.vehicle
# MAGIC WHERE vehicle_id LIKE 'SCD_TEST_%'
# MAGIC    OR customer_id LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.provider
# MAGIC WHERE provider_code LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.agent
# MAGIC WHERE agent_id LIKE 'SCD_TEST_%';
# MAGIC 
# MAGIC DELETE FROM silver.customer
# MAGIC WHERE customer_id LIKE 'SCD_TEST_%';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO silver.customer (
# MAGIC     customer_id, full_name, gender, dob, phone_number, email,
# MAGIC     city, district, created_at, updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_CUST_001', 'Anong Test Customer', 'F', DATE '1990-03-15',
# MAGIC     '0810000001', 'scd.customer.001@example.test',
# MAGIC     'Bangkok', 'Pathum Wan',
# MAGIC     TIMESTAMP '2026-01-01 08:00:00',
# MAGIC     TIMESTAMP '2026-01-01 08:00:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.agent (
# MAGIC     agent_id, agent_name, region, branch, manager_name,
# MAGIC     created_at, updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_AGENT_001', 'Somchai Test Agent', 'Central', 'Bangkok Main', 'Manager A',
# MAGIC     TIMESTAMP '2026-01-01 08:05:00',
# MAGIC     TIMESTAMP '2026-01-01 08:05:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.provider (
# MAGIC     provider_code, provider_name, provider_group, is_active,
# MAGIC     created_at, updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_PROVIDER_001', 'SCD Test Insurance Co', 'Group Alpha', true,
# MAGIC     TIMESTAMP '2026-01-01 08:10:00',
# MAGIC     TIMESTAMP '2026-01-01 08:10:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.vehicle (
# MAGIC     vehicle_id, customer_id, plate_number, vehicle_brand, vehicle_model,
# MAGIC     manufacture_year, vehicle_value, created_at, updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_VEH_001', 'SCD_TEST_CUST_001', 'TEST-1001', 'Toyota', 'Corolla Cross',
# MAGIC     2022, CAST(850000.00 AS DECIMAL(18,2)),
# MAGIC     TIMESTAMP '2026-01-01 08:15:00',
# MAGIC     TIMESTAMP '2026-01-01 08:15:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.quotation (
# MAGIC     quotation_id, customer_id, agent_id, provider_code,
# MAGIC     quotation_at, quotation_status, package_code, premium_amount,
# MAGIC     quotation_expiry_at, created_at, updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_QUOTE_001', 'SCD_TEST_CUST_001', 'SCD_TEST_AGENT_001', 'SCD_TEST_PROVIDER_001',
# MAGIC     TIMESTAMP '2026-01-02 09:00:00', 'QUOTED', 'SCD_TEST_PACKAGE_BASIC',
# MAGIC     CAST(12500.00 AS DECIMAL(18,2)),
# MAGIC     TIMESTAMP '2026-02-01 23:59:59',
# MAGIC     TIMESTAMP '2026-01-02 09:00:00',
# MAGIC     TIMESTAMP '2026-01-02 09:00:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.quotation_item (
# MAGIC     quotation_item_id, quotation_id, coverage_type, coverage_amount,
# MAGIC     deductible_amount, created_at, updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_QITEM_001', 'SCD_TEST_QUOTE_001', 'SCD_TEST_COLLISION',
# MAGIC     CAST(500000.00 AS DECIMAL(18,2)),
# MAGIC     CAST(5000.00 AS DECIMAL(18,2)),
# MAGIC     TIMESTAMP '2026-01-02 09:05:00',
# MAGIC     TIMESTAMP '2026-01-02 09:05:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.policy (
# MAGIC     policy_id, quotation_id, customer_id, provider_code, policy_number,
# MAGIC     policy_start_date, policy_end_date, policy_status, premium_amount,
# MAGIC     operation_type, is_deleted, issued_at, last_updated_at,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_POLICY_001', 'SCD_TEST_QUOTE_001', 'SCD_TEST_CUST_001',
# MAGIC     'SCD_TEST_PROVIDER_001', 'SCD_TEST_POLICY_NO_001',
# MAGIC     DATE '2026-01-10', DATE '2027-01-09', 'ACTIVE',
# MAGIC     CAST(12500.00 AS DECIMAL(18,2)),
# MAGIC     'I', false,
# MAGIC     TIMESTAMP '2026-01-10 10:00:00',
# MAGIC     TIMESTAMP '2026-01-10 10:00:00',
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.payment (
# MAGIC     payment_id, policy_id, payment_at, payment_method, payment_status,
# MAGIC     payment_amount, transaction_reference, last_updated_at, updated_at,
# MAGIC     operation_type, is_deleted,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_PAYMENT_001', 'SCD_TEST_POLICY_001',
# MAGIC     TIMESTAMP '2026-01-10 10:30:00', 'Credit Card', 'PAID',
# MAGIC     CAST(12500.00 AS DECIMAL(18,2)),
# MAGIC     'SCD_TEST_TXN_001',
# MAGIC     TIMESTAMP '2026-01-10 10:30:00',
# MAGIC     TIMESTAMP '2026-01-10 10:30:00',
# MAGIC     'I', false,
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO silver.cancellation (
# MAGIC     cancellation_id, policy_id, cancellation_at, cancellation_reason,
# MAGIC     refund_amount, last_updated_at, updated_at, operation_type, is_deleted,
# MAGIC     _batch_id, _loaded_at, _source_system, _source_name
# MAGIC )
# MAGIC VALUES (
# MAGIC     'SCD_TEST_CANCEL_001', 'SCD_TEST_POLICY_001',
# MAGIC     TIMESTAMP '2026-06-01 12:00:00', 'SCD_TEST_CUSTOMER_REQUEST',
# MAGIC     CAST(2500.00 AS DECIMAL(18,2)),
# MAGIC     TIMESTAMP '2026-06-01 12:00:00',
# MAGIC     TIMESTAMP '2026-06-01 12:00:00',
# MAGIC     'I', false,
# MAGIC     'SCD_TEST_BATCH_001', CURRENT_TIMESTAMP(), 'SCD_TEST', 'dummy_scd_seed'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM silver.customer WHERE customer_id LIKE 'SCD_TEST_%';
# MAGIC SELECT COUNT(*) FROM silver.quotation WHERE quotation_id LIKE 'SCD_TEST_%';
# MAGIC SELECT COUNT(*) FROM silver.policy WHERE policy_id LIKE 'SCD_TEST_%';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
