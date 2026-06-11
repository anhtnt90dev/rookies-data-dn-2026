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
# MAGIC UPDATE silver.quotation
# MAGIC SET quotation_expiry_at = TIMESTAMP '2026-03-15 23:59:59',
# MAGIC     updated_at = TIMESTAMP '2026-01-15 09:00:00',
# MAGIC     _batch_id = 'SCD_TEST_BATCH_002',
# MAGIC     _loaded_at = CURRENT_TIMESTAMP()
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001';
# MAGIC 
# MAGIC UPDATE silver.customer
# MAGIC SET city = 'Nonthaburi',
# MAGIC     district = 'Mueang Nonthaburi',
# MAGIC     updated_at = TIMESTAMP '2026-01-15 09:10:00',
# MAGIC     _batch_id = 'SCD_TEST_BATCH_002',
# MAGIC     _loaded_at = CURRENT_TIMESTAMP()
# MAGIC WHERE customer_id = 'SCD_TEST_CUST_001';
# MAGIC 
# MAGIC UPDATE silver.agent
# MAGIC SET region = 'East',
# MAGIC     branch = 'Chonburi Branch',
# MAGIC     manager_name = 'Manager B',
# MAGIC     updated_at = TIMESTAMP '2026-01-15 09:20:00',
# MAGIC     _batch_id = 'SCD_TEST_BATCH_002',
# MAGIC     _loaded_at = CURRENT_TIMESTAMP()
# MAGIC WHERE agent_id = 'SCD_TEST_AGENT_001';
# MAGIC 
# MAGIC UPDATE silver.provider
# MAGIC SET provider_group = 'Group Beta',
# MAGIC     is_active = false,
# MAGIC     updated_at = TIMESTAMP '2026-01-15 09:30:00',
# MAGIC     _batch_id = 'SCD_TEST_BATCH_002',
# MAGIC     _loaded_at = CURRENT_TIMESTAMP()
# MAGIC WHERE provider_code = 'SCD_TEST_PROVIDER_001';
# MAGIC 
# MAGIC UPDATE silver.vehicle
# MAGIC SET vehicle_value = CAST(790000.00 AS DECIMAL(18,2)),
# MAGIC     updated_at = TIMESTAMP '2026-01-15 09:40:00',
# MAGIC     _batch_id = 'SCD_TEST_BATCH_002',
# MAGIC     _loaded_at = CURRENT_TIMESTAMP()
# MAGIC WHERE vehicle_id = 'SCD_TEST_VEH_001';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- SCD1: dim_quotation should keep one row and overwrite the attribute.
# MAGIC SELECT quotation_id, COUNT(*) AS row_count
# MAGIC FROM gold.dim_quotation
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001'
# MAGIC GROUP BY quotation_id;
# MAGIC 
# MAGIC SELECT quotation_id, quotation_expiry_date
# MAGIC FROM gold.dim_quotation
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001';
# MAGIC 
# MAGIC -- SCD1 duplicate checks for test business keys.
# MAGIC SELECT package_code, COUNT(*) AS row_count
# MAGIC FROM gold.dim_package
# MAGIC WHERE package_code LIKE 'SCD_TEST_%'
# MAGIC GROUP BY package_code
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT coverage_type, COUNT(*) AS row_count
# MAGIC FROM gold.dim_coverage
# MAGIC WHERE coverage_type LIKE 'SCD_TEST_%'
# MAGIC GROUP BY coverage_type
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT policy_id, COUNT(*) AS row_count
# MAGIC FROM gold.dim_policy
# MAGIC WHERE policy_id LIKE 'SCD_TEST_%'
# MAGIC GROUP BY policy_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT quotation_status_code, COUNT(*) AS row_count
# MAGIC FROM gold.dim_quotation_status
# MAGIC WHERE quotation_status_code LIKE 'SCD_TEST_%'
# MAGIC GROUP BY quotation_status_code
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT policy_status_code, COUNT(*) AS row_count
# MAGIC FROM gold.dim_policy_status
# MAGIC WHERE policy_status_code LIKE 'SCD_TEST_%'
# MAGIC GROUP BY policy_status_code
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT payment_status_code, COUNT(*) AS row_count
# MAGIC FROM gold.dim_payment_status
# MAGIC WHERE payment_status_code LIKE 'SCD_TEST_%'
# MAGIC GROUP BY payment_status_code
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT payment_method_code, COUNT(*) AS row_count
# MAGIC FROM gold.dim_payment_method
# MAGIC WHERE payment_method_code LIKE 'SCD_TEST_%'
# MAGIC GROUP BY payment_method_code
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT cancellation_reason, COUNT(*) AS row_count
# MAGIC FROM gold.dim_cancellation_reason
# MAGIC WHERE cancellation_reason LIKE 'SCD_TEST_%'
# MAGIC GROUP BY cancellation_reason
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC -- SCD2 summary checks.
# MAGIC SELECT customer_id, COUNT(*) AS versions, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_id = 'SCD_TEST_CUST_001'
# MAGIC GROUP BY customer_id;
# MAGIC 
# MAGIC SELECT agent_id, COUNT(*) AS versions, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM gold.dim_agent
# MAGIC WHERE agent_id = 'SCD_TEST_AGENT_001'
# MAGIC GROUP BY agent_id;
# MAGIC 
# MAGIC SELECT provider_code, COUNT(*) AS versions, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM gold.dim_provider
# MAGIC WHERE provider_code = 'SCD_TEST_PROVIDER_001'
# MAGIC GROUP BY provider_code;
# MAGIC 
# MAGIC SELECT vehicle_id, COUNT(*) AS versions, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM gold.dim_vehicle
# MAGIC WHERE vehicle_id = 'SCD_TEST_VEH_001'
# MAGIC GROUP BY vehicle_id;
# MAGIC 
# MAGIC -- SCD2 current rows should show updated tracked attributes.
# MAGIC SELECT customer_id, city, district, effective_from, effective_to, is_current
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_id = 'SCD_TEST_CUST_001'
# MAGIC ORDER BY effective_from;
# MAGIC 
# MAGIC SELECT agent_id, region, branch, manager_name, effective_from, effective_to, is_current
# MAGIC FROM gold.dim_agent
# MAGIC WHERE agent_id = 'SCD_TEST_AGENT_001'
# MAGIC ORDER BY effective_from;
# MAGIC 
# MAGIC SELECT provider_code, provider_group, active_flag, effective_from, effective_to, is_current
# MAGIC FROM gold.dim_provider
# MAGIC WHERE provider_code = 'SCD_TEST_PROVIDER_001'
# MAGIC ORDER BY effective_from;
# MAGIC 
# MAGIC SELECT vehicle_id, vehicle_value, effective_from, effective_to, is_current
# MAGIC FROM gold.dim_vehicle
# MAGIC WHERE vehicle_id = 'SCD_TEST_VEH_001'
# MAGIC ORDER BY effective_from;
# MAGIC 
# MAGIC -- Old SCD2 rows should be closed.
# MAGIC SELECT *
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_id = 'SCD_TEST_CUST_001'
# MAGIC   AND is_current = false
# MAGIC   AND effective_to IS NOT NULL;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold.dim_agent
# MAGIC WHERE agent_id = 'SCD_TEST_AGENT_001'
# MAGIC   AND is_current = false
# MAGIC   AND effective_to IS NOT NULL;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold.dim_provider
# MAGIC WHERE provider_code = 'SCD_TEST_PROVIDER_001'
# MAGIC   AND is_current = false
# MAGIC   AND effective_to IS NOT NULL;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold.dim_vehicle
# MAGIC WHERE vehicle_id = 'SCD_TEST_VEH_001'
# MAGIC   AND is_current = false
# MAGIC   AND effective_to IS NOT NULL;
# MAGIC 
# MAGIC -- Unknown member checks.
# MAGIC SELECT 'dim_customer' AS dimension_name, COUNT(*) AS unknown_count FROM gold.dim_customer WHERE customer_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_agent', COUNT(*) FROM gold.dim_agent WHERE agent_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_provider', COUNT(*) FROM gold.dim_provider WHERE provider_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_vehicle', COUNT(*) FROM gold.dim_vehicle WHERE vehicle_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_package', COUNT(*) FROM gold.dim_package WHERE package_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_coverage', COUNT(*) FROM gold.dim_coverage WHERE coverage_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_quotation', COUNT(*) FROM gold.dim_quotation WHERE quotation_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_policy', COUNT(*) FROM gold.dim_policy WHERE policy_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_quotation_status', COUNT(*) FROM gold.dim_quotation_status WHERE quotation_status_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_policy_status', COUNT(*) FROM gold.dim_policy_status WHERE policy_status_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_payment_status', COUNT(*) FROM gold.dim_payment_status WHERE payment_status_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_payment_method', COUNT(*) FROM gold.dim_payment_method WHERE payment_method_key = -1
# MAGIC UNION ALL
# MAGIC SELECT 'dim_cancellation_reason', COUNT(*) FROM gold.dim_cancellation_reason WHERE cancellation_reason_key = -1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS fact_policy_test_rows
# MAGIC FROM gold.fact_policy
# MAGIC WHERE policy_id LIKE 'SCD_TEST_%';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT policy_id, COUNT(*) AS row_count
# MAGIC FROM gold.fact_policy
# MAGIC WHERE policy_id LIKE 'SCD_TEST_%'
# MAGIC GROUP BY policy_id
# MAGIC HAVING COUNT(*) > 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM gold.fact_policy
# MAGIC WHERE policy_id LIKE 'SCD_TEST_%'
# MAGIC   AND (
# MAGIC     customer_key = -1
# MAGIC     OR provider_key = -1
# MAGIC     OR agent_key = -1
# MAGIC     OR package_key = -1
# MAGIC     OR vehicle_key = -1
# MAGIC     OR quotation_key = -1
# MAGIC     OR policy_status_key = -1
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS total_fact_policy_rows
# MAGIC FROM gold.fact_policy;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE gold.fact_policy;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM gold.fact_policy
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver.policy
# MAGIC WHERE policy_id = 'SCD_TEST_POLICY_001';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver.quotation
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT policy_key, policy_id
# MAGIC FROM gold.dim_policy
# MAGIC WHERE policy_id = 'SCD_TEST_POLICY_001';
# MAGIC 
# MAGIC SELECT quotation_key, quotation_id
# MAGIC FROM gold.dim_quotation
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001';
# MAGIC 
# MAGIC SELECT customer_key, customer_id, is_current
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_id = 'SCD_TEST_CUST_001';
# MAGIC 
# MAGIC SELECT provider_key, provider_code, is_current
# MAGIC FROM gold.dim_provider
# MAGIC WHERE provider_code = 'SCD_TEST_PROVIDER_001';
# MAGIC 
# MAGIC SELECT agent_key, agent_id, is_current
# MAGIC FROM gold.dim_agent
# MAGIC WHERE agent_id = 'SCD_TEST_AGENT_001';
# MAGIC 
# MAGIC SELECT package_key, package_code
# MAGIC FROM gold.dim_package
# MAGIC WHERE package_code = 'SCD_TEST_PACKAGE_BASIC';
# MAGIC 
# MAGIC SELECT vehicle_key, vehicle_id, is_current
# MAGIC FROM gold.dim_vehicle
# MAGIC WHERE vehicle_id = 'SCD_TEST_VEH_001';
# MAGIC 
# MAGIC SELECT policy_status_key, policy_status_code
# MAGIC FROM gold.dim_policy_status
# MAGIC WHERE policy_status_code = 'ACTIVE';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE gold.dim_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM gold.dim_date
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM gold.dim_date
# MAGIC WHERE full_date IN (
# MAGIC   DATE '2026-01-10',
# MAGIC   DATE '2027-01-09'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS silver_policy_rows
# MAGIC FROM silver.policy
# MAGIC WHERE policy_id = 'SCD_TEST_POLICY_001';
# MAGIC 
# MAGIC SELECT COUNT(*) AS silver_quotation_rows
# MAGIC FROM silver.quotation
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001';
# MAGIC 
# MAGIC SELECT COUNT(*) AS dim_policy_rows
# MAGIC FROM gold.dim_policy
# MAGIC WHERE policy_id = 'SCD_TEST_POLICY_001';
# MAGIC 
# MAGIC SELECT COUNT(*) AS dim_quotation_rows
# MAGIC FROM gold.dim_quotation
# MAGIC WHERE quotation_id = 'SCD_TEST_QUOTE_001';
# MAGIC 
# MAGIC SELECT COUNT(*) AS current_customer_rows
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_id = 'SCD_TEST_CUST_001'
# MAGIC   AND is_current = true;
# MAGIC 
# MAGIC SELECT COUNT(*) AS current_provider_rows
# MAGIC FROM gold.dim_provider
# MAGIC WHERE provider_code = 'SCD_TEST_PROVIDER_001'
# MAGIC   AND is_current = true;
# MAGIC 
# MAGIC SELECT COUNT(*) AS current_agent_rows
# MAGIC FROM gold.dim_agent
# MAGIC WHERE agent_id = 'SCD_TEST_AGENT_001'
# MAGIC   AND is_current = true;
# MAGIC 
# MAGIC SELECT COUNT(*) AS current_vehicle_rows
# MAGIC FROM gold.dim_vehicle
# MAGIC WHERE vehicle_id = 'SCD_TEST_VEH_001'
# MAGIC   AND is_current = true;
# MAGIC 
# MAGIC SELECT COUNT(*) AS package_rows
# MAGIC FROM gold.dim_package
# MAGIC WHERE package_code = 'SCD_TEST_PACKAGE_BASIC';
# MAGIC 
# MAGIC SELECT COUNT(*) AS policy_status_rows
# MAGIC FROM gold.dim_policy_status
# MAGIC WHERE policy_status_code = 'ACTIVE';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
