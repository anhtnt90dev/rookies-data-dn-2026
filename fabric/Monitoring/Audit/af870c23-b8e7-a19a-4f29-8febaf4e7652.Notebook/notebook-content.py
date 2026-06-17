# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "44c157dd-49ca-46c5-896a-9cb48544300e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- notebook_name: nb_audit_pipeline_log_dev
# MAGIC -- purpose: Create audit/logging tables for Fabric ETL pipeline monitoring.
# MAGIC -- note: Run this notebook after attaching a target Lakehouse.
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS log;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_session (
# MAGIC     id STRING,
# MAGIC     session_status STRING,
# MAGIC     run_mode STRING,
# MAGIC     batch_id BIGINT,
# MAGIC     pipeline_name STRING,
# MAGIC     pipeline_run_id STRING,
# MAGIC     session_started TIMESTAMP,
# MAGIC     session_finished TIMESTAMP,
# MAGIC     duration_ms BIGINT,
# MAGIC     sla_target_ms BIGINT,
# MAGIC     sla_breached BOOLEAN,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_table_session (
# MAGIC     id STRING,
# MAGIC     session_id STRING,
# MAGIC     source_table_id BIGINT,
# MAGIC     batch_id BIGINT,
# MAGIC     table_session_status STRING,
# MAGIC     bronze_status STRING,
# MAGIC     silver_status STRING,
# MAGIC     gold_status STRING,
# MAGIC     load_type STRING,
# MAGIC     watermark_column STRING,
# MAGIC     watermark_before STRING,
# MAGIC     watermark_after STRING,
# MAGIC     load_window_start TIMESTAMP,
# MAGIC     load_window_end TIMESTAMP,
# MAGIC     bronze_started_at TIMESTAMP,
# MAGIC     silver_started_at TIMESTAMP,
# MAGIC     gold_started_at TIMESTAMP,
# MAGIC     bronze_ended_at TIMESTAMP,
# MAGIC     silver_ended_at TIMESTAMP,
# MAGIC     gold_ended_at TIMESTAMP,
# MAGIC     error_code STRING,
# MAGIC     error_message STRING,
# MAGIC     retry_count INT,
# MAGIC     last_retry_at TIMESTAMP,
# MAGIC     duration_ms BIGINT,
# MAGIC     sla_target_ms BIGINT,
# MAGIC     sla_breached BOOLEAN,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     source_table_name STRING
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_detail (
# MAGIC     id STRING,
# MAGIC     table_session_id STRING,
# MAGIC     attempt_no INT,
# MAGIC     detail_status STRING,
# MAGIC     layer STRING,
# MAGIC     watermark_before STRING,
# MAGIC     watermark_after STRING,
# MAGIC     load_window_start TIMESTAMP,
# MAGIC     load_window_end TIMESTAMP,
# MAGIC     source_row_count INT,
# MAGIC     target_row_count INT,
# MAGIC     inserted_row INT,
# MAGIC     updated_row INT,
# MAGIC     deleted_row INT,
# MAGIC     rejected_row INT,
# MAGIC     error_message STRING,
# MAGIC     error_type STRING,
# MAGIC     is_retryable BOOLEAN,
# MAGIC     duration_ms BIGINT,
# MAGIC     sla_target_ms BIGINT,
# MAGIC     sla_breached BOOLEAN,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Migration for existing dev tables created before target_row_count was added.
try:
    existing_columns = [field.name for field in spark.table("log.audit_detail").schema.fields]
    if "target_row_count" not in existing_columns:
        spark.sql("ALTER TABLE log.audit_detail ADD COLUMNS (target_row_count INT)")
        print("Added log.audit_detail.target_row_count")
    else:
        print("log.audit_detail.target_row_count already exists")
except Exception as error:
    print(f"Skipped target_row_count migration check: {error}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS log.retry_log (
# MAGIC     id STRING,
# MAGIC     file_session_id STRING,
# MAGIC     table_session_id STRING,
# MAGIC     attempt_no INT,
# MAGIC     layer STRING,
# MAGIC     status STRING,
# MAGIC     error_code STRING,
# MAGIC     error_message STRING,
# MAGIC     error_type STRING,
# MAGIC     is_retryable BOOLEAN,
# MAGIC     started_at TIMESTAMP,
# MAGIC     ended_at TIMESTAMP,
# MAGIC     duration_ms BIGINT,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS log.invalid_record (
# MAGIC     id STRING,
# MAGIC     file_session_id STRING,
# MAGIC     table_session_id STRING,
# MAGIC     layer STRING,
# MAGIC     target_table STRING,
# MAGIC     record_key STRING,
# MAGIC     raw_data STRING,
# MAGIC     error_column STRING,
# MAGIC     error_reason STRING,
# MAGIC     error_type STRING,
# MAGIC     is_retryable BOOLEAN,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_file_session (
# MAGIC     id STRING,
# MAGIC     session_id STRING,
# MAGIC     table_session_id STRING,
# MAGIC     source_table_id BIGINT,
# MAGIC     batch_id BIGINT,
# MAGIC     source_file STRING,
# MAGIC     file_status STRING,
# MAGIC     file_row_count INT,
# MAGIC     processed_row_count INT,
# MAGIC     rejected_row_count INT,
# MAGIC     error_code STRING,
# MAGIC     error_message STRING,
# MAGIC     error_type STRING,
# MAGIC     is_retryable BOOLEAN,
# MAGIC     retry_count INT,
# MAGIC     last_retry_at TIMESTAMP,
# MAGIC     started_at TIMESTAMP,
# MAGIC     completed_at TIMESTAMP,
# MAGIC     duration_ms BIGINT,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC TRUNCATE TABLE log.audit_session;
# MAGIC TRUNCATE TABLE log.audit_table_session;
# MAGIC TRUNCATE TABLE log.audit_detail;
# MAGIC TRUNCATE TABLE log.audit_file_session;
# MAGIC TRUNCATE TABLE log.retry_log;
# MAGIC TRUNCATE TABLE log.invalid_record;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
