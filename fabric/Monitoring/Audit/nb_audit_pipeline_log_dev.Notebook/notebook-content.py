# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC # Welcome to your new notebook
# MAGIC # Type here in the cell editor to add code!
# MAGIC %%sql
# MAGIC -- notebook_name: nb_audit_pipeline_log_dev
# MAGIC -- file_name: 010_create_audit_tables.sql
# MAGIC -- purpose: Create audit/logging tables for Fabric ETL pipeline monitoring.
# MAGIC -- note: Run this notebook after attaching a target Lakehouse.
# MAGIC -- note: This script only creates the table structure. Insert/update logging logic will be implemented in later tasks.
# MAGIC 
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS log;
# MAGIC -- ============================================================
# MAGIC -- Table: log.audit_session
# MAGIC -- Purpose:
# MAGIC --   Stores one row for each overall pipeline execution.
# MAGIC --   This is the pipeline-level summary log
# MAGIC --
# MAGIC -- Example:
# MAGIC --   One daily ETL pipeline run = one audit_session row.
# MAGIC --
# MAGIC -- Applied improvements:
# MAGIC --   Suggestion 1: audit_key for idempotency
# MAGIC --   Suggestion 7: duration_ms, sla_target_ms, sla_breached
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_session (
# MAGIC     id BIGINT,
# MAGIC 
# MAGIC     -- Deterministic key used later to prevent duplicate session logs.
# MAGIC     -- Suggested pattern: pipeline_name + batch_id + pipeline_run_id
# MAGIC     -- Example: pl_daily_carpro_etl_1001_run_abc123
# MAGIC     audit_key STRING,
# MAGIC 
# MAGIC     -- Overall pipeline status.
# MAGIC     -- Suggested values: RUNNING, SUCCESS, FAILED, RECOVERY, CANCELLED
# MAGIC     session_status STRING,
# MAGIC 
# MAGIC     -- Pipeline run mode.
# MAGIC     -- Suggested values: NEW, RECOVERY
# MAGIC     run_mode STRING,
# MAGIC 
# MAGIC     -- Logical batch identifier.
# MAGIC     -- Same batch_id may be reused during recovery.
# MAGIC     batch_id BIGINT,
# MAGIC 
# MAGIC     -- Fabric pipeline metadata.
# MAGIC     pipeline_name STRING,
# MAGIC     pipeline_run_id STRING,
# MAGIC 
# MAGIC     -- Pipeline execution timing.
# MAGIC     session_started TIMESTAMP,
# MAGIC     session_finished TIMESTAMP,
# MAGIC 
# MAGIC     -- Suggestion 7: duration of the full pipeline run in milliseconds.
# MAGIC     duration_ms BIGINT,
# MAGIC 
# MAGIC     -- Suggestion 7: optional SLA target for monitoring.
# MAGIC     -- Example: expected pipeline should finish within 30 minutes.
# MAGIC     sla_target_ms BIGINT,
# MAGIC 
# MAGIC     -- Suggestion 7: TRUE if duration_ms > sla_target_ms.
# MAGIC     sla_breached BOOLEAN,
# MAGIC 
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- Table: log.audit_table_session
# MAGIC -- Purpose:
# MAGIC --   Stores one row for each source table processed in a pipeline session.
# MAGIC --   Tracks the table status across Bronze, Silver, and Gold layers.
# MAGIC --
# MAGIC -- Example:
# MAGIC --   customer table in batch 1001 = one audit_table_session row.
# MAGIC --
# MAGIC -- Applied improvements:
# MAGIC --   Suggestion 1: audit_key
# MAGIC --   Suggestion 6: watermark/load window tracking
# MAGIC --   Suggestion 7: duration_ms, sla_target_ms, sla_breached
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_table_session (
# MAGIC     id BIGINT,
# MAGIC 
# MAGIC     -- Related pipeline-level audit session.
# MAGIC     session_id BIGINT,
# MAGIC 
# MAGIC     -- Deterministic key used later to prevent duplicate table-session logs.
# MAGIC     -- Suggested pattern: session_id + source_table_id
# MAGIC     -- Example: session_10_source_table_5
# MAGIC     audit_key STRING,
# MAGIC 
# MAGIC     -- Related source table configuration.
# MAGIC     source_table_id BIGINT,
# MAGIC 
# MAGIC     -- Optional but useful: keeps batch_id directly in this table for easier recovery queries.
# MAGIC     batch_id BIGINT,
# MAGIC 
# MAGIC     -- Overall status for this table across all layers.
# MAGIC     -- Suggested values: RUNNING, SUCCESS, FAILED, SKIPPED, NOT_RUN
# MAGIC     table_session_status STRING,
# MAGIC 
# MAGIC     -- Layer-level status.
# MAGIC     -- Suggested values: RUNNING, SUCCESS, FAILED, SKIPPED, NOT_RUN
# MAGIC     bronze_status STRING,
# MAGIC     silver_status STRING,
# MAGIC     gold_status STRING,
# MAGIC 
# MAGIC     -- Load strategy used for this table.
# MAGIC     -- Suggested values: FULL, INCREMENTAL
# MAGIC     load_type STRING,
# MAGIC 
# MAGIC     -- Suggestion 6: source watermark column used for incremental load.
# MAGIC     -- Example: updated_at, modified_date, created_at
# MAGIC     watermark_column STRING,
# MAGIC 
# MAGIC     -- Suggestion 6: watermark before the current load starts.
# MAGIC     -- STRING is used to support timestamp, numeric, or text watermark values.
# MAGIC     watermark_before STRING,
# MAGIC 
# MAGIC     -- Suggestion 6: watermark after the current load succeeds.
# MAGIC     watermark_after STRING,
# MAGIC 
# MAGIC     -- Suggestion 6: load window used for incremental extraction.
# MAGIC     -- Example: load records where updated_at > load_window_start and <= load_window_end
# MAGIC     load_window_start TIMESTAMP,
# MAGIC     load_window_end TIMESTAMP,
# MAGIC 
# MAGIC     -- Bronze/Silver/Gold start time.
# MAGIC     bronze_started_at TIMESTAMP,
# MAGIC     silver_started_at TIMESTAMP,
# MAGIC     gold_started_at TIMESTAMP,
# MAGIC 
# MAGIC     -- Bronze/Silver/Gold end time.
# MAGIC     bronze_ended_at TIMESTAMP,
# MAGIC     silver_ended_at TIMESTAMP,
# MAGIC     gold_ended_at TIMESTAMP,
# MAGIC 
# MAGIC     -- Retry summary for this table.
# MAGIC     retry_count INT,
# MAGIC     last_retry_at TIMESTAMP,
# MAGIC 
# MAGIC     -- Suggestion 7: duration of this table processing in milliseconds.
# MAGIC     duration_ms BIGINT,
# MAGIC 
# MAGIC     -- Suggestion 7: optional SLA target for this table.
# MAGIC     sla_target_ms BIGINT,
# MAGIC 
# MAGIC     -- Suggestion 7: TRUE if duration_ms > sla_target_ms.
# MAGIC     sla_breached BOOLEAN,
# MAGIC 
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     source_table_name STRING,
# MAGIC     target_table_name STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- Table: log.audit_detail
# MAGIC -- Purpose:
# MAGIC --   Stores detailed metrics for each table/layer execution.
# MAGIC --   This is where row counts and error details are stored.
# MAGIC --
# MAGIC -- Example:
# MAGIC --   Bronze customer load inserted 1000 rows, rejected 5 rows.
# MAGIC --
# MAGIC -- Applied improvements:
# MAGIC --   Suggestion 1: audit_key, attempt_no
# MAGIC --   Suggestion 3: error_type, is_retryable
# MAGIC --   Suggestion 6: watermark/load window tracking
# MAGIC --   Suggestion 7: duration_ms, sla_target_ms, sla_breached
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS log.audit_detail (
# MAGIC     id BIGINT,
# MAGIC 
# MAGIC     -- Related table-level audit session.
# MAGIC     table_session_id BIGINT,
# MAGIC 
# MAGIC     -- Deterministic key used later to prevent duplicate detail logs.
# MAGIC     -- Suggested pattern: table_session_id + layer + attempt_no
# MAGIC     -- Example: table_session_20_bronze_attempt_1
# MAGIC     audit_key STRING,
# MAGIC 
# MAGIC     -- Attempt number for this layer operation.
# MAGIC     -- First attempt = 1, retry attempt = 2, 3, ...
# MAGIC     attempt_no INT,
# MAGIC 
# MAGIC     -- Detail status for this layer operation.
# MAGIC     -- Suggested values: RUNNING, SUCCESS, FAILED, SKIPPED
# MAGIC     detail_status STRING,
# MAGIC 
# MAGIC     -- Processing layer.
# MAGIC     -- Suggested values: BRONZE, SILVER, GOLD
# MAGIC     layer STRING,
# MAGIC 
# MAGIC     -- Suggestion 6: useful when detail is related to incremental load.
# MAGIC     watermark_before STRING,
# MAGIC     watermark_after STRING,
# MAGIC     load_window_start TIMESTAMP,
# MAGIC     load_window_end TIMESTAMP,
# MAGIC 
# MAGIC     -- Row count metrics.
# MAGIC     source_row_count INT,
# MAGIC     inserted_row INT,
# MAGIC     updated_row INT,
# MAGIC     deleted_row INT,
# MAGIC     rejected_row INT,
# MAGIC 
# MAGIC     -- Error details if the operation fails.
# MAGIC     error_message STRING,
# MAGIC 
# MAGIC     -- Suggestion 3: classify the error.
# MAGIC     -- Suggested values:
# MAGIC     --   SYSTEM = timeout, connection issue, Spark failure
# MAGIC     --   DATA   = invalid format, null key, bad date
# MAGIC     --   RULE   = failed business rule
# MAGIC     --   UNKNOWN = not classified yet
# MAGIC     error_type STRING,
# MAGIC 
# MAGIC     -- Suggestion 3: TRUE if this error can be retried.
# MAGIC     -- Usually TRUE for SYSTEM errors, FALSE for DATA/RULE errors.
# MAGIC     is_retryable BOOLEAN,
# MAGIC 
# MAGIC     -- Suggestion 7: duration of this detail operation in milliseconds.
# MAGIC     duration_ms BIGINT,
# MAGIC 
# MAGIC     -- Suggestion 7: optional SLA target for this operation.
# MAGIC     sla_target_ms BIGINT,
# MAGIC 
# MAGIC     -- Suggestion 7: TRUE if duration_ms > sla_target_ms.
# MAGIC     sla_breached BOOLEAN,
# MAGIC 
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- Table: log.retry_log
# MAGIC -- Purpose:
# MAGIC --   Stores retry execution history for failed system/transient errors.
# MAGIC --
# MAGIC -- Example:
# MAGIC --   Silver policy transform failed because of timeout.
# MAGIC --   Retry attempt 1 failed.
# MAGIC --   Retry attempt 2 succeeded.
# MAGIC --
# MAGIC -- Applied improvements:
# MAGIC --   Suggestion 1: audit_key, attempt_no
# MAGIC --   Suggestion 3: error_type, is_retryable
# MAGIC --   Suggestion 7: duration_ms
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS log.retry_log (
# MAGIC     id BIGINT,
# MAGIC 
# MAGIC     -- Related table-level audit session.
# MAGIC     table_session_id BIGINT,
# MAGIC 
# MAGIC     -- Deterministic key used later to prevent duplicate retry logs.
# MAGIC     -- Suggested pattern: table_session_id + layer + attempt_no
# MAGIC     -- Example: table_session_20_silver_retry_2
# MAGIC     audit_key STRING,
# MAGIC 
# MAGIC     -- Retry attempt number.
# MAGIC     -- First retry = 1, second retry = 2, ...
# MAGIC     attempt_no INT,
# MAGIC 
# MAGIC     -- Layer where retry happened.
# MAGIC     -- Suggested values: BRONZE, SILVER, GOLD
# MAGIC     layer STRING,
# MAGIC 
# MAGIC     -- Retry status.
# MAGIC     -- Suggested values: RUNNING, SUCCESS, FAILED
# MAGIC     status STRING,
# MAGIC 
# MAGIC     -- Error information from the failed operation.
# MAGIC     error_code STRING,
# MAGIC     error_message STRING,
# MAGIC 
# MAGIC     -- Suggestion 3: classify the error.
# MAGIC     -- Retry log should usually be for SYSTEM errors.
# MAGIC     error_type STRING,
# MAGIC 
# MAGIC     -- Suggestion 3: TRUE if retry is allowed.
# MAGIC     is_retryable BOOLEAN,
# MAGIC 
# MAGIC     -- Retry timing.
# MAGIC     started_at TIMESTAMP,
# MAGIC     ended_at TIMESTAMP,
# MAGIC 
# MAGIC     -- Suggestion 7: duration of this retry attempt in milliseconds.
# MAGIC     duration_ms BIGINT,
# MAGIC 
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- Table: log.invalid_record
# MAGIC -- Purpose:
# MAGIC --   Stores records that fail validation or transformation rules.
# MAGIC --
# MAGIC -- Example:
# MAGIC --   A policy record has null policy_id or invalid policy_status.
# MAGIC --
# MAGIC -- Applied improvements:
# MAGIC --   Suggestion 1: audit_key
# MAGIC --   Suggestion 3: error_type, is_retryable
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS log.invalid_record (
# MAGIC     id BIGINT,
# MAGIC 
# MAGIC     -- Related table-level audit session.
# MAGIC     table_session_id BIGINT,
# MAGIC 
# MAGIC     -- Deterministic key used later to prevent duplicate invalid-record logs.
# MAGIC     -- Suggested pattern: table_session_id + layer + target_table + record_key + error_column
# MAGIC     audit_key STRING,
# MAGIC 
# MAGIC     -- Layer where the invalid record was detected.
# MAGIC     -- Suggested values: BRONZE, SILVER, GOLD
# MAGIC     layer STRING,
# MAGIC 
# MAGIC     -- Target table related to the failed record.
# MAGIC     target_table STRING,
# MAGIC 
# MAGIC     -- Business key or primary key value of the failed record.
# MAGIC     record_key STRING,
# MAGIC 
# MAGIC     -- Raw record content.
# MAGIC     -- Warning: be careful with PII/customer-sensitive data.
# MAGIC     -- In production, this should be masked or limited.
# MAGIC     raw_data STRING,
# MAGIC 
# MAGIC     -- Column that failed validation.
# MAGIC     error_column STRING,
# MAGIC 
# MAGIC     -- Validation or transformation error reason.
# MAGIC     error_reason STRING,
# MAGIC 
# MAGIC     -- Suggestion 3: classify the error.
# MAGIC     -- Usually DATA or RULE for invalid records.
# MAGIC     error_type STRING,
# MAGIC 
# MAGIC     -- Suggestion 3: invalid data is usually not retryable.
# MAGIC     -- Usually FALSE for invalid records.
# MAGIC     is_retryable BOOLEAN,
# MAGIC 
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
