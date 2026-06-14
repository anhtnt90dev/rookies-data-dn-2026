# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "44c157dd-49ca-46c5-896a-9cb48544300e",
# META       "default_lakehouse_name": "audit_lakehouse_test",
# META       "default_lakehouse_workspace_id": "e1832509-bd92-47cc-be34-c5e939a6456a",
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
# MAGIC CREATE OR REPLACE VIEW log.vw_etl_table_layer_monitor AS
# MAGIC WITH latest_detail AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             d.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY d.table_session_id, d.layer
# MAGIC                 ORDER BY d.attempt_no DESC, d.created_at DESC
# MAGIC             ) AS rn
# MAGIC         FROM log.audit_detail d
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     s.id AS session_id,
# MAGIC     s.batch_id,
# MAGIC     s.pipeline_name,
# MAGIC     s.pipeline_run_id,
# MAGIC     s.run_mode,
# MAGIC     s.session_status,
# MAGIC     s.session_started,
# MAGIC     s.session_finished,
# MAGIC     s.duration_ms AS pipeline_duration_ms,
# MAGIC     s.sla_breached AS pipeline_sla_breached,
# MAGIC     t.id AS table_session_id,
# MAGIC     t.source_table_id,
# MAGIC     t.source_table_name,
# MAGIC     t.table_session_status,
# MAGIC     t.error_code AS table_error_code,
# MAGIC     t.error_message AS table_error_message,
# MAGIC     d.layer,
# MAGIC     CASE d.layer
# MAGIC         WHEN 'BRONZE' THEN t.bronze_status
# MAGIC         WHEN 'SILVER' THEN t.silver_status
# MAGIC         WHEN 'GOLD' THEN t.gold_status
# MAGIC         ELSE t.table_session_status
# MAGIC     END AS layer_status,
# MAGIC     CASE d.layer
# MAGIC         WHEN 'BRONZE' THEN t.bronze_started_at
# MAGIC         WHEN 'SILVER' THEN t.silver_started_at
# MAGIC         WHEN 'GOLD' THEN t.gold_started_at
# MAGIC     END AS layer_started_at,
# MAGIC     CASE d.layer
# MAGIC         WHEN 'BRONZE' THEN t.bronze_ended_at
# MAGIC         WHEN 'SILVER' THEN t.silver_ended_at
# MAGIC         WHEN 'GOLD' THEN t.gold_ended_at
# MAGIC     END AS layer_ended_at,
# MAGIC     d.detail_status,
# MAGIC     d.source_row_count,
# MAGIC     d.target_row_count,
# MAGIC     d.inserted_row,
# MAGIC     d.updated_row,
# MAGIC     d.deleted_row,
# MAGIC     d.rejected_row,
# MAGIC     d.error_type,
# MAGIC     d.is_retryable,
# MAGIC     d.error_message AS detail_error_message,
# MAGIC     d.created_at AS detail_created_at
# MAGIC FROM log.audit_session s
# MAGIC LEFT JOIN log.audit_table_session t
# MAGIC     ON s.id = t.session_id
# MAGIC LEFT JOIN latest_detail d
# MAGIC     ON t.id = d.table_session_id;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW log.vw_etl_pipeline_run_summary AS
# MAGIC WITH latest_detail AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             d.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY d.table_session_id, d.layer
# MAGIC                 ORDER BY d.attempt_no DESC, d.created_at DESC
# MAGIC             ) AS rn
# MAGIC         FROM log.audit_detail d
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC joined_log AS (
# MAGIC     SELECT
# MAGIC         s.id AS session_id,
# MAGIC         s.batch_id,
# MAGIC         s.pipeline_name,
# MAGIC         s.pipeline_run_id,
# MAGIC         s.run_mode,
# MAGIC         s.session_status,
# MAGIC         s.session_started,
# MAGIC         s.session_finished,
# MAGIC         s.duration_ms,
# MAGIC         s.sla_breached,
# MAGIC         t.id AS table_session_id,
# MAGIC         t.table_session_status,
# MAGIC         t.error_code AS table_error_code,
# MAGIC         t.error_message AS table_error_message,
# MAGIC         d.detail_status,
# MAGIC         d.source_row_count,
# MAGIC         d.target_row_count,
# MAGIC         d.inserted_row,
# MAGIC         d.updated_row,
# MAGIC         d.deleted_row,
# MAGIC         d.rejected_row,
# MAGIC         d.error_message AS detail_error_message
# MAGIC     FROM log.audit_session s
# MAGIC     LEFT JOIN log.audit_table_session t
# MAGIC         ON s.id = t.session_id
# MAGIC     LEFT JOIN latest_detail d
# MAGIC         ON t.id = d.table_session_id
# MAGIC )
# MAGIC SELECT
# MAGIC     session_id,
# MAGIC     batch_id,
# MAGIC     pipeline_name,
# MAGIC     pipeline_run_id,
# MAGIC     run_mode,
# MAGIC     session_status,
# MAGIC     session_started,
# MAGIC     session_finished,
# MAGIC     duration_ms,
# MAGIC     sla_breached,
# MAGIC     COUNT(DISTINCT table_session_id) AS table_session_count,
# MAGIC     SUM(CASE WHEN table_session_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_table_count,
# MAGIC     SUM(CASE WHEN table_session_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_table_count,
# MAGIC     SUM(COALESCE(source_row_count, 0)) AS total_source_row_count,
# MAGIC     SUM(COALESCE(target_row_count, 0)) AS total_target_row_count,
# MAGIC     SUM(COALESCE(inserted_row, 0)) AS total_inserted_row,
# MAGIC     SUM(COALESCE(updated_row, 0)) AS total_updated_row,
# MAGIC     SUM(COALESCE(deleted_row, 0)) AS total_deleted_row,
# MAGIC     SUM(COALESCE(rejected_row, 0)) AS total_rejected_row,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN session_status = 'FAILED'
# MAGIC               OR table_session_status = 'FAILED'
# MAGIC               OR detail_status = 'FAILED'
# MAGIC               OR table_error_code IS NOT NULL
# MAGIC               OR table_error_message IS NOT NULL
# MAGIC               OR detail_error_message IS NOT NULL
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS issue_count
# MAGIC FROM joined_log
# MAGIC GROUP BY
# MAGIC     session_id,
# MAGIC     batch_id,
# MAGIC     pipeline_name,
# MAGIC     pipeline_run_id,
# MAGIC     run_mode,
# MAGIC     session_status,
# MAGIC     session_started,
# MAGIC     session_finished,
# MAGIC     duration_ms,
# MAGIC     sla_breached;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW log.vw_etl_investigation_queue AS
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM log.vw_etl_table_layer_monitor
# MAGIC WHERE session_status = 'FAILED'
# MAGIC    OR table_session_status = 'FAILED'
# MAGIC    OR detail_status = 'FAILED'
# MAGIC    OR table_error_code IS NOT NULL
# MAGIC    OR table_error_message IS NOT NULL
# MAGIC    OR detail_error_message IS NOT NULL
# MAGIC    OR COALESCE(rejected_row, 0) > 0
# MAGIC    OR COALESCE(pipeline_sla_breached, FALSE) = TRUE;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM log.vw_etl_pipeline_run_summary
# MAGIC ORDER BY session_started DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM log.vw_etl_table_layer_monitor
# MAGIC ORDER BY session_started DESC, layer;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM log.vw_etl_investigation_queue
# MAGIC ORDER BY detail_created_at DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     s.batch_id,
# MAGIC     s.pipeline_name,
# MAGIC     s.pipeline_run_id,
# MAGIC     f.source_table_id,
# MAGIC     f.source_file,
# MAGIC     f.file_status,
# MAGIC     f.file_row_count,
# MAGIC     f.processed_row_count,
# MAGIC     f.rejected_row_count,
# MAGIC     f.retry_count,
# MAGIC     f.error_code,
# MAGIC     f.error_message,
# MAGIC     f.started_at,
# MAGIC     f.completed_at
# MAGIC FROM log.audit_file_session f
# MAGIC LEFT JOIN log.audit_session s
# MAGIC     ON f.session_id = s.id
# MAGIC ORDER BY f.started_at DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     r.table_session_id,
# MAGIC     r.file_session_id,
# MAGIC     r.layer,
# MAGIC     r.attempt_no,
# MAGIC     r.status,
# MAGIC     r.error_code,
# MAGIC     r.error_type,
# MAGIC     r.is_retryable,
# MAGIC     r.error_message,
# MAGIC     r.started_at,
# MAGIC     r.ended_at
# MAGIC FROM log.retry_log r
# MAGIC ORDER BY r.started_at DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     table_session_id,
# MAGIC     file_session_id,
# MAGIC     layer,
# MAGIC     target_table,
# MAGIC     record_key,
# MAGIC     error_column,
# MAGIC     error_reason,
# MAGIC     error_type,
# MAGIC     is_retryable,
# MAGIC     created_at
# MAGIC FROM log.invalid_record
# MAGIC ORDER BY created_at DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
