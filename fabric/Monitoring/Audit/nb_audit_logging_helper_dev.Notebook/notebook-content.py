# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Welcome to your new notebook
# Type here in the cell editor to add code!
# ============================================================
# notebook_name: nb_audit_logging_helper_dev
# purpose:
#   Reusable MVP helper functions for Fabric pipeline audit logging.
#
# This notebook supports Task 2:
#   - Capture whole pipeline execution status
#   - Capture table/layer execution status
#   - Capture error message when a layer fails
#
# Important:
#   This is MVP logic for dev/UAT.
#   It uses MAX(id) + 1 for IDs, which is okay for solo/dev testing.
#   For production/concurrent runs, use stronger ID generation.
# ============================================================

from datetime import datetime
import time


# ============================================================
# Helper: SQL literal formatter
# Purpose:
#   Safely convert Python values into SQL values.
# ============================================================

def format_sql_value(value):
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


# ============================================================
# Helper: Generate MVP numeric ID
# Purpose:
#   Get next ID using MAX(id) + 1.
#
# Note:
#   This is fine for MVP/dev.
#   In production, concurrent pipeline runs may create conflicts.
# ============================================================

def get_next_id(table_name: str) -> int:
    result = spark.sql(f"""
        SELECT COALESCE(MAX(id), 0) + 1 AS next_id
        FROM {table_name}
    """).collect()[0]["next_id"]

    return int(result)


# ============================================================
# Helper: Get ID by audit_key
# Purpose:
#   After MERGE/insert, fetch the generated row ID.
# ============================================================

def get_id_by_audit_key(table_name: str, audit_key: str) -> int:
    result = spark.sql(f"""
        SELECT id
        FROM {table_name}
        WHERE audit_key = {format_sql_value(audit_key)}
        LIMIT 1
    """).collect()

    if not result:
        raise Exception(f"No record found in {table_name} for audit_key = {audit_key}")

    return int(result[0]["id"])


# ============================================================
# Function: start_pipeline_session
# Purpose:
#   Create or update one pipeline-level audit session.
#
# Writes to:
#   log.audit_session
#
# Example:
#   One pipeline run = one row in log.audit_session
# ============================================================

def start_pipeline_session(
    pipeline_name: str,
    pipeline_run_id: str,
    batch_id: int,
    run_mode: str = "NEW",
    sla_target_ms: int = None
) -> int:

    audit_key = f"{pipeline_name}|{batch_id}|{pipeline_run_id}"
    next_audit_session_id = get_next_id("log.audit_session")

    spark.sql(f"""
        MERGE INTO log.audit_session AS target
        USING (
            SELECT
                {next_audit_session_id} AS id,
                {format_sql_value(audit_key)} AS audit_key,
                'RUNNING' AS session_status,
                {format_sql_value(run_mode)} AS run_mode,
                {batch_id} AS batch_id,
                {format_sql_value(pipeline_name)} AS pipeline_name,
                {format_sql_value(pipeline_run_id)} AS pipeline_run_id,
                current_timestamp() AS session_started,
                CAST(NULL AS TIMESTAMP) AS session_finished,
                CAST(NULL AS BIGINT) AS duration_ms,
                {format_sql_value(sla_target_ms)} AS sla_target_ms,
                CAST(NULL AS BOOLEAN) AS sla_breached,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
        ) AS source
        ON target.audit_key = source.audit_key

        WHEN MATCHED THEN UPDATE SET
            target.session_status = 'RUNNING',
            target.session_started = source.session_started,
            target.session_finished = NULL,
            target.duration_ms = NULL,
            target.sla_target_ms = source.sla_target_ms,
            target.sla_breached = NULL,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            id,
            audit_key,
            session_status,
            run_mode,
            batch_id,
            pipeline_name,
            pipeline_run_id,
            session_started,
            session_finished,
            duration_ms,
            sla_target_ms,
            sla_breached,
            created_at,
            updated_at
        )
        VALUES (
            source.id,
            source.audit_key,
            source.session_status,
            source.run_mode,
            source.batch_id,
            source.pipeline_name,
            source.pipeline_run_id,
            source.session_started,
            source.session_finished,
            source.duration_ms,
            source.sla_target_ms,
            source.sla_breached,
            source.created_at,
            source.updated_at
        )
    """)

    session_id = get_id_by_audit_key("log.audit_session", audit_key)

    print(f"Started pipeline session: session_id={session_id}, audit_key={audit_key}")
    return session_id


# ============================================================
# Function: finish_pipeline_session
# Purpose:
#   Mark the whole pipeline run as SUCCESS or FAILED.
#
# Writes to:
#   log.audit_session
# ============================================================

def finish_pipeline_session(session_id: int, final_status: str):
    final_status = final_status.upper()

    if final_status not in ["SUCCESS", "FAILED", "CANCELLED"]:
        raise ValueError("final_status must be SUCCESS, FAILED, or CANCELLED")

    spark.sql(f"""
        UPDATE log.audit_session
        SET
            session_status = {format_sql_value(final_status)},
            session_finished = current_timestamp(),
            duration_ms = CAST(
                (unix_timestamp(current_timestamp()) - unix_timestamp(session_started)) * 1000
                AS BIGINT
            ),
            sla_breached = CASE
                WHEN sla_target_ms IS NOT NULL
                 AND CAST((unix_timestamp(current_timestamp()) - unix_timestamp(session_started)) * 1000 AS BIGINT) > sla_target_ms
                THEN TRUE
                ELSE FALSE
            END,
            updated_at = current_timestamp()
        WHERE id = {session_id}
    """)

    print(f"Finished pipeline session: session_id={session_id}, status={final_status}")


# ============================================================
# Function: start_table_layer
# Purpose:
#   Create/update table-level session and mark one layer as RUNNING.
#
# Writes to:
#   log.audit_table_session
#
# Example:
#   table = policy
#   layer = SILVER
#   status becomes silver_status = RUNNING
# ============================================================

def start_table_layer(
    session_id: int,
    source_table_id: int,
    source_table_name: str,
    layer: str,
    batch_id: int,
    target_table_name: str = None,
    load_type: str = "FULL",
    watermark_column: str = None,
    watermark_before: str = None,
    load_window_start: str = None,
    load_window_end: str = None,
    sla_target_ms: int = None
) -> int:

    layer = layer.upper()

    layer_start_columns = {
        "BRONZE": "bronze_started_at",
        "SILVER": "silver_started_at",
        "GOLD": "gold_started_at"
    }

    layer_status_columns = {
        "BRONZE": "bronze_status",
        "SILVER": "silver_status",
        "GOLD": "gold_status"
    }

    if layer not in layer_start_columns:
        raise ValueError("layer must be BRONZE, SILVER, or GOLD")

    layer_started_column = layer_start_columns[layer]
    layer_status_column = layer_status_columns[layer]

    # Deterministic table-session key.
    # When cfg.source_table is ready, source_table_id should come from cfg.source_table.id.
    audit_key = f"session_{session_id}|source_{source_table_id}|table_{source_table_name}"
    next_audit_table_session_id = get_next_id("log.audit_table_session")

    spark.sql(f"""
        MERGE INTO log.audit_table_session AS target
        USING (
            SELECT
                {next_audit_table_session_id} AS id,
                {session_id} AS session_id,
                {format_sql_value(audit_key)} AS audit_key,
                {source_table_id} AS source_table_id,
                {format_sql_value(source_table_name)} AS source_table_name,
                {format_sql_value(target_table_name)} AS target_table_name,
                {batch_id} AS batch_id,
                'RUNNING' AS table_session_status,
                'NOT_RUN' AS bronze_status,
                'NOT_RUN' AS silver_status,
                'NOT_RUN' AS gold_status,
                {format_sql_value(load_type)} AS load_type,
                {format_sql_value(watermark_column)} AS watermark_column,
                {format_sql_value(watermark_before)} AS watermark_before,
                CAST(NULL AS STRING) AS watermark_after,
                CAST({format_sql_value(load_window_start)} AS TIMESTAMP) AS load_window_start,
                CAST({format_sql_value(load_window_end)} AS TIMESTAMP) AS load_window_end,
                CAST(NULL AS TIMESTAMP) AS bronze_started_at,
                CAST(NULL AS TIMESTAMP) AS silver_started_at,
                CAST(NULL AS TIMESTAMP) AS gold_started_at,
                CAST(NULL AS TIMESTAMP) AS bronze_ended_at,
                CAST(NULL AS TIMESTAMP) AS silver_ended_at,
                CAST(NULL AS TIMESTAMP) AS gold_ended_at,
                0 AS retry_count,
                CAST(NULL AS TIMESTAMP) AS last_retry_at,
                CAST(NULL AS BIGINT) AS duration_ms,
                {format_sql_value(sla_target_ms)} AS sla_target_ms,
                CAST(NULL AS BOOLEAN) AS sla_breached,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
        ) AS source
        ON target.audit_key = source.audit_key

        WHEN MATCHED THEN UPDATE SET
            target.table_session_status = 'RUNNING',
            target.{layer_status_column} = 'RUNNING',
            target.{layer_started_column} = current_timestamp(),
            target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT (
            id,
            session_id,
            audit_key,
            source_table_id,
            source_table_name,
            target_table_name,
            batch_id,
            table_session_status,
            bronze_status,
            silver_status,
            gold_status,
            load_type,
            watermark_column,
            watermark_before,
            watermark_after,
            load_window_start,
            load_window_end,
            bronze_started_at,
            silver_started_at,
            gold_started_at,
            bronze_ended_at,
            silver_ended_at,
            gold_ended_at,
            retry_count,
            last_retry_at,
            duration_ms,
            sla_target_ms,
            sla_breached,
            created_at,
            updated_at
        )
        VALUES (
            source.id,
            source.session_id,
            source.audit_key,
            source.source_table_id,
            source.source_table_name,
            source.target_table_name,
            source.batch_id,
            source.table_session_status,
            CASE WHEN '{layer}' = 'BRONZE' THEN 'RUNNING' ELSE source.bronze_status END,
            CASE WHEN '{layer}' = 'SILVER' THEN 'RUNNING' ELSE source.silver_status END,
            CASE WHEN '{layer}' = 'GOLD' THEN 'RUNNING' ELSE source.gold_status END,
            source.load_type,
            source.watermark_column,
            source.watermark_before,
            source.watermark_after,
            source.load_window_start,
            source.load_window_end,
            CASE WHEN '{layer}' = 'BRONZE' THEN current_timestamp() ELSE source.bronze_started_at END,
            CASE WHEN '{layer}' = 'SILVER' THEN current_timestamp() ELSE source.silver_started_at END,
            CASE WHEN '{layer}' = 'GOLD' THEN current_timestamp() ELSE source.gold_started_at END,
            source.bronze_ended_at,
            source.silver_ended_at,
            source.gold_ended_at,
            source.retry_count,
            source.last_retry_at,
            source.duration_ms,
            source.sla_target_ms,
            source.sla_breached,
            source.created_at,
            source.updated_at
        )
    """)

    table_session_id = get_id_by_audit_key("log.audit_table_session", audit_key)

    print(f"Started {layer} layer: table_session_id={table_session_id}, table={source_table_name}")
    return table_session_id


# ============================================================
# Function: finish_table_layer
# Purpose:
#   Mark a table/layer as SUCCESS or FAILED.
#   Also writes one row into log.audit_detail.
#
# Writes to:
#   log.audit_table_session
#   log.audit_detail
# ============================================================

def finish_table_layer(
    table_session_id: int,
    layer: str,
    status: str,
    is_final_table_step: bool = False,
    source_row_count: int = None,
    inserted_row: int = None,
    updated_row: int = None,
    deleted_row: int = None,
    rejected_row: int = None,
    error_message: str = None,
    error_type: str = None,
    is_retryable: bool = None,
    watermark_after: str = None,
    sla_target_ms: int = None
):

    layer = layer.upper()
    status = status.upper()

    if layer not in ["BRONZE", "SILVER", "GOLD"]:
        raise ValueError("layer must be BRONZE, SILVER, or GOLD")

    if status not in ["SUCCESS", "FAILED", "SKIPPED"]:
        raise ValueError("status must be SUCCESS, FAILED, or SKIPPED")

    layer_end_columns = {
        "BRONZE": "bronze_ended_at",
        "SILVER": "silver_ended_at",
        "GOLD": "gold_ended_at"
    }

    layer_start_columns = {
        "BRONZE": "bronze_started_at",
        "SILVER": "silver_started_at",
        "GOLD": "gold_started_at"
    }

    layer_status_columns = {
        "BRONZE": "bronze_status",
        "SILVER": "silver_status",
        "GOLD": "gold_status"
    }

    layer_ended_column = layer_end_columns[layer]
    layer_started_column = layer_start_columns[layer]
    layer_status_column = layer_status_columns[layer]

    # If this layer failed, table session is failed.
    # If this layer succeeded and it is the final step for this table, mark table session success.
    # Otherwise keep it running because another layer may still need to run.
    if status == "FAILED":
        table_session_status = "FAILED"
    elif is_final_table_step:
        table_session_status = "SUCCESS"
    else:
        table_session_status = "RUNNING"

    spark.sql(f"""
        UPDATE log.audit_table_session
        SET
            {layer_status_column} = {format_sql_value(status)},
            {layer_ended_column} = current_timestamp(),
            watermark_after = COALESCE({format_sql_value(watermark_after)}, watermark_after),
            table_session_status = {format_sql_value(table_session_status)},
            duration_ms = CAST(
                (unix_timestamp(current_timestamp()) - unix_timestamp(COALESCE({layer_started_column}, created_at))) * 1000
                AS BIGINT
            ),
            sla_target_ms = COALESCE({format_sql_value(sla_target_ms)}, sla_target_ms),
            sla_breached = CASE
                WHEN COALESCE({format_sql_value(sla_target_ms)}, sla_target_ms) IS NOT NULL
                 AND CAST((unix_timestamp(current_timestamp()) - unix_timestamp(COALESCE({layer_started_column}, created_at))) * 1000 AS BIGINT)
                     > COALESCE({format_sql_value(sla_target_ms)}, sla_target_ms)
                THEN TRUE
                ELSE FALSE
            END,
            updated_at = current_timestamp()
        WHERE id = {table_session_id}
    """)

    # Determine attempt number for this table/layer.
    attempt_result = spark.sql(f"""
        SELECT COALESCE(MAX(attempt_no), 0) + 1 AS next_attempt
        FROM log.audit_detail
        WHERE table_session_id = {table_session_id}
          AND layer = {format_sql_value(layer)}
    """).collect()[0]["next_attempt"]

    attempt_no = int(attempt_result)

    audit_detail_id = get_next_id("log.audit_detail")
    detail_audit_key = f"table_session_{table_session_id}|layer_{layer}|attempt_{attempt_no}"

    spark.sql(f"""
        INSERT INTO log.audit_detail (
            id,
            table_session_id,
            audit_key,
            attempt_no,
            detail_status,
            layer,
            watermark_before,
            watermark_after,
            load_window_start,
            load_window_end,
            source_row_count,
            inserted_row,
            updated_row,
            deleted_row,
            rejected_row,
            error_message,
            error_type,
            is_retryable,
            duration_ms,
            sla_target_ms,
            sla_breached,
            created_at,
            updated_at
        )
        SELECT
            {audit_detail_id} AS id,
            {table_session_id} AS table_session_id,
            {format_sql_value(detail_audit_key)} AS audit_key,
            {attempt_no} AS attempt_no,
            {format_sql_value(status)} AS detail_status,
            {format_sql_value(layer)} AS layer,
            ats.watermark_before,
            COALESCE({format_sql_value(watermark_after)}, ats.watermark_after) AS watermark_after,
            ats.load_window_start,
            ats.load_window_end,
            {format_sql_value(source_row_count)} AS source_row_count,
            {format_sql_value(inserted_row)} AS inserted_row,
            {format_sql_value(updated_row)} AS updated_row,
            {format_sql_value(deleted_row)} AS deleted_row,
            {format_sql_value(rejected_row)} AS rejected_row,
            {format_sql_value(error_message)} AS error_message,
            {format_sql_value(error_type)} AS error_type,
            {format_sql_value(is_retryable)} AS is_retryable,
            ats.duration_ms AS duration_ms,
            COALESCE({format_sql_value(sla_target_ms)}, ats.sla_target_ms) AS sla_target_ms,
            ats.sla_breached AS sla_breached,
            current_timestamp() AS created_at,
            current_timestamp() AS updated_at
        FROM log.audit_table_session ats
        WHERE ats.id = {table_session_id}
    """)

    print(f"Finished {layer} layer: table_session_id={table_session_id}, status={status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
