# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Reusable audit logging helpers for Microsoft Fabric notebooks.
import re
import uuid
from enum import Enum

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class AuditStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    NOT_RUN = "NOT_RUN"
    CANCELLED = "CANCELLED"


class Layer(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"


class RunMode(str, Enum):
    NEW = "NEW"
    RECOVERY = "RECOVERY"


class ErrorType(str, Enum):
    SYSTEM = "SYSTEM"
    DATA = "DATA"
    RULE = "RULE"
    CONFIG = "CONFIG"
    UNKNOWN = "UNKNOWN"


VALID_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")

AUDIT_SESSION_TABLE = "log.audit_session"
AUDIT_TABLE_SESSION_TABLE = "log.audit_table_session"
AUDIT_DETAIL_TABLE = "log.audit_detail"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def enum_value(value):
    if isinstance(value, Enum):
        return value.value
    return str(value).upper()


def validate_table_name(table_name: str) -> str:
    if not VALID_TABLE_NAME_PATTERN.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def require_layer(layer: str) -> str:
    layer_value = enum_value(layer)
    if layer_value not in {item.value for item in Layer}:
        raise ValueError("layer must be BRONZE, SILVER, or GOLD")
    return layer_value


def require_status(status: str, allowed_statuses) -> str:
    status_value = enum_value(status)
    allowed_values = {enum_value(item) for item in allowed_statuses}
    if status_value not in allowed_values:
        raise ValueError(f"status must be one of: {', '.join(sorted(allowed_values))}")
    return status_value


def new_audit_id() -> str:
    return str(uuid.uuid4())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_temp_view_from_rows(rows, schema, view_prefix="audit_source"):
    view_name = f"_{view_prefix}_{uuid.uuid4().hex}"
    spark.createDataFrame(rows, schema).createOrReplaceTempView(view_name)
    return view_name


def get_id_by_audit_key(table_name: str, audit_key: str) -> str:
    table_name = validate_table_name(table_name)
    result = (
        spark.table(table_name)
        .where(F.col("audit_key") == F.lit(audit_key))
        .select("id")
        .limit(1)
        .collect()
    )

    if not result:
        raise Exception(f"No record found in {table_name} for audit_key = {audit_key}")

    return str(result[0]["id"])


def get_next_attempt_no(audit_detail_table: str, table_session_id: str, layer: str) -> int:
    audit_detail_table = validate_table_name(audit_detail_table)
    layer = require_layer(layer)
    result = (
        spark.table(audit_detail_table)
        .where((F.col("table_session_id") == F.lit(table_session_id)) & (F.col("layer") == F.lit(layer)))
        .agg(F.coalesce(F.max("attempt_no"), F.lit(0)).alias("max_attempt_no"))
        .collect()[0]["max_attempt_no"]
    )
    return int(result) + 1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def start_pipeline_session(
    pipeline_name: str,
    pipeline_run_id: str,
    batch_id: int,
    run_mode: str = RunMode.NEW,
    sla_target_ms: int = None,
    audit_session_table: str = AUDIT_SESSION_TABLE,
) -> str:
    audit_session_table = validate_table_name(audit_session_table)
    run_mode = require_status(run_mode, [RunMode.NEW, RunMode.RECOVERY])
    audit_key = f"{pipeline_name}|{batch_id}|{pipeline_run_id}"

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("audit_key", StringType(), False),
        StructField("session_status", StringType(), False),
        StructField("run_mode", StringType(), False),
        StructField("batch_id", LongType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("sla_target_ms", LongType(), True),
    ])
    source_view = create_temp_view_from_rows([
        Row(
            id=new_audit_id(),
            audit_key=audit_key,
            session_status=AuditStatus.RUNNING.value,
            run_mode=run_mode,
            batch_id=int(batch_id),
            pipeline_name=pipeline_name,
            pipeline_run_id=pipeline_run_id,
            sla_target_ms=sla_target_ms,
        )
    ], schema, "audit_session")

    spark.sql(f"""
        MERGE INTO {audit_session_table} AS target
        USING (
            SELECT
                id,
                audit_key,
                session_status,
                run_mode,
                batch_id,
                pipeline_name,
                pipeline_run_id,
                current_timestamp() AS session_started,
                CAST(NULL AS TIMESTAMP) AS session_finished,
                CAST(NULL AS BIGINT) AS duration_ms,
                sla_target_ms,
                CAST(NULL AS BOOLEAN) AS sla_breached,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
            FROM {source_view}
        ) AS source
        ON target.audit_key = source.audit_key
        WHEN MATCHED THEN UPDATE SET
            target.session_status = source.session_status,
            target.session_started = source.session_started,
            target.session_finished = NULL,
            target.duration_ms = NULL,
            target.sla_target_ms = source.sla_target_ms,
            target.sla_breached = NULL,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, audit_key, session_status, run_mode, batch_id,
            pipeline_name, pipeline_run_id, session_started,
            session_finished, duration_ms, sla_target_ms,
            sla_breached, created_at, updated_at
        ) VALUES (
            source.id, source.audit_key, source.session_status, source.run_mode, source.batch_id,
            source.pipeline_name, source.pipeline_run_id, source.session_started,
            source.session_finished, source.duration_ms, source.sla_target_ms,
            source.sla_breached, source.created_at, source.updated_at
        )
    """)

    session_id = get_id_by_audit_key(audit_session_table, audit_key)
    print(f"Started pipeline session: session_id={session_id}, audit_key={audit_key}")
    return session_id


def finish_pipeline_session(
    session_id: str,
    final_status: str,
    audit_session_table: str = AUDIT_SESSION_TABLE,
):
    audit_session_table = validate_table_name(audit_session_table)
    final_status = require_status(final_status, [AuditStatus.SUCCESS, AuditStatus.FAILED, AuditStatus.CANCELLED])

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("session_status", StringType(), False),
    ])
    source_view = create_temp_view_from_rows([
        Row(id=str(session_id), session_status=final_status)
    ], schema, "finish_session")

    spark.sql(f"""
        MERGE INTO {audit_session_table} AS target
        USING (
            SELECT id, session_status, current_timestamp() AS updated_at
            FROM {source_view}
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.session_status = source.session_status,
            target.session_finished = source.updated_at,
            target.duration_ms = CAST(
                (unix_timestamp(source.updated_at) - unix_timestamp(target.session_started)) * 1000 AS BIGINT
            ),
            target.sla_breached = CASE
                WHEN target.sla_target_ms IS NOT NULL
                 AND CAST((unix_timestamp(source.updated_at) - unix_timestamp(target.session_started)) * 1000 AS BIGINT) > target.sla_target_ms
                THEN TRUE
                ELSE FALSE
            END,
            target.updated_at = source.updated_at
    """)

    print(f"Finished pipeline session: session_id={session_id}, status={final_status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def start_table_layer(
    session_id: str,
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
    sla_target_ms: int = None,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
) -> str:
    audit_table_session_table = validate_table_name(audit_table_session_table)
    layer = require_layer(layer)
    layer_started_column = {Layer.BRONZE.value: "bronze_started_at", Layer.SILVER.value: "silver_started_at", Layer.GOLD.value: "gold_started_at"}[layer]
    layer_status_column = {Layer.BRONZE.value: "bronze_status", Layer.SILVER.value: "silver_status", Layer.GOLD.value: "gold_status"}[layer]
    audit_key = f"session_{session_id}|source_{source_table_id}|table_{source_table_name}"

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("audit_key", StringType(), False),
        StructField("source_table_id", LongType(), False),
        StructField("source_table_name", StringType(), False),
        StructField("target_table_name", StringType(), True),
        StructField("batch_id", LongType(), False),
        StructField("load_type", StringType(), True),
        StructField("watermark_column", StringType(), True),
        StructField("watermark_before", StringType(), True),
        StructField("load_window_start_text", StringType(), True),
        StructField("load_window_end_text", StringType(), True),
        StructField("sla_target_ms", LongType(), True),
    ])
    source_view = create_temp_view_from_rows([
        Row(
            id=new_audit_id(),
            session_id=str(session_id),
            audit_key=audit_key,
            source_table_id=int(source_table_id),
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            batch_id=int(batch_id),
            load_type=load_type,
            watermark_column=watermark_column,
            watermark_before=watermark_before,
            load_window_start_text=load_window_start,
            load_window_end_text=load_window_end,
            sla_target_ms=sla_target_ms,
        )
    ], schema, "table_layer")

    spark.sql(f"""
        MERGE INTO {audit_table_session_table} AS target
        USING (
            SELECT
                id, session_id, audit_key, source_table_id, source_table_name, target_table_name, batch_id,
                '{AuditStatus.RUNNING.value}' AS table_session_status,
                '{AuditStatus.NOT_RUN.value}' AS bronze_status,
                '{AuditStatus.NOT_RUN.value}' AS silver_status,
                '{AuditStatus.NOT_RUN.value}' AS gold_status,
                load_type, watermark_column, watermark_before,
                CAST(NULL AS STRING) AS watermark_after,
                CAST(load_window_start_text AS TIMESTAMP) AS load_window_start,
                CAST(load_window_end_text AS TIMESTAMP) AS load_window_end,
                CAST(NULL AS TIMESTAMP) AS bronze_started_at,
                CAST(NULL AS TIMESTAMP) AS silver_started_at,
                CAST(NULL AS TIMESTAMP) AS gold_started_at,
                CAST(NULL AS TIMESTAMP) AS bronze_ended_at,
                CAST(NULL AS TIMESTAMP) AS silver_ended_at,
                CAST(NULL AS TIMESTAMP) AS gold_ended_at,
                0 AS retry_count,
                CAST(NULL AS TIMESTAMP) AS last_retry_at,
                CAST(NULL AS BIGINT) AS duration_ms,
                sla_target_ms,
                CAST(NULL AS BOOLEAN) AS sla_breached,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
            FROM {source_view}
        ) AS source
        ON target.audit_key = source.audit_key
        WHEN MATCHED THEN UPDATE SET
            target.table_session_status = source.table_session_status,
            target.{layer_status_column} = source.table_session_status,
            target.{layer_started_column} = source.updated_at,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, session_id, audit_key, source_table_id, batch_id, table_session_status,
            bronze_status, silver_status, gold_status, load_type, watermark_column,
            watermark_before, watermark_after, load_window_start, load_window_end,
            bronze_started_at, silver_started_at, gold_started_at,
            bronze_ended_at, silver_ended_at, gold_ended_at,
            retry_count, last_retry_at, duration_ms, sla_target_ms, sla_breached,
            created_at, updated_at, source_table_name, target_table_name
        ) VALUES (
            source.id, source.session_id, source.audit_key, source.source_table_id, source.batch_id, source.table_session_status,
            CASE WHEN '{layer}' = '{Layer.BRONZE.value}' THEN '{AuditStatus.RUNNING.value}' ELSE source.bronze_status END,
            CASE WHEN '{layer}' = '{Layer.SILVER.value}' THEN '{AuditStatus.RUNNING.value}' ELSE source.silver_status END,
            CASE WHEN '{layer}' = '{Layer.GOLD.value}' THEN '{AuditStatus.RUNNING.value}' ELSE source.gold_status END,
            source.load_type, source.watermark_column,
            source.watermark_before, source.watermark_after, source.load_window_start, source.load_window_end,
            CASE WHEN '{layer}' = '{Layer.BRONZE.value}' THEN source.updated_at ELSE source.bronze_started_at END,
            CASE WHEN '{layer}' = '{Layer.SILVER.value}' THEN source.updated_at ELSE source.silver_started_at END,
            CASE WHEN '{layer}' = '{Layer.GOLD.value}' THEN source.updated_at ELSE source.gold_started_at END,
            source.bronze_ended_at, source.silver_ended_at, source.gold_ended_at,
            source.retry_count, source.last_retry_at, source.duration_ms, source.sla_target_ms, source.sla_breached,
            source.created_at, source.updated_at, source.source_table_name, source.target_table_name
        )
    """)

    table_session_id = get_id_by_audit_key(audit_table_session_table, audit_key)
    print(f"Started {layer} layer: table_session_id={table_session_id}, table={source_table_name}")
    return table_session_id


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AUDIT_DETAIL_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("table_session_id", StringType(), False),
    StructField("audit_key", StringType(), False),
    StructField("attempt_no", IntegerType(), True),
    StructField("detail_status", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("watermark_before", StringType(), True),
    StructField("watermark_after", StringType(), True),
    StructField("load_window_start", TimestampType(), True),
    StructField("load_window_end", TimestampType(), True),
    StructField("source_row_count", IntegerType(), True),
    StructField("target_row_count", IntegerType(), True),
    StructField("inserted_row", IntegerType(), True),
    StructField("updated_row", IntegerType(), True),
    StructField("deleted_row", IntegerType(), True),
    StructField("rejected_row", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("error_type", StringType(), True),
    StructField("is_retryable", BooleanType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("sla_target_ms", LongType(), True),
    StructField("sla_breached", BooleanType(), True),
])


def append_audit_detail(row_values, audit_detail_table: str = AUDIT_DETAIL_TABLE):
    audit_detail_table = validate_table_name(audit_detail_table)
    detail_df = (
        spark.createDataFrame([Row(**row_values)], AUDIT_DETAIL_SCHEMA)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    detail_df.write.format("delta").mode("append").saveAsTable(audit_detail_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def finish_table_layer(
    table_session_id: str,
    layer: str,
    status: str,
    is_final_table_step: bool = False,
    source_row_count: int = None,
    target_row_count: int = None,
    inserted_row: int = None,
    updated_row: int = None,
    deleted_row: int = None,
    rejected_row: int = None,
    error_message: str = None,
    error_type: str = None,
    is_retryable: bool = None,
    watermark_after: str = None,
    sla_target_ms: int = None,
    write_detail: bool = True,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
    audit_detail_table: str = AUDIT_DETAIL_TABLE,
):
    audit_table_session_table = validate_table_name(audit_table_session_table)
    audit_detail_table = validate_table_name(audit_detail_table)
    layer = require_layer(layer)
    status = require_status(status, [AuditStatus.SUCCESS, AuditStatus.FAILED, AuditStatus.SKIPPED])
    layer_ended_column = {Layer.BRONZE.value: "bronze_ended_at", Layer.SILVER.value: "silver_ended_at", Layer.GOLD.value: "gold_ended_at"}[layer]
    layer_started_column = {Layer.BRONZE.value: "bronze_started_at", Layer.SILVER.value: "silver_started_at", Layer.GOLD.value: "gold_started_at"}[layer]
    layer_status_column = {Layer.BRONZE.value: "bronze_status", Layer.SILVER.value: "silver_status", Layer.GOLD.value: "gold_status"}[layer]

    if status == AuditStatus.FAILED.value:
        table_session_status = AuditStatus.FAILED.value
    elif is_final_table_step:
        table_session_status = AuditStatus.SUCCESS.value
    else:
        table_session_status = AuditStatus.RUNNING.value

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("layer_status", StringType(), False),
        StructField("table_session_status", StringType(), False),
        StructField("watermark_after", StringType(), True),
        StructField("sla_target_ms", LongType(), True),
    ])
    source_view = create_temp_view_from_rows([
        Row(id=str(table_session_id), layer_status=status, table_session_status=table_session_status, watermark_after=watermark_after, sla_target_ms=sla_target_ms)
    ], schema, "finish_table_layer")

    spark.sql(f"""
        MERGE INTO {audit_table_session_table} AS target
        USING (
            SELECT
                id,
                layer_status,
                table_session_status,
                watermark_after,
                sla_target_ms,
                current_timestamp() AS finished_at
            FROM {source_view}
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.{layer_status_column} = source.layer_status,
            target.{layer_ended_column} = source.finished_at,
            target.watermark_after = COALESCE(source.watermark_after, target.watermark_after),
            target.table_session_status = source.table_session_status,
            target.duration_ms = CAST(
                (unix_timestamp(source.finished_at) - unix_timestamp(COALESCE(target.{layer_started_column}, target.created_at))) * 1000 AS BIGINT
            ),
            target.sla_target_ms = COALESCE(source.sla_target_ms, target.sla_target_ms),
            target.sla_breached = CASE
                WHEN COALESCE(source.sla_target_ms, target.sla_target_ms) IS NOT NULL
                 AND CAST((unix_timestamp(source.finished_at) - unix_timestamp(COALESCE(target.{layer_started_column}, target.created_at))) * 1000 AS BIGINT)
                     > COALESCE(source.sla_target_ms, target.sla_target_ms)
                THEN TRUE
                ELSE FALSE
            END,
            target.updated_at = source.finished_at
    """)

    if write_detail:
        table_snapshot = (
            spark.table(audit_table_session_table)
            .where(F.col("id") == F.lit(str(table_session_id)))
            .select("watermark_before", "watermark_after", "load_window_start", "load_window_end", "duration_ms", "sla_target_ms", "sla_breached")
            .limit(1)
            .collect()
        )
        if not table_snapshot:
            raise Exception(f"No table session found for id={table_session_id}")

        table_row = table_snapshot[0]
        attempt_no = get_next_attempt_no(audit_detail_table, str(table_session_id), layer)
        detail_audit_key = f"table_session_{table_session_id}|layer_{layer}|attempt_{attempt_no}"
        append_audit_detail({
            "id": new_audit_id(),
            "table_session_id": str(table_session_id),
            "audit_key": detail_audit_key,
            "attempt_no": attempt_no,
            "detail_status": status,
            "layer": layer,
            "watermark_before": table_row["watermark_before"],
            "watermark_after": watermark_after or table_row["watermark_after"],
            "load_window_start": table_row["load_window_start"],
            "load_window_end": table_row["load_window_end"],
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "inserted_row": inserted_row,
            "updated_row": updated_row,
            "deleted_row": deleted_row,
            "rejected_row": rejected_row,
            "error_message": error_message,
            "error_type": enum_value(error_type) if error_type is not None else None,
            "is_retryable": is_retryable,
            "duration_ms": table_row["duration_ms"],
            "sla_target_ms": sla_target_ms or table_row["sla_target_ms"],
            "sla_breached": table_row["sla_breached"],
        }, audit_detail_table)

    print(f"Finished {layer} layer: table_session_id={table_session_id}, status={status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
