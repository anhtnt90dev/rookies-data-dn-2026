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
# META     },
# META     "environment": {
# META       "environmentId": "0519000f-54ce-ab89-4a90-a6f539ba103a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

session_id = ""
batch_id = ""
previous_session_id = ""
run_mode = ""
pipeline_run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("The 'batch_id' parameter must be provided as a non-empty integer.")
else:
    batch_id = int(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

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

# MARKDOWN ********************

# ## **INSERT AUDIT_SESSION**

# CELL ********************

class AuditStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class RunMode(str, Enum):
    NEW = "NEW"
    RECOVERY = "RECOVERY"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALID_TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

AUDIT_SESSION_TABLE = "log.audit_session"

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


def require_status(status: str, allowed_statuses) -> str:
    status_value = enum_value(status)
    allowed_values = {enum_value(item) for item in allowed_statuses}

    if status_value not in allowed_values:
        raise ValueError(
            f"status must be one of: {', '.join(sorted(allowed_values))}"
        )

    return status_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def start_pipeline_session(
    session_id: str,
    pipeline_name: str,
    pipeline_run_id: str,
    batch_id: int,
    run_mode: str = RunMode.NEW,
    sla_target_ms: int = None,
    audit_session_table: str = AUDIT_SESSION_TABLE,
) -> str:

    audit_session_table = validate_table_name(audit_session_table)

    run_mode = require_status(
        run_mode,
        [RunMode.NEW, RunMode.RECOVERY]
    )
    
    query = f"""
        INSERT INTO {audit_session_table}
        VALUES (
            '{session_id}',
            '{AuditStatus.RUNNING.value}',
            '{run_mode}',
            {int(batch_id)},
            '{pipeline_name}',
            '{pipeline_run_id}',
            current_timestamp(),
            NULL,
            NULL,
            {sla_target_ms if sla_target_ms is not None else 'NULL'},
            NULL,
            current_timestamp(),
            current_timestamp()
        )
    """

    spark.sql(query)

    print(
        f"Started pipeline session: "
        f"session_id={session_id}, "
        f"pipeline_run_id={pipeline_run_id}"
    )

    return session_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_mode = RunMode.NEW if run_mode == "NEW" else RunMode.RECOVERY
batch_id = int(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_pipeline_session(
    session_id=session_id,
    pipeline_name="pl_master_etl",
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode=run_mode
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **METADATA LOOKUP**

# CELL ********************

source_table_values = spark.sql("""
SELECT
    s.id,
    s.source_system,
    s.source_type,
    s.source_name,
    s.source_format,
    s.source_location,
    s.load_type,
    s.watermark_column,
    s.source_to_bronze_mapping_path,
    s.bronze_table_name,
    s.load_sequence,
    w.watermark_value
FROM cfg.source_table s
LEFT JOIN cfg.watermark w
    ON s.id = w.source_table_id
WHERE s.is_active = 1
ORDER BY s.load_sequence
""").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **INSERT AUDIT RECORDS INTO log.audit_table_session**

# CELL ********************

class AuditStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    NOT_RUN = "NOT_RUN"
    CANCELLED = "CANCELLED"


class RunMode(str, Enum):
    NEW = "NEW"
    RECOVERY = "RECOVERY"


VALID_TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

AUDIT_TABLE_SESSION_TABLE = "log.audit_table_session"


def enum_value(value) -> str:
    if isinstance(value, Enum):
        return value.value
    return str(value).upper()


def validate_table_name(table_name: str) -> str:
    if not VALID_TABLE_NAME_PATTERN.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def require_status(status: str, allowed_statuses) -> str:
    status_value = enum_value(status)
    allowed_values = {enum_value(item) for item in allowed_statuses}

    if status_value not in allowed_values:
        raise ValueError(
            f"status must be one of: {', '.join(sorted(allowed_values))}"
        )

    return status_value


def new_audit_id() -> str:
    return str(uuid.uuid4())


def to_recovery_status(previous_status: str) -> str:
    if previous_status == AuditStatus.SUCCESS.value:
        return AuditStatus.SKIPPED.value
    return AuditStatus.NOT_RUN.value


def insert_audit_table_sessions(
    session_id: str,
    batch_id: int,
    sources,
    run_mode: str = RunMode.NEW,
    recovery_session_id: str = None,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
) -> None:
    audit_table_session_table = validate_table_name(audit_table_session_table)
    run_mode = require_status(run_mode, [RunMode.NEW, RunMode.RECOVERY])

    if hasattr(sources, "collect"):
        sources = sources.collect()

    if not sources:
        print("No active sources found. No audit table sessions inserted.")
        return

    previous_status_by_source = {}

    if run_mode == RunMode.RECOVERY.value:
        if not recovery_session_id:
            raise ValueError("recovery_session_id is required for RECOVERY mode")

        previous_rows = (
            spark.table(audit_table_session_table)
            .where(F.col("session_id") == F.lit(str(recovery_session_id)))
            .select(
                "source_table_id",
                "bronze_status",
                "silver_status",
                "gold_status",
            )
            .collect()
        )

        previous_status_by_source = {}

        for row in previous_rows:
            previous_status_by_source[int(row["source_table_id"])] = {
                "bronze_status": row["bronze_status"],
                "silver_status": row["silver_status"],
                "gold_status": row["gold_status"]
            }

    rows = []

    for source in sources:
        source_table_id = int(source["id"])
        previous_status = previous_status_by_source.get(source_table_id, {})

        if run_mode == RunMode.RECOVERY.value:
            bronze_status = to_recovery_status(previous_status.get("bronze_status"))
            silver_status = to_recovery_status(previous_status.get("silver_status"))
            gold_status = to_recovery_status(previous_status.get("gold_status"))
        else:
            bronze_status = AuditStatus.NOT_RUN.value
            silver_status = AuditStatus.NOT_RUN.value
            gold_status = AuditStatus.NOT_RUN.value

        rows.append(
            Row(
                id=new_audit_id(),
                session_id=str(session_id),
                source_table_id=source_table_id,
                batch_id=int(batch_id),
                table_session_status=AuditStatus.NOT_RUN.value,
                bronze_status=bronze_status,
                silver_status=silver_status,
                gold_status=gold_status,
                load_type=source["load_type"],
                source_table_name=source["source_name"],
                watermark_column=source["watermark_column"],
                watermark_before=source["watermark_value"],
            )
        )

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("source_table_id", LongType(), False),
        StructField("batch_id", LongType(), False),
        StructField("table_session_status", StringType(), False),
        StructField("bronze_status", StringType(), False),
        StructField("silver_status", StringType(), False),
        StructField("gold_status", StringType(), False),
        StructField("load_type", StringType(), True),
        StructField("source_table_name", StringType(), True),
        StructField("watermark_column", StringType(), True),
        StructField("watermark_before", StringType(), True),
    ])

    audit_df = (
        spark.createDataFrame(rows, schema)
        .withColumn("watermark_after", F.lit(None).cast("string"))
        .withColumn("load_window_start", F.lit(None).cast("timestamp"))
        .withColumn("load_window_end", F.lit(None).cast("timestamp"))
        .withColumn("bronze_started_at", F.lit(None).cast("timestamp"))
        .withColumn("silver_started_at", F.lit(None).cast("timestamp"))
        .withColumn("gold_started_at", F.lit(None).cast("timestamp"))
        .withColumn("bronze_ended_at", F.lit(None).cast("timestamp"))
        .withColumn("silver_ended_at", F.lit(None).cast("timestamp"))
        .withColumn("gold_ended_at", F.lit(None).cast("timestamp"))
        .withColumn("error_code", F.lit(None).cast("string"))
        .withColumn("error_message", F.lit(None).cast("string"))
        .withColumn("retry_count", F.lit(0))
        .withColumn("last_retry_at", F.lit(None).cast("timestamp"))
        .withColumn("duration_ms", F.lit(None).cast("bigint"))
        .withColumn("sla_target_ms", F.lit(None).cast("bigint"))
        .withColumn("sla_breached", F.lit(None).cast("boolean"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    audit_df.write.format("delta").mode("append").saveAsTable(audit_table_session_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

insert_audit_table_sessions(
    session_id=session_id,
    batch_id=batch_id,
    sources=source_table_values,
    run_mode = RunMode.NEW if run_mode == "NEW" else RunMode.RECOVERY,
    recovery_session_id = previous_session_id if previous_session_id else None,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **PROCESS SOURCE-TO-BRONZE INGESTION**

# CELL ********************

# =========================
# Context
# =========================

@dataclass
class RunContext:
    session_id: str
    batch_id: int
    run_mode: str   # NEW | RECOVERY


@dataclass
class SourceConfig:
    id: int
    source_system: str
    source_type: str
    source_name: str
    source_location: str
    source_format: str
    load_type: str
    watermark_column: Optional[str]
    source_to_bronze_mapping_path: str
    bronze_table_name: str


# =========================
# Common helpers
# =========================

def escape_sql(value):
    if value is None:
        return None
    return str(value).replace("'", "''")


def get_fs_utils():
    if "notebookutils" in globals():
        return notebookutils.fs
    if "mssparkutils" in globals():
        return mssparkutils.fs
    raise RuntimeError("No Fabric filesystem utility found: notebookutils/mssparkutils")


def list_files(path: str) -> list[str]:
    fs = get_fs_utils()
    return [f.path for f in fs.ls(path) if not f.isDir]


def align_to_target_schema(spark, df, target_table: str):
    target_schema = spark.table(target_table).schema

    exprs = []
    for field in target_schema:
        if field.name in df.columns:
            exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    return df.select(*exprs)


def apply_load_type_filter(df, source: SourceConfig, watermark_before):
    load_type = (source.load_type or "").upper()

    if load_type == "FULL":
        return df

    if load_type == "INCREMENTAL":
        if not source.watermark_column or source.watermark_column.strip() == "":
            raise ValueError("watermark_column is required for INCREMENTAL load")

        if watermark_before is None:
            return df

        return df.where(
            F.to_timestamp(F.col(source.watermark_column)) > F.lit(watermark_before)
        )

    raise ValueError(f"Unsupported load_type: {source.load_type}")


# =========================
# Watermark helpers
# =========================

def read_watermark(spark, source_table_id: int):
    rows = (
        spark.table("cfg.watermark")
        .where(F.col("source_table_id") == F.lit(source_table_id))
        .select("watermark_value")
        .limit(1)
        .collect()
    )

    if not rows:
        return None

    return rows[0]["watermark_value"]


def update_watermark(spark, source_table_id: int, watermark_after):
    if watermark_after is None:
        return

    safe_watermark = escape_sql(format_watermark(watermark_after))

    spark.sql(f"""
        MERGE INTO cfg.watermark AS target
        USING (
            SELECT
                CAST({source_table_id} AS BIGINT) AS source_table_id,
                TIMESTAMP('{safe_watermark}') AS watermark_value
        ) AS source
        ON target.source_table_id = source.source_table_id

        WHEN MATCHED THEN
            UPDATE SET
                target.watermark_value = source.watermark_value,
                target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN
            INSERT (
                source_table_id,
                watermark_value,
                created_at,
                updated_at
            )
            VALUES (
                source.source_table_id,
                source.watermark_value,
                current_timestamp(),
                current_timestamp()
            )
    """)


def get_max_watermark(df, source: SourceConfig):
    watermark_column = source.watermark_column

    if not watermark_column or watermark_column.strip() == "":
        return None

    if watermark_column not in df.columns:
        raise ValueError(
            f"watermark_column '{watermark_column}' not found in source columns: {df.columns}"
        )

    result = (
        df
        .agg(
            F.count("*").alias("row_count"),
            F.max(F.to_timestamp(F.col(watermark_column))).alias("watermark_after")
        )
        .collect()[0]
    )

    row_count = result["row_count"]
    watermark_after = result["watermark_after"]

    if row_count > 0 and watermark_after is None:
        raise ValueError(
            f"watermark_column '{watermark_column}' exists but all values are NULL or invalid timestamp"
        )

    return watermark_after


# =========================
# Mapping helpers
# =========================

def read_mapping(mapping_path: str) -> dict:
    full_path = f"/lakehouse/default/{mapping_path}"

    with open(full_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_source_schema(df, mapping: dict):
    required_columns = [
        col["expression"]
        for col in mapping["columns"]
        if col["expression"] is not None
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing source columns: {missing_columns}")


def apply_source_to_bronze_mapping(
    df,
    mapping: dict,
    ctx: RunContext,
    source: SourceConfig,
    source_file: str = None
):
    select_exprs = []

    for col in mapping["columns"]:
        target = col["target"]
        expression = col["expression"]

        if expression is not None:
            select_exprs.append(F.col(expression).alias(target))

        elif target == "_batch_id":
            select_exprs.append(F.lit(str(ctx.batch_id)).alias(target))

        elif target == "_loaded_at":
            select_exprs.append(F.current_timestamp().alias(target))

        elif target == "_source_system":
            select_exprs.append(F.lit(source.source_system).alias(target))

        elif target == "_source_name":
            select_exprs.append(F.lit(source.source_name).alias(target))

        elif target == "_source_file":
            select_exprs.append(F.lit(source_file).alias(target))

        else:
            select_exprs.append(F.lit(None).alias(target))

    return df.select(*select_exprs)


# =========================
# Audit table session
# =========================

def get_table_session_id(spark, ctx: RunContext, source: SourceConfig):
    rows = (
        spark.table("log.audit_table_session")
        .where(
            (F.col("session_id") == F.lit(ctx.session_id))
            & (F.col("batch_id") == F.lit(ctx.batch_id))
            & (F.col("source_table_id") == F.lit(source.id))
        )
        .select("id", "bronze_status", "watermark_before")
        .limit(1)
        .collect()
    )

    if not rows:
        raise Exception(
            f"No audit_table_session found for session_id={ctx.session_id}, "
            f"batch_id={ctx.batch_id}, source_table_id={source.id}"
        )

    return rows[0]["id"], rows[0]["bronze_status"], rows[0]["watermark_before"]

def bulk_update_audit_table_session(spark, results: list[dict]):
    if not results:
        return

    rows = [
        (
            r["table_session_id"],
            r["status"],
            format_watermark(r.get("watermark_before")),
            format_watermark(r.get("watermark_after")),
            r.get("error_message")
        )
        for r in results
        if r.get("table_session_id") is not None
    ]

    if not rows:
        return

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("watermark_before", StringType(), True),
        StructField("watermark_after", StringType(), True),
        StructField("error_message", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema)
    df.createOrReplaceTempView("tmp_bronze_results")

    spark.sql("""
        MERGE INTO log.audit_table_session AS target
        USING tmp_bronze_results AS source
        ON target.id = source.id

        WHEN MATCHED THEN UPDATE SET
            target.bronze_status = source.status,
            target.table_session_status =
                CASE
                    WHEN source.status = 'FAILED' THEN 'FAILED'
                    ELSE 'RUNNING'
                END,
            target.watermark_before = source.watermark_before,
            target.watermark_after = source.watermark_after,
            target.bronze_ended_at = current_timestamp(),
            target.error_code =
                CASE
                    WHEN source.status = 'FAILED' THEN 'BRONZE_LOAD_FAILED'
                    ELSE NULL
                END,
            target.error_message = source.error_message,
            target.updated_at = current_timestamp()
    """)


# =========================
# Audit detail
# =========================

def bulk_insert_audit_detail(spark, results: list[dict]):
    if not results:
        return

    rows = []

    for r in results:
        if r.get("table_session_id") is None:
            continue

        rows.append(Row(
            id=str(uuid.uuid4()),
            table_session_id=r["table_session_id"],
            attempt_no=1,
            detail_status=r["status"],
            layer="BRONZE",
            watermark_before=format_watermark(r.get("watermark_before")),
            watermark_after=format_watermark(r.get("watermark_after")),
            load_window_start=None,
            load_window_end=None,
            source_row_count=int(r.get("source_row_count") or 0),
            target_row_count=int(r.get("target_row_count") or 0),
            inserted_row=int(r.get("inserted_row") or 0),
            updated_row=0,
            deleted_row=0,
            rejected_row=int(r.get("rejected_row") or 0),
            error_message=r.get("error_message"),
            error_type=None,
            is_retryable=None,
            duration_ms=None,
            sla_target_ms=None,
            sla_breached=None
        ))

    if not rows:
        return

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("table_session_id", StringType(), False),
        StructField("attempt_no", IntegerType(), True),
        StructField("detail_status", StringType(), False),
        StructField("layer", StringType(), False),
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

    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("append").saveAsTable("log.audit_detail")


# =========================
# File session helpers
# =========================

def get_relative_source_file(source_file: str) -> str:
    marker = "Files/"

    if marker in source_file:
        return marker + source_file.split(marker, 1)[1]

    return source_file


def read_dirty_json_file_or_folder(spark, path: str, max_size_mb: int = 50):
    fs = get_fs_utils()

    file_size_bytes = sum(file.size for file in fs.ls(path))
    file_size_mb = file_size_bytes / 1024 / 1024

    if file_size_mb > max_size_mb:
        raise ValueError(
            f"Dirty JSON file exceeds the supported size limit "
            f"({file_size_mb:.2f} MB > {max_size_mb} MB). "
            f"Please provide a valid JSON file or preprocess the file before Bronze ingestion."
        )

    content = "".join(
        row["value"]
        for row in spark.read.text(path).collect()
    )

    start_idx = content.find("[")
    end_idx = content.rfind("]")

    if start_idx == -1 or end_idx == -1:
        raise ValueError(f"Cannot find JSON array in path: {path}")

    json_text = content[start_idx:end_idx + 1]
    records = json.loads(json_text)

    return spark.createDataFrame(records)


def upsert_file_sessions_not_run(
    spark,
    ctx: RunContext,
    source: SourceConfig,
    table_session_id: str,
    files: list[str]
):
    if not files:
        return

    rows = [(str(uuid.uuid4()), f) for f in files]

    df = spark.createDataFrame(rows, ["new_id", "source_file"])
    df.createOrReplaceTempView("tmp_upsert_files")

    spark.sql(f"""
        MERGE INTO log.audit_file_session AS target
        USING (
            SELECT
                new_id,
                '{ctx.session_id}' AS session_id,
                '{table_session_id}' AS table_session_id,
                {source.id} AS source_table_id,
                {ctx.batch_id} AS batch_id,
                source_file
            FROM tmp_upsert_files
        ) AS source
        ON target.batch_id = source.batch_id
           AND target.source_table_id = source.source_table_id
           AND target.source_file = source.source_file

        WHEN MATCHED AND target.file_status <> 'SUCCESS' THEN
            UPDATE SET
                target.session_id = source.session_id,
                target.table_session_id = source.table_session_id,
                target.file_status = 'NOT_RUN',
                target.error_code = NULL,
                target.error_message = NULL,
                target.started_at = NULL,
                target.completed_at = NULL,
                target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN
            INSERT (
                id,
                session_id,
                table_session_id,
                source_table_id,
                batch_id,
                source_file,
                file_status,
                file_row_count,
                processed_row_count,
                rejected_row_count,
                error_code,
                error_message,
                retry_count,
                started_at,
                completed_at,
                created_at,
                updated_at
            )
            VALUES (
                source.new_id,
                source.session_id,
                source.table_session_id,
                source.source_table_id,
                source.batch_id,
                source.source_file,
                'NOT_RUN',
                NULL,
                NULL,
                0,
                NULL,
                NULL,
                0,
                NULL,
                NULL,
                current_timestamp(),
                current_timestamp()
            )
    """)


def get_files_to_process(
    spark,
    ctx: RunContext,
    source: SourceConfig,
    all_files: list[str]
) -> list[str]:
    if ctx.run_mode == "RECOVERY":
        rows = (
            spark.table("log.audit_file_session")
            .where(
                (F.col("batch_id") == F.lit(ctx.batch_id))
                & (F.col("source_table_id") == F.lit(source.id))
                & (F.col("file_status").isin("FAILED", "RUNNING", "NOT_RUN"))
            )
            .select("source_file")
            .distinct()
            .collect()
        )

        return [row["source_file"] for row in rows]

    success_files = set(
        row["source_file"]
        for row in (
            spark.table("log.audit_file_session")
            .where(
                (F.col("source_table_id") == F.lit(source.id))
                & (F.col("file_status") == F.lit("SUCCESS"))
            )
            .select("source_file")
            .collect()
        )
    )

    return [f for f in all_files if f not in success_files]


def update_file_sessions_running(
    spark,
    ctx: RunContext,
    source: SourceConfig,
    table_session_id: str,
    files_to_process: list[str]
):
    if not files_to_process:
        return

    rows = [(source_file,) for source_file in files_to_process]

    df = spark.createDataFrame(rows, ["source_file"])
    df.createOrReplaceTempView("tmp_running_files")

    spark.sql(f"""
        MERGE INTO log.audit_file_session AS target
        USING (
            SELECT
                {ctx.batch_id} AS batch_id,
                {source.id} AS source_table_id,
                source_file
            FROM tmp_running_files
        ) AS source
        ON target.batch_id = source.batch_id
           AND target.source_table_id = source.source_table_id
           AND target.source_file = source.source_file

        WHEN MATCHED THEN
            UPDATE SET
                target.file_status = 'RUNNING',
                target.session_id = '{ctx.session_id}',
                target.table_session_id = '{table_session_id}',
                target.started_at = current_timestamp(),
                target.updated_at = current_timestamp()
    """)


def bulk_finish_file_sessions(
    spark,
    ctx: RunContext,
    source: SourceConfig,
    table_session_id: str,
    file_results: list[dict]
):
    if not file_results:
        return

    rows = [
        Row(
            source_file=r["source_file"],
            file_status=r["status"],
            file_row_count=r.get("row_count"),
            error_message=r.get("error_message")
        )
        for r in file_results
    ]

    schema = StructType([
        StructField("source_file", StringType(), False),
        StructField("file_status", StringType(), False),
        StructField("file_row_count", LongType(), True),
        StructField("error_message", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema)
    df.createOrReplaceTempView("tmp_file_results")

    spark.sql(f"""
        MERGE INTO log.audit_file_session AS target
        USING (
            SELECT
                '{ctx.session_id}' AS session_id,
                '{table_session_id}' AS table_session_id,
                {source.id} AS source_table_id,
                {ctx.batch_id} AS batch_id,
                source_file,
                file_status,
                file_row_count,
                CASE
                    WHEN file_status = 'SUCCESS' THEN file_row_count
                    ELSE NULL
                END AS processed_row_count,
                0 AS rejected_row_count,
                error_message,
                CASE
                    WHEN file_status = 'FAILED' THEN 'FILE_LOAD_FAILED'
                    ELSE NULL
                END AS error_code
            FROM tmp_file_results
        ) AS source
        ON target.batch_id = source.batch_id
           AND target.source_table_id = source.source_table_id
           AND target.source_file = source.source_file

        WHEN MATCHED THEN
            UPDATE SET
                target.session_id = source.session_id,
                target.table_session_id = source.table_session_id,
                target.file_status = source.file_status,
                target.file_row_count = source.file_row_count,
                target.processed_row_count = source.processed_row_count,
                target.rejected_row_count = source.rejected_row_count,
                target.error_code = source.error_code,
                target.error_message = source.error_message,
                target.completed_at = current_timestamp(),
                target.updated_at = current_timestamp()
    """)


def get_file_session_summary(spark, table_session_id: str):
    summary = (
        spark.table("log.audit_file_session")
        .where(F.col("table_session_id") == F.lit(table_session_id))
        .agg(
            F.sum(F.coalesce(F.col("file_row_count"), F.lit(0))).alias("source_row_count"),
            F.sum(F.coalesce(F.col("processed_row_count"), F.lit(0))).alias("inserted_row"),
            F.sum(F.coalesce(F.col("rejected_row_count"), F.lit(0))).alias("rejected_row"),
            F.sum(F.when(F.col("file_status") == F.lit("FAILED"), 1).otherwise(0)).alias("failed_count")
        )
        .collect()[0]
    )

    failed_rows = (
        spark.table("log.audit_file_session")
        .where(
            (F.col("table_session_id") == F.lit(table_session_id))
            & (F.col("file_status") == F.lit("FAILED"))
        )
        .select("source_file", "error_message")
        .collect()
    )

    error_message = None

    if failed_rows:
        error_message = " | ".join(
            [f"{row['source_file']}: {row['error_message']}" for row in failed_rows]
        )

    failed_count = summary["failed_count"] or 0

    return {
        "status": "FAILED" if failed_count > 0 else "SUCCESS",
        "source_row_count": summary["source_row_count"] or 0,
        "inserted_row": summary["inserted_row"] or 0,
        "rejected_row": summary["rejected_row"] or 0,
        "error_message": error_message
    }


# =========================
# Source processing
# =========================

def format_watermark(value):
    if value is None:
        return None

    try:
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S")
    except:
        pass

    return str(value)

def process_database_source(
    spark,
    ctx: RunContext,
    source: SourceConfig,
    mapping: dict,
    watermark_before
):
    source_df = spark.read.table(source.source_location)

    validate_source_schema(source_df, mapping)

    source_df = apply_load_type_filter(source_df, source, watermark_before)
    source_df.cache()

    try:
        source_row_count = source_df.count()
        if source_row_count == 0:
            watermark_after = watermark_before
        else:
            watermark_after = get_max_watermark(source_df, source)

        bronze_df = apply_source_to_bronze_mapping(
            df=source_df,
            mapping=mapping,
            ctx=ctx,
            source=source
        )

        bronze_df = align_to_target_schema(spark, bronze_df, source.bronze_table_name)

        write_mode = "overwrite" if source.load_type.upper() == "FULL" else "append"

        bronze_df.write.format("delta").mode(write_mode).saveAsTable(source.bronze_table_name)

        target_row_count = source_row_count

    finally:
        source_df.unpersist()

    return {
        "watermark_after": watermark_after,
        "source_row_count": source_row_count,
        "target_row_count": target_row_count,
        "inserted_row": target_row_count,
        "rejected_row": source_row_count - target_row_count
    }


def process_file_source(
    spark,
    ctx: RunContext,
    source: SourceConfig,
    mapping: dict,
    table_session_id: str,
    watermark_before
):
    all_files = list_files(source.source_location)

    if not all_files:
        print(f"No files found in path: {source.source_location}")

        return {
            "watermark_after": None,
            "source_row_count": 0,
            "target_row_count": 0,
            "inserted_row": 0,
            "rejected_row": 0
        }

    files_to_process = get_files_to_process(
        spark=spark,
        ctx=ctx,
        source=source,
        all_files=all_files
    )

    if not files_to_process:
        print(f"No files to process for source: {source.source_name}")
        return {
            "watermark_after": None,
            "source_row_count": 0,
            "target_row_count": 0,
            "inserted_row": 0,
            "rejected_row": 0
        }
    
    upsert_file_sessions_not_run(
        spark=spark,
        ctx=ctx,
        source=source,
        table_session_id=table_session_id,
        files=files_to_process
    )

    update_file_sessions_running(
        spark=spark,
        ctx=ctx,
        source=source,
        table_session_id=table_session_id,
        files_to_process=files_to_process
    )

    file_results = []
    failed_files = []
    total_watermark_max = None

    for source_file in files_to_process:
        current_file_log = {
            "source_file": source_file,
            "status": "RUNNING",
            "row_count": None,
            "error_message": None
        }

        try:
            source_format = source.source_format.lower()

            if source_format == "json":
                source_df = read_dirty_json_file_or_folder(spark, source_file, 50)

            elif source_format == "csv":
                source_df = spark.read.option("header", "true").csv(source_file)

            elif source_format == "parquet":
                source_df = spark.read.parquet(source_file)

            else:
                raise ValueError(f"Unsupported source_format: {source.source_format}")

            validate_source_schema(source_df, mapping)

            source_df = apply_load_type_filter(source_df, source, watermark_before)
            source_df.cache()

            try:
                row_count = source_df.count()

                if row_count == 0:
                    file_watermark_after = watermark_before
                else:
                    file_watermark_after = get_max_watermark(source_df, source)

                if file_watermark_after is not None:
                    if total_watermark_max is None or file_watermark_after > total_watermark_max:
                        total_watermark_max = file_watermark_after

                relative_source_file = get_relative_source_file(source_file)

                bronze_df = apply_source_to_bronze_mapping(
                    df=source_df,
                    mapping=mapping,
                    ctx=ctx,
                    source=source,
                    source_file=relative_source_file
                )

                bronze_df = align_to_target_schema(spark, bronze_df, source.bronze_table_name)

                write_mode = "overwrite" if source.load_type.upper() == "FULL" else "append"

                bronze_df.write.format("delta").mode(write_mode).saveAsTable(source.bronze_table_name)

                current_file_log["status"] = "SUCCESS"
                current_file_log["row_count"] = row_count

            finally:
                source_df.unpersist()

        except Exception as e:
            current_file_log["status"] = "FAILED"
            current_file_log["error_message"] = str(e)
            failed_files.append(source_file)

        file_results.append(current_file_log)

    bulk_finish_file_sessions(
        spark=spark,
        ctx=ctx,
        source=source,
        table_session_id=table_session_id,
        file_results=file_results
    )

    summary_metrics = get_file_session_summary(spark, table_session_id)

    if failed_files:
        raise Exception(f"Failed files: {failed_files}")

    return {
        "watermark_after": total_watermark_max,
        "source_row_count": summary_metrics["source_row_count"],
        "target_row_count": summary_metrics["inserted_row"],
        "inserted_row": summary_metrics["inserted_row"],
        "rejected_row": summary_metrics["rejected_row"]
    }


# =========================
# Main source runner
# =========================

def run_source_to_bronze(spark, ctx: RunContext, source: SourceConfig):
    table_session_id, bronze_status, audit_watermark_before = get_table_session_id(
        spark=spark,
        ctx=ctx,
        source=source
    )

    watermark_before = read_watermark(spark, source.id)

    if watermark_before is None:
        watermark_before = audit_watermark_before

    if ctx.run_mode == "RECOVERY" and bronze_status == "SKIPPED":
        return {
            "source_table_id": source.id,
            "table_session_id": table_session_id,
            "status": "SKIPPED",
            "watermark_before": format_watermark(watermark_before),
            "watermark_after": format_watermark(watermark_before),
            "source_row_count": 0,
            "target_row_count": 0,
            "inserted_row": 0,
            "rejected_row": 0,
            "error_message": None
        }

    try:
        mapping = read_mapping(source.source_to_bronze_mapping_path)

        if source.source_type.lower() == "database":
            execution_result = process_database_source(
                spark=spark,
                ctx=ctx,
                source=source,
                mapping=mapping,
                watermark_before=watermark_before
            )

        elif source.source_type.lower() == "file":
            execution_result = process_file_source(
                spark=spark,
                ctx=ctx,
                source=source,
                mapping=mapping,
                table_session_id=table_session_id,
                watermark_before=watermark_before
            )

        else:
            raise ValueError(f"Unsupported source_type: {source.source_type}")

        watermark_after = execution_result["watermark_after"]

        if watermark_after is None:
            watermark_after = watermark_before

        if source.watermark_column and watermark_after is not None:
            update_watermark(spark, source.id, watermark_after)

        return {
            "source_table_id": source.id,
            "table_session_id": table_session_id,
            "status": "SUCCESS",
            "watermark_before": format_watermark(watermark_before),
            "watermark_after": format_watermark(watermark_after),
            "source_row_count": execution_result["source_row_count"],
            "target_row_count": execution_result["target_row_count"],
            "inserted_row": execution_result["inserted_row"],
            "rejected_row": execution_result["rejected_row"],
            "error_message": None
        }

    except Exception as e:
        error_message = str(e)

        if source.source_type.lower() == "file":
            summary = get_file_session_summary(spark, table_session_id)

            return {
                "source_table_id": source.id,
                "table_session_id": table_session_id,
                "status": "FAILED",
                "watermark_before": format_watermark(watermark_before),
                "watermark_after": None,
                "source_row_count": summary["source_row_count"],
                "target_row_count": 0,
                "inserted_row": summary["inserted_row"],
                "rejected_row": summary["rejected_row"],
                "error_message": summary["error_message"] or error_message
            }

        return {
            "source_table_id": source.id,
            "table_session_id": table_session_id,
            "status": "FAILED",
            "watermark_before": format_watermark(watermark_before),
            "watermark_after": None,
            "source_row_count": 0,
            "target_row_count": 0,
            "inserted_row": 0,
            "rejected_row": 0,
            "error_message": error_message
        }


# =========================
# Sequential runner
# =========================

def run_all_sources(
    spark,
    ctx: RunContext,
    source_configs: list[SourceConfig]
):
    results = []

    for source in source_configs:
        result = run_source_to_bronze(
            spark=spark,
            ctx=ctx,
            source=source
        )
        results.append(result)

    bulk_update_audit_table_session(spark, results)
    bulk_insert_audit_detail(spark, results)

    return results


# =========================
# Layer gate
# =========================

def update_next_run_mode_recovery(spark, batch_id: int, session_id: str):
    spark.sql(f"""
        UPDATE cfg.next_run_mode
        SET next_run_mode = 'RECOVERY',
            batch_id = {batch_id},
            session_id = '{session_id}',
            updated_at = current_timestamp()
    """)


def update_audit_session_failed(spark, session_id: str, error_message: str):

    spark.sql(f"""
        UPDATE log.audit_session
        SET session_status = 'FAILED',
            session_finished = current_timestamp(),
            updated_at = current_timestamp()
        WHERE id = '{session_id}'
    """)


def run_layer_gate(spark, layer: str, ctx: RunContext):
    audit_df = (
        spark.table("log.audit_table_session")
        .where(
            (F.col("session_id") == F.lit(ctx.session_id))
            & (F.col("batch_id") == F.lit(ctx.batch_id))
        )
    )

    status_col = f"{layer.lower()}_status"

    invalid_count = (
        audit_df
        .where(
            F.col(status_col).isNull()
            | ~F.col(status_col).isin("SUCCESS", "SKIPPED")
        )
        .count()
    )

    if invalid_count > 0:
        error_message = (
            f"{layer} gate failed. "
            f"{invalid_count} table(s) are not SUCCESS/SKIPPED."
        )

        update_next_run_mode_recovery(spark, ctx.batch_id, ctx.session_id)
        update_audit_session_failed(spark, ctx.session_id, error_message)

        raise Exception(error_message)

    total_target_rows = (
        spark.table("log.audit_detail").alias("d")
        .join(
            F.broadcast(audit_df.select("id").alias("t")),
            F.col("d.table_session_id") == F.col("t.id"),
            "inner"
        )
        .where(
            (F.col("d.layer") == F.lit(layer))
            & (F.col("d.detail_status").isin("SUCCESS", "SKIPPED"))
        )
        .agg(F.coalesce(F.sum("d.target_row_count"), F.lit(0)).alias("total_target_rows"))
        .collect()[0]["total_target_rows"]
    )

    if total_target_rows == 0:
        print(
            f"{layer} gate stopped. "
            f"All tables are SUCCESS/SKIPPED, but total target_row_count = 0."
        )
        return False

    print(f"{layer} gate passed. Total target_row_count = {total_target_rows}.")
    return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ctx = RunContext(
    session_id=session_id,
    batch_id=batch_id,
    run_mode=run_mode
)

source_configs = [
    SourceConfig(
        id=row["id"],
        source_system=row["source_system"],
        source_type=row["source_type"],
        source_name=row["source_name"],
        source_location=row["source_location"],
        source_format=row["source_format"],
        load_type=row["load_type"],
        watermark_column=row["watermark_column"],
        source_to_bronze_mapping_path=row["source_to_bronze_mapping_path"],
        bronze_table_name=row["bronze_table_name"]
    )
    for row in source_table_values
]

results = run_all_sources(
    spark=spark,
    ctx=ctx,
    source_configs=source_configs
)

should_continue = run_layer_gate(
    spark=spark,
    layer="BRONZE",
    ctx=ctx
)

if not should_continue:
    spark.sql(f"""
        UPDATE log.audit_session
        SET session_status = 'SUCCESS',
            session_finished = current_timestamp(),
            updated_at = current_timestamp()
        WHERE id = '{ctx.session_id}'
    """)

    print("No data loaded in Bronze. Stop before Silver.")
    mssparkutils.notebook.exit("NO_DATA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
