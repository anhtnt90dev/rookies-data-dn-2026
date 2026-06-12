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

# PARAMETERS CELL ********************

session_id = ""
batch_id = ""
previous_session_id = ""
source_table_value = ""
run_mode = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(source_table_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import re
import uuid
from enum import Enum

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
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
    source_list_json: str,
    run_mode: str = RunMode.NEW,
    recovery_session_id: str = None,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
) -> None:
    audit_table_session_table = validate_table_name(audit_table_session_table)
    run_mode = require_status(run_mode, [RunMode.NEW, RunMode.RECOVERY])

    sources = json.loads(source_list_json)

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
                load_type=source.get("load_type"),
                source_table_name=source.get("source_name"),
                watermark_column=source.get("watermark_column"),
                watermark_before=source.get("watermark_before")
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
    source_list_json=source_table_value,
    run_mode = RunMode.NEW if run_mode == "NEW" else RunMode.RECOVERY,
    recovery_session_id = previous_session_id if previous_session_id else None,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
