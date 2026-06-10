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

# Reusable audit logging helpers for Microsoft Fabric notebooks.
import re
import time
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

class FileStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


VALID_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")

AUDIT_SESSION_TABLE = "log.audit_session"
AUDIT_TABLE_SESSION_TABLE = "log.audit_table_session"
AUDIT_DETAIL_TABLE = "log.audit_detail"
AUDIT_FILE_SESSION_TABLE = "log.audit_file_session"
RETRY_LOG_TABLE = "log.retry_log"
INVALID_RECORD_TABLE = "log.invalid_record"
RETRY_POLICY_TABLE = "cfg.retry_policy"
NEXT_RUN_MODE_TABLE = "cfg.next_run_mode"


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


def get_single_id(table_name: str, filters: dict) -> str:
    table_name = validate_table_name(table_name)
    result_df = spark.table(table_name)

    for column_name, value in filters.items():
        result_df = result_df.where(F.col(column_name) == F.lit(value))

    rows = result_df.select("id").limit(2).collect()

    if len(rows) == 0:
        raise Exception(f"No record found in {table_name} for filters={filters}")

    if len(rows) > 1:
        raise Exception(f"Multiple records found in {table_name} for filters={filters}")

    return str(rows[0]["id"])


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


def get_next_retry_attempt_no(
    retry_log_table: str,
    table_session_id: str,
    layer: str,
    file_session_id: str = None,
) -> int:
    retry_log_table = validate_table_name(retry_log_table)
    layer = require_layer(layer)
    result_df = (
        spark.table(retry_log_table)
        .where((F.col("table_session_id") == F.lit(table_session_id)) & (F.col("layer") == F.lit(layer)))
    )

    if file_session_id:
        result_df = result_df.where(F.col("file_session_id") == F.lit(file_session_id))
    else:
        result_df = result_df.where(F.col("file_session_id").isNull())

    result = result_df.agg(F.coalesce(F.max("attempt_no"), F.lit(0)).alias("max_attempt_no")).collect()[0]["max_attempt_no"]
    return int(result) + 1

def get_file_session_id(
    batch_id: int,
    source_table_id: int,
    source_file: str,
    audit_file_session_table: str = AUDIT_FILE_SESSION_TABLE,
):
    audit_file_session_table = validate_table_name(audit_file_session_table)

    rows = (
        spark.table(audit_file_session_table)
        .where(
            (F.col("batch_id") == F.lit(int(batch_id)))
            & (F.col("source_table_id") == F.lit(int(source_table_id)))
            & (F.col("source_file") == F.lit(source_file))
        )
        .select("id")
        .limit(2)
        .collect()
    )

    if len(rows) == 0:
        return None
    if len(rows) > 1:
        raise Exception(
            f"Multiple file sessions found for batch_id={batch_id}, "
            f"source_table_id={source_table_id}, source_file={source_file}"
        )
    return str(rows[0]["id"])


def should_process_file(
    batch_id: int,
    source_table_id: int,
    source_file: str,
    audit_file_session_table: str = AUDIT_FILE_SESSION_TABLE,
) -> bool:
    audit_file_session_table = validate_table_name(audit_file_session_table)

    existing_file = (
        spark.table(audit_file_session_table)
        .where(
            (F.col("batch_id") == F.lit(int(batch_id)))
            & (F.col("source_table_id") == F.lit(int(source_table_id)))
            & (F.col("source_file") == F.lit(source_file))
        )
        .select("file_status")
        .limit(1)
        .collect()
    )

    if not existing_file:
        return True

    return existing_file[0]["file_status"] != AuditStatus.SUCCESS.value


def get_failed_or_missing_files(
    batch_id: int,
    source_table_id: int,
    source_files,
    audit_file_session_table: str = AUDIT_FILE_SESSION_TABLE,
) -> list:
    """Return source files that need processing for recovery: missing, failed, or incomplete."""
    audit_file_session_table = validate_table_name(audit_file_session_table)
    files_to_process = []

    for source_file in source_files:
        existing_file = (
            spark.table(audit_file_session_table)
            .where(
                (F.col("batch_id") == F.lit(int(batch_id)))
                & (F.col("source_table_id") == F.lit(int(source_table_id)))
                & (F.col("source_file") == F.lit(source_file))
            )
            .select("id", "file_status")
            .limit(1)
            .collect()
        )

        if not existing_file:
            files_to_process.append({
                "source_file": source_file,
                "reason": "MISSING",
                "file_session_id": None,
            })
            continue

        file_status = existing_file[0]["file_status"]
        if file_status != AuditStatus.SUCCESS.value:
            files_to_process.append({
                "source_file": source_file,
                "reason": file_status or "INCOMPLETE",
                "file_session_id": str(existing_file[0]["id"]),
            })

    return files_to_process


def should_process_table_layer(
    batch_id: int,
    source_table_id: int,
    layer: str,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
) -> bool:
    """Return False when a prior session already completed the batch/source/layer successfully."""
    audit_table_session_table = validate_table_name(audit_table_session_table)
    layer = require_layer(layer)
    layer_status_column = {
        Layer.BRONZE.value: "bronze_status",
        Layer.SILVER.value: "silver_status",
        Layer.GOLD.value: "gold_status",
    }[layer]

    successful_rows = (
        spark.table(audit_table_session_table)
        .where(
            (F.col("batch_id") == F.lit(int(batch_id)))
            & (F.col("source_table_id") == F.lit(int(source_table_id)))
            & (F.col(layer_status_column) == F.lit(AuditStatus.SUCCESS.value))
        )
        .limit(1)
        .collect()
    )

    return len(successful_rows) == 0


def get_recovery_table_plan(
    batch_id: int,
    source_table_ids,
    layer: str,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
) -> list:
    layer = require_layer(layer)
    return [
        {
            "source_table_id": int(source_table_id),
            "should_process": should_process_table_layer(
                batch_id=batch_id,
                source_table_id=int(source_table_id),
                layer=layer,
                audit_table_session_table=audit_table_session_table,
            ),
        }
        for source_table_id in source_table_ids
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def start_file_session(
    session_id: str,
    table_session_id: str,
    source_table_id: int,
    batch_id: int,
    source_file: str,
    file_row_count: int = None,
    audit_file_session_table: str = AUDIT_FILE_SESSION_TABLE,
) -> str:
    audit_file_session_table = validate_table_name(audit_file_session_table)

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("table_session_id", StringType(), False),
        StructField("source_table_id", LongType(), False),
        StructField("batch_id", LongType(), False),
        StructField("source_file", StringType(), False),
        StructField("file_row_count", IntegerType(), True),
    ])

    source_view = create_temp_view_from_rows([
        Row(
            id=new_audit_id(),
            session_id=str(session_id),
            table_session_id=str(table_session_id),
            source_table_id=int(source_table_id),
            batch_id=int(batch_id),
            source_file=source_file,
            file_row_count=file_row_count,
        )
    ], schema, "audit_file_session")

    spark.sql(f"""
        MERGE INTO {audit_file_session_table} AS target
        USING (
            SELECT
                id,
                session_id,
                table_session_id,
                source_table_id,
                batch_id,
                source_file,
                '{AuditStatus.RUNNING.value}' AS file_status,
                file_row_count,
                CAST(NULL AS INT) AS processed_row_count,
                CAST(NULL AS INT) AS rejected_row_count,
                CAST(NULL AS STRING) AS error_code,
                CAST(NULL AS STRING) AS error_message,
                CAST(NULL AS STRING) AS error_type,
                CAST(NULL AS BOOLEAN) AS is_retryable,
                0 AS retry_count,
                CAST(NULL AS TIMESTAMP) AS last_retry_at,
                current_timestamp() AS started_at,
                CAST(NULL AS TIMESTAMP) AS completed_at,
                CAST(NULL AS BIGINT) AS duration_ms,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
            FROM {source_view}
        ) AS source
        ON target.batch_id = source.batch_id
           AND target.source_table_id = source.source_table_id
           AND target.source_file = source.source_file
        WHEN MATCHED THEN UPDATE SET
            target.session_id = source.session_id,
            target.table_session_id = source.table_session_id,
            target.file_status = source.file_status,
            target.file_row_count = source.file_row_count,
            target.error_code = NULL,
            target.error_message = NULL,
            target.error_type = NULL,
            target.is_retryable = NULL,
            target.started_at = source.started_at,
            target.completed_at = NULL,
            target.duration_ms = NULL,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    file_session_id = get_file_session_id(batch_id, source_table_id, source_file, audit_file_session_table)
    print(f"Started file session: file_session_id={file_session_id}, source_file={source_file}")
    return file_session_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def finish_file_session(
    file_session_id: str,
    status: str,
    processed_row_count: int = None,
    rejected_row_count: int = None,
    error_code: str = None,
    error_message: str = None,
    error_type: str = None,
    is_retryable: bool = None,
    audit_file_session_table: str = AUDIT_FILE_SESSION_TABLE,
):
    audit_file_session_table = validate_table_name(audit_file_session_table)
    status = require_status(status, [AuditStatus.SUCCESS, AuditStatus.FAILED, AuditStatus.SKIPPED])

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("file_status", StringType(), False),
        StructField("processed_row_count", IntegerType(), True),
        StructField("rejected_row_count", IntegerType(), True),
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("is_retryable", BooleanType(), True),
    ])

    source_view = create_temp_view_from_rows([
        Row(
            id=str(file_session_id),
            file_status=status,
            processed_row_count=processed_row_count,
            rejected_row_count=rejected_row_count,
            error_code=error_code,
            error_message=error_message,
            error_type=enum_value(error_type) if error_type is not None else None,
            is_retryable=is_retryable,
        )
    ], schema, "finish_file_session")

    spark.sql(f"""
        MERGE INTO {audit_file_session_table} AS target
        USING (
            SELECT *, current_timestamp() AS completed_at
            FROM {source_view}
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.file_status = source.file_status,
            target.processed_row_count = source.processed_row_count,
            target.rejected_row_count = source.rejected_row_count,
            target.error_code = source.error_code,
            target.error_message = source.error_message,
            target.error_type = source.error_type,
            target.is_retryable = source.is_retryable,
            target.completed_at = source.completed_at,
            target.duration_ms = CAST(
                (unix_timestamp(source.completed_at) - unix_timestamp(target.started_at)) * 1000 AS BIGINT
            ),
            target.updated_at = source.completed_at
    """)

    print(f"Finished file session: file_session_id={file_session_id}, status={status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_retry_attempt(
    table_session_id: str,
    layer: str,
    status: str,
    file_session_id: str = None,
    error_code: str = None,
    error_message: str = None,
    error_type: str = None,
    is_retryable: bool = True,
    retry_log_table: str = RETRY_LOG_TABLE,
    audit_table_session_table: str = AUDIT_TABLE_SESSION_TABLE,
    audit_file_session_table: str = AUDIT_FILE_SESSION_TABLE,
):
    retry_log_table = validate_table_name(retry_log_table)
    audit_table_session_table = validate_table_name(audit_table_session_table)
    audit_file_session_table = validate_table_name(audit_file_session_table)
    layer = require_layer(layer)
    status = require_status(status, [AuditStatus.RUNNING, AuditStatus.SUCCESS, AuditStatus.FAILED])

    attempt_no = get_next_retry_attempt_no(retry_log_table, str(table_session_id), layer, file_session_id)

    row = Row(
        id=new_audit_id(),
        table_session_id=str(table_session_id),
        file_session_id=str(file_session_id) if file_session_id else None,
        attempt_no=attempt_no,
        layer=layer,
        status=status,
        error_code=error_code,
        error_message=error_message,
        error_type=enum_value(error_type) if error_type is not None else None,
        is_retryable=is_retryable,
    )

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("table_session_id", StringType(), False),
        StructField("file_session_id", StringType(), True),
        StructField("attempt_no", IntegerType(), True),
        StructField("layer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("is_retryable", BooleanType(), True),
    ])

    retry_df = (
        spark.createDataFrame([row], schema)
        .withColumn("started_at", F.current_timestamp())
        .withColumn("ended_at", F.current_timestamp())
        .withColumn("duration_ms", F.lit(None).cast("bigint"))
        .withColumn("created_at", F.current_timestamp())
    )

    retry_df.write.format("delta").mode("append").saveAsTable(retry_log_table)

    spark.sql(f"""
        UPDATE {audit_table_session_table}
        SET retry_count = COALESCE(retry_count, 0) + 1,
            last_retry_at = current_timestamp(),
            updated_at = current_timestamp()
        WHERE id = '{str(table_session_id)}'
    """)

    if file_session_id:
        spark.sql(f"""
            UPDATE {audit_file_session_table}
            SET retry_count = COALESCE(retry_count, 0) + 1,
                last_retry_at = current_timestamp(),
                updated_at = current_timestamp()
            WHERE id = '{str(file_session_id)}'
        """)


def parse_error_type_list(error_types) -> list:
    if error_types is None:
        return []

    return [
        item.strip().upper()
        for item in str(error_types).split(",")
        if item and item.strip()
    ]


def get_active_retry_policy(
    layer: str = None,
    source_table_id: int = None,
    retry_policy_table: str = RETRY_POLICY_TABLE,
) -> dict:
    """Return the active retry policy. Layer/source parameters are reserved for future scoped policies."""
    retry_policy_table = validate_table_name(retry_policy_table)

    policy_rows = (
        spark.table(retry_policy_table)
        .where(F.col("is_active") == F.lit(True))
        .orderBy(F.col("id"))
        .limit(1)
        .collect()
    )

    if not policy_rows:
        raise Exception(f"No active retry policy found in {retry_policy_table}")

    policy = policy_rows[0].asDict()
    return {
        "id": policy["id"],
        "policy_name": policy["policy_name"],
        "max_retry_count": int(policy["max_retry_count"]),
        "retry_delay_seconds": int(policy["retry_delay_seconds"]),
        "backoff_strategy": str(policy["backoff_strategy"]).upper(),
        "retryable_error_types": parse_error_type_list(policy["retryable_error_types"]),
        "non_retryable_error_types": parse_error_type_list(policy["non_retryable_error_types"]),
    }


def is_retryable_error(error_type, retry_policy: dict) -> bool:
    error_type_value = enum_value(error_type)

    if error_type_value in retry_policy.get("retryable_error_types", []):
        return True

    if error_type_value in retry_policy.get("non_retryable_error_types", []):
        return False

    return error_type_value == ErrorType.SYSTEM.value


def get_retry_delay_seconds(retry_policy: dict, retry_attempt_no: int) -> int:
    base_delay = int(retry_policy.get("retry_delay_seconds", 0))
    backoff_strategy = str(retry_policy.get("backoff_strategy", "FIXED_DELAY")).upper()

    if backoff_strategy == "EXPONENTIAL":
        return base_delay * (2 ** max(retry_attempt_no - 1, 0))

    return base_delay


def get_error_attribute(error, attribute_name: str, default_value=None):
    if hasattr(error, attribute_name):
        return getattr(error, attribute_name)
    return default_value


def run_with_retry(
    operation_fn,
    table_session_id: str,
    layer: str,
    operation_name: str,
    error_type: str = ErrorType.SYSTEM,
    policy: dict = None,
    file_session_id: str = None,
    is_final_table_step: bool = True,
    apply_retry_delay: bool = False,
):
    """Run a controlled operation with audited retries. This is a framework helper, not production orchestration."""
    layer = require_layer(layer)
    retry_policy = policy or get_active_retry_policy()
    retry_attempt_count = 0

    while True:
        try:
            result = operation_fn()
            finish_table_layer(
                table_session_id=table_session_id,
                layer=layer,
                status=AuditStatus.SUCCESS,
                is_final_table_step=is_final_table_step,
                write_detail=True,
            )
            return result
        except Exception as error:
            current_error_type = enum_value(get_error_attribute(error, "error_type", error_type))
            current_error_code = get_error_attribute(
                error,
                "error_code",
                f"{operation_name.upper()}_{current_error_type}_ERROR",
            )
            current_error_message = str(error)[:1000]
            should_retry = is_retryable_error(current_error_type, retry_policy)

            if (not should_retry) or retry_attempt_count >= retry_policy["max_retry_count"]:
                finish_table_layer(
                    table_session_id=table_session_id,
                    layer=layer,
                    status=AuditStatus.FAILED,
                    is_final_table_step=is_final_table_step,
                    error_code=current_error_code,
                    error_message=current_error_message,
                    error_type=current_error_type,
                    is_retryable=should_retry,
                    write_detail=True,
                )
                raise

            retry_attempt_count += 1
            log_retry_attempt(
                table_session_id=table_session_id,
                file_session_id=file_session_id,
                layer=layer,
                status=AuditStatus.FAILED,
                error_code=current_error_code,
                error_message=current_error_message,
                error_type=current_error_type,
                is_retryable=True,
            )

            if apply_retry_delay:
                time.sleep(get_retry_delay_seconds(retry_policy, retry_attempt_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_invalid_record(
    table_session_id: str,
    layer: str,
    target_table: str,
    record_key: str,
    raw_data: str,
    error_reason: str,
    file_session_id: str = None,
    error_column: str = None,
    error_type: str = ErrorType.DATA,
    is_retryable: bool = False,
    invalid_record_table: str = INVALID_RECORD_TABLE,
):
    invalid_record_table = validate_table_name(invalid_record_table)
    layer = require_layer(layer)

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("table_session_id", StringType(), False),
        StructField("file_session_id", StringType(), True),
        StructField("layer", StringType(), False),
        StructField("target_table", StringType(), True),
        StructField("record_key", StringType(), True),
        StructField("raw_data", StringType(), True),
        StructField("error_column", StringType(), True),
        StructField("error_reason", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("is_retryable", BooleanType(), True),
    ])

    invalid_df = (
        spark.createDataFrame([
            Row(
                id=new_audit_id(),
                table_session_id=str(table_session_id),
                file_session_id=str(file_session_id) if file_session_id else None,
                layer=layer,
                target_table=target_table,
                record_key=record_key,
                raw_data=raw_data,
                error_column=error_column,
                error_reason=error_reason,
                error_type=enum_value(error_type),
                is_retryable=is_retryable,
            )
        ], schema)
        .withColumn("created_at", F.current_timestamp())
    )

    invalid_df.write.format("delta").mode("append").saveAsTable(invalid_record_table)


def log_invalid_records_from_dataframe(
    invalid_df,
    table_session_id: str,
    layer: str,
    target_table: str,
    record_key_column: str,
    raw_data_column: str,
    error_reason_column: str,
    file_session_id: str = None,
    error_column_column: str = None,
    error_type: str = ErrorType.DATA,
    is_retryable: bool = False,
    invalid_record_table: str = INVALID_RECORD_TABLE,
):
    """Append invalid records from a dataframe using the existing log.invalid_record schema."""
    invalid_record_table = validate_table_name(invalid_record_table)
    layer = require_layer(layer)

    selected_df = invalid_df.select(
        F.expr("uuid()").alias("id"),
        F.lit(str(table_session_id)).alias("table_session_id"),
        F.lit(str(file_session_id) if file_session_id else None).cast("string").alias("file_session_id"),
        F.lit(layer).alias("layer"),
        F.lit(target_table).alias("target_table"),
        F.col(record_key_column).cast("string").alias("record_key"),
        F.col(raw_data_column).cast("string").alias("raw_data"),
        (
            F.col(error_column_column).cast("string")
            if error_column_column
            else F.lit(None).cast("string")
        ).alias("error_column"),
        F.col(error_reason_column).cast("string").alias("error_reason"),
        F.lit(enum_value(error_type)).alias("error_type"),
        F.lit(bool(is_retryable)).alias("is_retryable"),
        F.current_timestamp().alias("created_at"),
    )

    selected_df.write.format("delta").mode("append").saveAsTable(invalid_record_table)

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

    schema = StructType([
        StructField("id", StringType(), False),
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
        ON target.pipeline_run_id = source.pipeline_run_id
        WHEN MATCHED THEN UPDATE SET
            target.session_status = source.session_status,
            target.session_started = source.session_started,
            target.session_finished = NULL,
            target.duration_ms = NULL,
            target.sla_target_ms = source.sla_target_ms,
            target.sla_breached = NULL,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, session_status, run_mode, batch_id,
            pipeline_name, pipeline_run_id, session_started,
            session_finished, duration_ms, sla_target_ms,
            sla_breached, created_at, updated_at
        ) VALUES (
            source.id, source.session_status, source.run_mode, source.batch_id,
            source.pipeline_name, source.pipeline_run_id, source.session_started,
            source.session_finished, source.duration_ms, source.sla_target_ms,
            source.sla_breached, source.created_at, source.updated_at
        )
    """)

    session_id = get_single_id(audit_session_table, {"pipeline_run_id": pipeline_run_id})
    print(f"Started pipeline session: session_id={session_id}, pipeline_run_id={pipeline_run_id}")
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


def ensure_next_run_mode_table(next_run_mode_table: str = NEXT_RUN_MODE_TABLE):
    next_run_mode_table = validate_table_name(next_run_mode_table)
    spark.sql("CREATE SCHEMA IF NOT EXISTS cfg")
    
    # Safely migrate existing BIGINT column to STRING by dropping the table if the schema is incorrect.
    try:
        schema = spark.table(next_run_mode_table).schema
        if any(f.name == "session_id" and isinstance(f.dataType, LongType) for f in schema.fields):
            spark.sql(f"DROP TABLE IF EXISTS {next_run_mode_table}")
            print(f"Dropped {next_run_mode_table} to migrate session_id to STRING")
    except Exception:
        pass

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {next_run_mode_table} (
            next_run_mode STRING,
            batch_id BIGINT,
            session_id STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)


def set_next_run_mode(
    next_run_mode: str,
    batch_id: int = None,
    session_id: str = None,
    next_run_mode_table: str = NEXT_RUN_MODE_TABLE,
):
    """
    Store the singleton next-run context used by framework recovery simulations.
    - batch_id is the durable recovery key.
    - session_id stores the failed/previous audit session UUID for recovery lineage.
    """
    next_run_mode_table = validate_table_name(next_run_mode_table)
    next_run_mode = require_status(next_run_mode, [RunMode.NEW, RunMode.RECOVERY])
    ensure_next_run_mode_table(next_run_mode_table)

    schema = StructType([
        StructField("next_run_mode", StringType(), False),
        StructField("batch_id", LongType(), True),
        StructField("session_id", StringType(), True),
    ])
    next_run_df = (
        spark.createDataFrame([
            Row(
                next_run_mode=next_run_mode,
                batch_id=int(batch_id) if batch_id is not None else None,
                session_id=str(session_id) if session_id is not None else None,
            )
        ], schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    spark.sql(f"DELETE FROM {next_run_mode_table}")
    next_run_df.write.format("delta").mode("append").saveAsTable(next_run_mode_table)


def reset_next_run_mode(next_run_mode_table: str = NEXT_RUN_MODE_TABLE):
    set_next_run_mode(RunMode.NEW, None, None, next_run_mode_table)


def get_next_batch_id(audit_session_table: str = AUDIT_SESSION_TABLE) -> int:
    audit_session_table = validate_table_name(audit_session_table)
    try:
        current_max = (
            spark.table(audit_session_table)
            .agg(F.coalesce(F.max("batch_id"), F.lit(0)).alias("max_batch_id"))
            .collect()[0]["max_batch_id"]
        )
        return int(current_max) + 1
    except Exception:
        return int(time.time() * 1000)


def get_next_run_context(
    requested_run_mode: str = None,
    next_run_mode_table: str = NEXT_RUN_MODE_TABLE,
) -> dict:
    ensure_next_run_mode_table(next_run_mode_table)
    requested_mode = require_status(requested_run_mode, [RunMode.NEW, RunMode.RECOVERY]) if requested_run_mode else None

    rows = (
        spark.table(next_run_mode_table)
        .select("next_run_mode", "batch_id", "session_id")
        .limit(1)
        .collect()
    )

    stored_context = rows[0].asDict() if rows else {
        "next_run_mode": RunMode.NEW.value,
        "batch_id": None,
        "session_id": None,
    }

    run_mode = requested_mode or enum_value(stored_context.get("next_run_mode") or RunMode.NEW)
    run_mode = require_status(run_mode, [RunMode.NEW, RunMode.RECOVERY])

    batch_id = stored_context.get("batch_id")
    if run_mode == RunMode.RECOVERY.value and batch_id is None:
        raise Exception("RECOVERY run requires cfg.next_run_mode.batch_id")

    return {
        "run_mode": run_mode,
        "batch_id": int(batch_id) if batch_id is not None else None,
        "previous_session_id": stored_context.get("session_id"),
    }


def initialize_run_context(
    pipeline_name: str,
    pipeline_run_id: str,
    requested_run_mode: str = None,
    sla_target_ms: int = None,
    next_run_mode_table: str = NEXT_RUN_MODE_TABLE,
    audit_session_table: str = AUDIT_SESSION_TABLE,
) -> dict:
    """Create a fresh audit session and select the correct logical batch for NEW or RECOVERY."""
    context = get_next_run_context(requested_run_mode, next_run_mode_table)

    if context["run_mode"] == RunMode.NEW.value:
        batch_id = get_next_batch_id(audit_session_table)
    else:
        batch_id = context["batch_id"]

    session_id = start_pipeline_session(
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        run_mode=context["run_mode"],
        sla_target_ms=sla_target_ms,
        audit_session_table=audit_session_table,
    )

    return {
        "run_mode": context["run_mode"],
        "batch_id": batch_id,
        "session_id": session_id,
        "previous_session_id": context.get("previous_session_id"),
        "pipeline_name": pipeline_name,
        "pipeline_run_id": pipeline_run_id,
    }


def mark_recovery_required(
    batch_id: int,
    failed_layer: str,
    failed_table: str = None,
    failed_file: str = None,
    error_code: str = None,
    error_message: str = None,
    session_id: str = None,
    next_run_mode_table: str = NEXT_RUN_MODE_TABLE,
):
    """Set the next run to RECOVERY for the failed logical batch."""
    require_layer(failed_layer)
    set_next_run_mode(RunMode.RECOVERY, batch_id, session_id, next_run_mode_table)
    print({
        "next_run_mode": RunMode.RECOVERY.value,
        "batch_id": int(batch_id),
        "session_id": str(session_id) if session_id else None,
        "failed_layer": enum_value(failed_layer),
        "failed_table": failed_table,
        "failed_file": failed_file,
        "error_code": error_code,
        "error_message": error_message,
    })


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
    layer_ended_column = {Layer.BRONZE.value: "bronze_ended_at", Layer.SILVER.value: "silver_ended_at", Layer.GOLD.value: "gold_ended_at"}[layer]
    layer_status_column = {Layer.BRONZE.value: "bronze_status", Layer.SILVER.value: "silver_status", Layer.GOLD.value: "gold_status"}[layer]

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("source_table_id", LongType(), False),
        StructField("source_table_name", StringType(), False),
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
            source_table_id=int(source_table_id),
            source_table_name=source_table_name,
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
                id, session_id, source_table_id, source_table_name, batch_id,
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
                CAST(NULL AS STRING) AS error_code,
                CAST(NULL AS STRING) AS error_message,
                0 AS retry_count,
                CAST(NULL AS TIMESTAMP) AS last_retry_at,
                CAST(NULL AS BIGINT) AS duration_ms,
                sla_target_ms,
                CAST(NULL AS BOOLEAN) AS sla_breached,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at
            FROM {source_view}
        ) AS source
        ON target.session_id = source.session_id
           AND target.source_table_id = source.source_table_id
        WHEN MATCHED THEN UPDATE SET
            target.table_session_status = source.table_session_status,
            target.{layer_status_column} = source.table_session_status,
            target.{layer_started_column} = source.updated_at,
            target.{layer_ended_column} = NULL,
            target.error_code = NULL,
            target.error_message = NULL,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, session_id, source_table_id, batch_id, table_session_status,
            bronze_status, silver_status, gold_status, load_type, watermark_column,
            watermark_before, watermark_after, load_window_start, load_window_end,
            bronze_started_at, silver_started_at, gold_started_at,
            bronze_ended_at, silver_ended_at, gold_ended_at,
            error_code, error_message,
            retry_count, last_retry_at, duration_ms, sla_target_ms, sla_breached,
            created_at, updated_at, source_table_name
        ) VALUES (
            source.id, source.session_id, source.source_table_id, source.batch_id, source.table_session_status,
            CASE WHEN '{layer}' = '{Layer.BRONZE.value}' THEN '{AuditStatus.RUNNING.value}' ELSE source.bronze_status END,
            CASE WHEN '{layer}' = '{Layer.SILVER.value}' THEN '{AuditStatus.RUNNING.value}' ELSE source.silver_status END,
            CASE WHEN '{layer}' = '{Layer.GOLD.value}' THEN '{AuditStatus.RUNNING.value}' ELSE source.gold_status END,
            source.load_type, source.watermark_column,
            source.watermark_before, source.watermark_after, source.load_window_start, source.load_window_end,
            CASE WHEN '{layer}' = '{Layer.BRONZE.value}' THEN source.updated_at ELSE source.bronze_started_at END,
            CASE WHEN '{layer}' = '{Layer.SILVER.value}' THEN source.updated_at ELSE source.silver_started_at END,
            CASE WHEN '{layer}' = '{Layer.GOLD.value}' THEN source.updated_at ELSE source.gold_started_at END,
            source.bronze_ended_at, source.silver_ended_at, source.gold_ended_at,
            source.error_code, source.error_message,
            source.retry_count, source.last_retry_at, source.duration_ms, source.sla_target_ms, source.sla_breached,
            source.created_at, source.updated_at, source.source_table_name
        )
    """)

    table_session_id = get_single_id(
        audit_table_session_table,
        {
            "session_id": str(session_id),
            "source_table_id": int(source_table_id),
        },
    )
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
    error_code: str = None,
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
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("watermark_after", StringType(), True),
        StructField("sla_target_ms", LongType(), True),
    ])
    source_view = create_temp_view_from_rows([
        Row(
            id=str(table_session_id),
            layer_status=status,
            table_session_status=table_session_status,
            error_code=error_code,
            error_message=error_message,
            watermark_after=watermark_after,
            sla_target_ms=sla_target_ms,
        )
    ], schema, "finish_table_layer")

    spark.sql(f"""
        MERGE INTO {audit_table_session_table} AS target
        USING (
            SELECT
                id,
                layer_status,
                table_session_status,
                error_code,
                error_message,
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
            target.error_code = CASE
                WHEN source.layer_status = '{AuditStatus.FAILED.value}' THEN source.error_code
                ELSE NULL
            END,
            target.error_message = CASE
                WHEN source.layer_status = '{AuditStatus.FAILED.value}' THEN source.error_message
                ELSE NULL
            END,
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
        append_audit_detail({
            "id": new_audit_id(),
            "table_session_id": str(table_session_id),
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
