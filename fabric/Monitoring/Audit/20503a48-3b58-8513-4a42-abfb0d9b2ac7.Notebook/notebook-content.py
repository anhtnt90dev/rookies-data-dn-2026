# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def does_table_exist(table_name):
    table_name = validate_table_name(table_name)
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def has_table_column(table_name, column_name):
    table_name = validate_table_name(table_name)
    if not does_table_exist(table_name):
        return False

    table_column_names = [field.name for field in spark.table(table_name).schema.fields]
    return column_name in table_column_names


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def count_table_rows(table_name, batch_id=None, batch_column="_batch_id", use_batch_filter=True):
    table_name = validate_table_name(table_name)
    if not does_table_exist(table_name):
        raise Exception(f"Table not found: {table_name}")

    table_df = spark.table(table_name)
    should_use_batch_filter = use_batch_filter and batch_id is not None and has_table_column(table_name, batch_column)

    if should_use_batch_filter:
        table_df = table_df.where(F.col(batch_column) == F.lit(batch_id))

    return int(table_df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def classify_count_error(error_message):
    lower_error_message = str(error_message).lower()

    if "table not found" in lower_error_message or "not found" in lower_error_message:
        return ErrorType.CONFIG.value, False

    if "permission" in lower_error_message or "connection" in lower_error_message or "timeout" in lower_error_message:
        return ErrorType.SYSTEM.value, True

    return ErrorType.UNKNOWN.value, False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_row_count_detail(
    table_session_id,
    layer,
    detail_status,
    source_row_count=None,
    target_row_count=None,
    inserted_row=None,
    updated_row=0,
    deleted_row=0,
    rejected_row=0,
    error_message=None,
    error_type=None,
    is_retryable=None,
    audit_detail_table=AUDIT_DETAIL_TABLE,
):
    audit_detail_table = validate_table_name(audit_detail_table)
    layer = require_layer(layer)
    detail_status = require_status(detail_status, [AuditStatus.SUCCESS, AuditStatus.FAILED, AuditStatus.SKIPPED])
    attempt_no = get_next_attempt_no(audit_detail_table, str(table_session_id), layer)
    audit_key = f"table_session_{table_session_id}|layer_{layer}|row_count_attempt_{attempt_no}"

    append_audit_detail({
        "id": new_audit_id(),
        "table_session_id": str(table_session_id),
        "audit_key": audit_key,
        "attempt_no": attempt_no,
        "detail_status": detail_status,
        "layer": layer,
        "watermark_before": None,
        "watermark_after": None,
        "load_window_start": None,
        "load_window_end": None,
        "source_row_count": source_row_count,
        "target_row_count": target_row_count,
        "inserted_row": inserted_row,
        "updated_row": updated_row,
        "deleted_row": deleted_row,
        "rejected_row": rejected_row,
        "error_message": error_message,
        "error_type": enum_value(error_type) if error_type is not None else None,
        "is_retryable": is_retryable,
        "duration_ms": None,
        "sla_target_ms": None,
        "sla_breached": None,
    }, audit_detail_table)

    print(f"Logged row counts for {layer}, table_session_id={table_session_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def capture_row_counts(config, audit_detail_table=AUDIT_DETAIL_TABLE):
    required_keys = ["table_session_id", "layer", "source_table", "target_table", "batch_id"]
    for key in required_keys:
        if key not in config or config[key] is None:
            raise ValueError(f"Missing required config value: {key}")

    table_session_id = config["table_session_id"]
    layer = require_layer(config["layer"])
    source_table = validate_table_name(config["source_table"])
    target_table = validate_table_name(config["target_table"])
    batch_id = config["batch_id"]
    batch_column = config.get("batch_column", "_batch_id")
    should_filter_source_by_batch = config.get("source_use_batch_filter", True)
    should_filter_target_by_batch = config.get("target_use_batch_filter", True)
    rejected_row = config.get("rejected_row", 0)
    updated_row = config.get("updated_row", 0)
    deleted_row = config.get("deleted_row", 0)

    try:
        source_row_count = count_table_rows(source_table, batch_id, batch_column, should_filter_source_by_batch)
        target_row_count = count_table_rows(target_table, batch_id, batch_column, should_filter_target_by_batch)
        inserted_row = config.get("inserted_row")
        if inserted_row is None:
            inserted_row = target_row_count

        status = AuditStatus.SUCCESS.value
        error_message = None
        error_type = None
        is_retryable = None

        if config.get("fail_on_zero_source", False) and source_row_count == 0:
            status = AuditStatus.FAILED.value
            error_message = "Source row count is zero"
            error_type = ErrorType.RULE.value
            is_retryable = False

        log_row_count_detail(
            table_session_id=table_session_id,
            layer=layer,
            detail_status=status,
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            inserted_row=inserted_row,
            updated_row=updated_row,
            deleted_row=deleted_row,
            rejected_row=rejected_row,
            error_message=error_message,
            error_type=error_type,
            is_retryable=is_retryable,
            audit_detail_table=audit_detail_table,
        )

        return {
            "status": status,
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "inserted_row": inserted_row,
            "updated_row": updated_row,
            "deleted_row": deleted_row,
            "rejected_row": rejected_row,
        }

    except Exception as error:
        error_message = str(error)[:1000]
        error_type, is_retryable = classify_count_error(error_message)

        log_row_count_detail(
            table_session_id=table_session_id,
            layer=layer,
            detail_status=AuditStatus.FAILED.value,
            source_row_count=None,
            target_row_count=None,
            inserted_row=0,
            updated_row=0,
            deleted_row=0,
            rejected_row=0,
            error_message=error_message,
            error_type=error_type,
            is_retryable=is_retryable,
            audit_detail_table=audit_detail_table,
        )

        return {
            "status": AuditStatus.FAILED.value,
            "error_message": error_message,
            "error_type": error_type,
            "is_retryable": is_retryable,
        }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
