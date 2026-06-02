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


def does_table_exist(table_name):
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def has_table_column(table_name, column_name):
    if not does_table_exist(table_name):
        return False

    table_column_names = [field.name for field in spark.table(table_name).schema.fields]
    return column_name in table_column_names


def count_table_rows(table_name, batch_id=None, batch_column="_batch_id", use_batch_filter=True):
    if not does_table_exist(table_name):
        raise Exception(f"Table not found: {table_name}")

    should_use_batch_filter = (
        use_batch_filter
        and batch_id is not None
        and has_table_column(table_name, batch_column)
    )

    if should_use_batch_filter:
        query = f"""
            SELECT COUNT(*) AS row_count
            FROM {table_name}
            WHERE {batch_column} = {batch_id}
        """
    else:
        query = f"""
            SELECT COUNT(*) AS row_count
            FROM {table_name}
        """

    return int(spark.sql(query).collect()[0]["row_count"])


def classify_count_error(error_message):
    lower_error_message = str(error_message).lower()

    if "table not found" in lower_error_message or "not found" in lower_error_message:
        return "CONFIG", False

    if "permission" in lower_error_message or "connection" in lower_error_message or "timeout" in lower_error_message:
        return "SYSTEM", True

    return "UNKNOWN", False


# Function to write row count to log.audit_detail.
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
    is_retryable=None
):
    layer = layer.upper()
    detail_status = detail_status.upper()

    attempt_no = int(spark.sql(f"""
        SELECT COALESCE(MAX(attempt_no), 0) + 1 AS next_attempt
        FROM log.audit_detail
        WHERE table_session_id = {table_session_id}
          AND layer = {format_sql_value(layer)}
    """).collect()[0]["next_attempt"])

    audit_detail_id = get_next_id("log.audit_detail")
    audit_key = f"table_session_{table_session_id}|layer_{layer}|row_count_attempt_{attempt_no}"

    spark.sql(f"""
        INSERT INTO log.audit_detail (
            id,
            table_session_id,
            audit_key,
            attempt_no,
            detail_status,
            layer,
            source_row_count,
            target_row_count,
            inserted_row,
            updated_row,
            deleted_row,
            rejected_row,
            error_message,
            error_type,
            is_retryable,
            created_at,
            updated_at
        )
        VALUES (
            {audit_detail_id},
            {table_session_id},
            {format_sql_value(audit_key)},
            {attempt_no},
            {format_sql_value(detail_status)},
            {format_sql_value(layer)},
            {format_sql_value(source_row_count)},
            {format_sql_value(target_row_count)},
            {format_sql_value(inserted_row)},
            {format_sql_value(updated_row)},
            {format_sql_value(deleted_row)},
            {format_sql_value(rejected_row)},
            {format_sql_value(error_message)},
            {format_sql_value(error_type)},
            {format_sql_value(is_retryable)},
            current_timestamp(),
            current_timestamp()
        )
    """)

    print(f"Logged row counts for {layer}, table_session_id={table_session_id}")


# Generic row count executor.
def capture_row_counts(config):
    required_keys = [
        "table_session_id",
        "layer",
        "source_table",
        "target_table",
        "batch_id"
    ]

    for key in required_keys:
        if key not in config or config[key] is None:
            raise ValueError(f"Missing required config value: {key}")

    table_session_id = config["table_session_id"]
    layer = config["layer"]
    source_table = config["source_table"]
    target_table = config["target_table"]
    batch_id = config["batch_id"]

    batch_column = config.get("batch_column", "_batch_id")
    should_filter_source_by_batch = config.get("source_use_batch_filter", True)
    should_filter_target_by_batch = config.get("target_use_batch_filter", True)
    rejected_row = config.get("rejected_row", 0)
    updated_row = config.get("updated_row", 0)
    deleted_row = config.get("deleted_row", 0)

    try:
        source_row_count = count_table_rows(
            table_name=source_table,
            batch_id=batch_id,
            batch_column=batch_column,
            use_batch_filter=should_filter_source_by_batch
        )

        target_row_count = count_table_rows(
            table_name=target_table,
            batch_id=batch_id,
            batch_column=batch_column,
            use_batch_filter=should_filter_target_by_batch
        )

        inserted_row = config.get("inserted_row")
        if inserted_row is None:
            inserted_row = target_row_count

        status = "SUCCESS"
        error_message = None
        error_type = None
        is_retryable = None

        if config.get("fail_on_zero_source", False) and source_row_count == 0:
            status = "FAILED"
            error_message = "Source row count is zero"
            error_type = "RULE"
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
            is_retryable=is_retryable
        )

        return {
            "status": status,
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "inserted_row": inserted_row,
            "updated_row": updated_row,
            "deleted_row": deleted_row,
            "rejected_row": rejected_row
        }

    except Exception as error:
        error_message = str(error)[:1000]
        error_type, is_retryable = classify_count_error(error_message)

        log_row_count_detail(
            table_session_id=table_session_id,
            layer=layer,
            detail_status="FAILED",
            source_row_count=None,
            target_row_count=None,
            inserted_row=0,
            updated_row=0,
            deleted_row=0,
            rejected_row=0,
            error_message=error_message,
            error_type=error_type,
            is_retryable=is_retryable
        )

        return {
            "status": "FAILED",
            "error_message": error_message,
            "error_type": error_type,
            "is_retryable": is_retryable
        }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
