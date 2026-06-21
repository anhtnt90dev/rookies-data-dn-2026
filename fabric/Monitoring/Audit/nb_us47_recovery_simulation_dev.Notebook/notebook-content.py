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
# META     }
# META   }
# META }

# CELL ********************

# Dev-only US47 framework simulation.
# This notebook does not modify Source-to-Bronze watermark behavior and does not call production Bronze/Silver/Gold flows.
try:
    notebook_runner = notebookutils.notebook
except NameError:
    from notebookutils import notebook as notebook_runner

notebook_runner.run("nb_audit_pipeline_log_dev", 300)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_audit_logging_helper_dev


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
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

def ensure_retry_policy_for_simulation():
    spark.sql("CREATE SCHEMA IF NOT EXISTS cfg")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS cfg.retry_policy (
            id BIGINT,
            policy_name STRING,
            max_retry_count INT,
            retry_delay_seconds INT,
            backoff_strategy STRING,
            retryable_error_types STRING,
            non_retryable_error_types STRING,
            is_active BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)

    active_policy_count = spark.table("cfg.retry_policy").where(F.col("is_active") == F.lit(True)).count()
    if active_policy_count > 0:
        return

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("policy_name", StringType(), False),
        StructField("max_retry_count", IntegerType(), False),
        StructField("retry_delay_seconds", IntegerType(), False),
        StructField("backoff_strategy", StringType(), False),
        StructField("retryable_error_types", StringType(), False),
        StructField("non_retryable_error_types", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ])

    retry_policy_df = (
        spark.createDataFrame([
            Row(
                id=1,
                policy_name="default_transient_system_retry",
                max_retry_count=2,
                retry_delay_seconds=60,
                backoff_strategy="FIXED_DELAY",
                retryable_error_types="SYSTEM",
                non_retryable_error_types="DATA,RULE,CONFIG,UNKNOWN",
                is_active=True,
            )
        ], schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    retry_policy_df.write.format("delta").mode("append").saveAsTable("cfg.retry_policy")


class SimulatedPipelineError(Exception):
    def __init__(self, message, error_type, error_code):
        super().__init__(message)
        self.error_type = enum_value(error_type)
        self.error_code = error_code


def table_session_row(table_session_id: str):
    return (
        spark.table(AUDIT_TABLE_SESSION_TABLE)
        .where(F.col("id") == F.lit(str(table_session_id)))
        .select("table_session_status", "bronze_status", "retry_count", "error_code", "error_message")
        .collect()[0]
    )


def retry_log_count(table_session_id: str) -> int:
    return spark.table(RETRY_LOG_TABLE).where(F.col("table_session_id") == F.lit(str(table_session_id))).count()


def invalid_record_count(table_session_id: str) -> int:
    return spark.table(INVALID_RECORD_TABLE).where(F.col("table_session_id") == F.lit(str(table_session_id))).count()


def file_status(batch_id: int, source_table_id: int, source_file: str):
    rows = (
        spark.table(AUDIT_FILE_SESSION_TABLE)
        .where(
            (F.col("batch_id") == F.lit(int(batch_id)))
            & (F.col("source_table_id") == F.lit(int(source_table_id)))
            & (F.col("source_file") == F.lit(source_file))
        )
        .select("file_status")
        .limit(1)
        .collect()
    )
    return rows[0]["file_status"] if rows else None


ensure_retry_policy_for_simulation()
retry_policy = get_active_retry_policy()
reset_next_run_mode()

assert is_retryable_error(ErrorType.SYSTEM, retry_policy) is True
assert is_retryable_error(ErrorType.DATA, retry_policy) is False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test A: NEW run creates a new batch context and audit session.
new_context = initialize_run_context(
    pipeline_name="nb_us47_recovery_simulation_dev",
    pipeline_run_id=f"us47_new_{int(time.time() * 1000)}",
)

assert new_context["run_mode"] == RunMode.NEW.value
assert new_context["batch_id"] is not None
assert new_context["session_id"] is not None

new_session_row = (
    spark.table(AUDIT_SESSION_TABLE)
    .where(F.col("id") == F.lit(new_context["session_id"]))
    .select("run_mode", "batch_id", "session_status")
    .collect()[0]
)
assert new_session_row["run_mode"] == RunMode.NEW.value
assert new_session_row["batch_id"] == new_context["batch_id"]
assert new_session_row["session_status"] == AuditStatus.RUNNING.value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test B: retryable system error succeeds after one audited retry.
table_session_retry_success = start_table_layer(
    session_id=new_context["session_id"],
    source_table_id=4741,
    source_table_name="us47_retry_success",
    layer=Layer.BRONZE,
    batch_id=new_context["batch_id"],
    load_type="SIMULATION",
)
attempt_state = {"count": 0}


def fails_once_then_succeeds():
    attempt_state["count"] += 1
    if attempt_state["count"] == 1:
        raise SimulatedPipelineError("Transient storage failure", ErrorType.SYSTEM, "SIM_SYSTEM_TRANSIENT")
    return "SUCCESS_AFTER_RETRY"


retry_success_result = run_with_retry(
    operation_fn=fails_once_then_succeeds,
    table_session_id=table_session_retry_success,
    layer=Layer.BRONZE,
    operation_name="us47_retry_success",
    error_type=ErrorType.SYSTEM,
    policy=retry_policy,
    apply_retry_delay=False,
)
retry_success_row = table_session_row(table_session_retry_success)

assert retry_success_result == "SUCCESS_AFTER_RETRY"
assert retry_log_count(table_session_retry_success) == 1
assert retry_success_row["retry_count"] == 1
assert retry_success_row["table_session_status"] == AuditStatus.SUCCESS.value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test C: retryable system error exhausts retries and marks the table session FAILED.
table_session_retry_exhausted = start_table_layer(
    session_id=new_context["session_id"],
    source_table_id=4742,
    source_table_name="us47_retry_exhausted",
    layer=Layer.BRONZE,
    batch_id=new_context["batch_id"],
    load_type="SIMULATION",
)


def always_fails_system():
    raise SimulatedPipelineError("Persistent system failure", ErrorType.SYSTEM, "SIM_SYSTEM_EXHAUSTED")


try:
    run_with_retry(
        operation_fn=always_fails_system,
        table_session_id=table_session_retry_exhausted,
        layer=Layer.BRONZE,
        operation_name="us47_retry_exhausted",
        error_type=ErrorType.SYSTEM,
        policy=retry_policy,
        apply_retry_delay=False,
    )
    raise AssertionError("Expected retry exhaustion")
except SimulatedPipelineError:
    pass

retry_exhausted_row = table_session_row(table_session_retry_exhausted)
assert retry_log_count(table_session_retry_exhausted) == retry_policy["max_retry_count"]
assert retry_exhausted_row["retry_count"] == retry_policy["max_retry_count"]
assert retry_exhausted_row["table_session_status"] == AuditStatus.FAILED.value
assert retry_exhausted_row["error_code"] == "SIM_SYSTEM_EXHAUSTED"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test D: non-retryable data error writes invalid records and does not retry.
table_session_data_error = start_table_layer(
    session_id=new_context["session_id"],
    source_table_id=4743,
    source_table_name="us47_data_error",
    layer=Layer.SILVER,
    batch_id=new_context["batch_id"],
    load_type="SIMULATION",
)


def fails_data_validation():
    log_invalid_record(
        table_session_id=table_session_data_error,
        layer=Layer.SILVER,
        target_table="silver.us47_data_error",
        record_key="business_key_001",
        raw_data='{"business_key":"business_key_001","required_value":null}',
        error_column="required_value",
        error_reason="Required value is null",
        error_type=ErrorType.DATA,
        is_retryable=False,
    )
    raise SimulatedPipelineError("Data validation failed", ErrorType.DATA, "SIM_DATA_VALIDATION")


try:
    run_with_retry(
        operation_fn=fails_data_validation,
        table_session_id=table_session_data_error,
        layer=Layer.SILVER,
        operation_name="us47_data_error",
        error_type=ErrorType.DATA,
        policy=retry_policy,
        apply_retry_delay=False,
    )
    raise AssertionError("Expected non-retryable data error")
except SimulatedPipelineError:
    pass

data_error_row = table_session_row(table_session_data_error)
assert retry_log_count(table_session_data_error) == 0
assert invalid_record_count(table_session_data_error) == 1
assert data_error_row["table_session_status"] == AuditStatus.FAILED.value
assert data_error_row["error_code"] == "SIM_DATA_VALIDATION"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test E: file recovery skips successful files and processes failed or missing files.
table_session_file_recovery = start_table_layer(
    session_id=new_context["session_id"],
    source_table_id=4744,
    source_table_name="us47_file_recovery",
    layer=Layer.BRONZE,
    batch_id=new_context["batch_id"],
    load_type="SIMULATION",
)

success_file_id = start_file_session(
    session_id=new_context["session_id"],
    table_session_id=table_session_file_recovery,
    source_table_id=4744,
    batch_id=new_context["batch_id"],
    source_file="Files/us47/success.json",
    file_row_count=10,
)
finish_file_session(success_file_id, AuditStatus.SUCCESS, processed_row_count=10, rejected_row_count=0)

failed_file_id = start_file_session(
    session_id=new_context["session_id"],
    table_session_id=table_session_file_recovery,
    source_table_id=4744,
    batch_id=new_context["batch_id"],
    source_file="Files/us47/failed.json",
    file_row_count=8,
)
finish_file_session(
    failed_file_id,
    AuditStatus.FAILED,
    processed_row_count=0,
    rejected_row_count=0,
    error_code="SIM_FILE_FAILURE",
    error_message="Simulated file read failure",
    error_type=ErrorType.SYSTEM,
    is_retryable=True,
)

file_recovery_plan = get_failed_or_missing_files(
    batch_id=new_context["batch_id"],
    source_table_id=4744,
    source_files=[
        "Files/us47/success.json",
        "Files/us47/failed.json",
        "Files/us47/missing.json",
    ],
)
planned_files = {item["source_file"] for item in file_recovery_plan}
assert "Files/us47/success.json" not in planned_files
assert "Files/us47/failed.json" in planned_files
assert "Files/us47/missing.json" in planned_files

for file_item in file_recovery_plan:
    recovery_file_id = start_file_session(
        session_id=new_context["session_id"],
        table_session_id=table_session_file_recovery,
        source_table_id=4744,
        batch_id=new_context["batch_id"],
        source_file=file_item["source_file"],
        file_row_count=1,
    )
    finish_file_session(recovery_file_id, AuditStatus.SUCCESS, processed_row_count=1, rejected_row_count=0)

assert get_failed_or_missing_files(
    batch_id=new_context["batch_id"],
    source_table_id=4744,
    source_files=[
        "Files/us47/success.json",
        "Files/us47/failed.json",
        "Files/us47/missing.json",
    ],
) == []
assert file_status(new_context["batch_id"], 4744, "Files/us47/failed.json") == AuditStatus.SUCCESS.value
finish_table_layer(table_session_file_recovery, Layer.BRONZE, AuditStatus.SUCCESS, is_final_table_step=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test F: RECOVERY reuses failed batch_id, creates a new session_id, skips successful work, and resumes failed work.
mark_recovery_required(
    batch_id=new_context["batch_id"],
    session_id=new_context["session_id"],
    failed_layer=Layer.BRONZE,
    failed_table="us47_retry_exhausted",
    error_code="SIM_SYSTEM_EXHAUSTED",
    error_message="Persistent system failure",
)
finish_pipeline_session(new_context["session_id"], AuditStatus.FAILED)

recovery_context = initialize_run_context(
    pipeline_name="nb_us47_recovery_simulation_dev",
    pipeline_run_id=f"us47_recovery_{int(time.time() * 1000)}",
)

assert recovery_context["run_mode"] == RunMode.RECOVERY.value
assert recovery_context["batch_id"] == new_context["batch_id"]
assert recovery_context["session_id"] != new_context["session_id"]
assert recovery_context["previous_session_id"] == new_context["session_id"]

recovery_table_plan = get_recovery_table_plan(
    batch_id=recovery_context["batch_id"],
    source_table_ids=[4741, 4742],
    layer=Layer.BRONZE,
)
plan_by_source = {item["source_table_id"]: item["should_process"] for item in recovery_table_plan}
assert plan_by_source[4741] is False
assert plan_by_source[4742] is True

recovered_table_session = start_table_layer(
    session_id=recovery_context["session_id"],
    source_table_id=4742,
    source_table_name="us47_retry_exhausted",
    layer=Layer.BRONZE,
    batch_id=recovery_context["batch_id"],
    load_type="SIMULATION",
)
finish_table_layer(recovered_table_session, Layer.BRONZE, AuditStatus.SUCCESS, is_final_table_step=True)

post_recovery_table_plan = get_recovery_table_plan(
    batch_id=recovery_context["batch_id"],
    source_table_ids=[4741, 4742],
    layer=Layer.BRONZE,
)
assert all(item["should_process"] is False for item in post_recovery_table_plan)

post_recovery_file_plan = get_failed_or_missing_files(
    batch_id=recovery_context["batch_id"],
    source_table_id=4744,
    source_files=[
        "Files/us47/success.json",
        "Files/us47/failed.json",
        "Files/us47/missing.json",
    ],
)
assert post_recovery_file_plan == []

finish_pipeline_session(recovery_context["session_id"], AuditStatus.SUCCESS)
reset_next_run_mode()

next_context = get_next_run_context()
assert next_context["run_mode"] == RunMode.NEW.value
assert next_context["batch_id"] is None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print({
    "simulation": "nb_us47_recovery_simulation_dev",
    "batch_id": new_context["batch_id"],
    "new_session_id": new_context["session_id"],
    "recovery_session_id": recovery_context["session_id"],
    "previous_session_id_returned": recovery_context["previous_session_id"],
    "tests": {
        "A_new_run": {
            "run_mode": new_context["run_mode"],
            "session_status": new_session_row["session_status"],
        },
        "B_retry_success": {
            "retry_count": retry_success_row["retry_count"],
            "retry_log_count": retry_log_count(table_session_retry_success),
            "table_status": retry_success_row["table_session_status"],
        },
        "C_retry_exhausted": {
            "retry_count": retry_exhausted_row["retry_count"],
            "retry_log_count": retry_log_count(table_session_retry_exhausted),
            "table_status": retry_exhausted_row["table_session_status"],
        },
        "D_data_error": {
            "retry_log_count": retry_log_count(table_session_data_error),
            "invalid_record_count": invalid_record_count(table_session_data_error),
            "table_status": data_error_row["table_session_status"],
        },
        "E_file_recovery": {
            "initial_plan": file_recovery_plan,
            "post_recovery_plan": post_recovery_file_plan,
        },
        "F_recovery_run": {
            "run_mode": recovery_context["run_mode"],
            "same_batch_id": recovery_context["batch_id"] == new_context["batch_id"],
            "new_session_id": recovery_context["session_id"] != new_context["session_id"],
            "table_plan_before": recovery_table_plan,
            "table_plan_after": post_recovery_table_plan,
            "next_run_mode_after_reset": next_context["run_mode"],
        },
    },
})
