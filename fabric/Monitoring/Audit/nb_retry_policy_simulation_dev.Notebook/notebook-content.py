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

# CELL ********************

# Dev-only proof for retry policy and retry logging helpers.
# This notebook does not integrate retry behavior into Bronze, Silver, or Gold production flows.
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


ensure_retry_policy_for_simulation()
retry_policy = get_active_retry_policy()

assert retry_policy["max_retry_count"] == 2
assert is_retryable_error(ErrorType.SYSTEM, retry_policy) is True
assert is_retryable_error(ErrorType.DATA, retry_policy) is False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class SimulatedPipelineError(Exception):
    def __init__(self, message, error_type, error_code):
        super().__init__(message)
        self.error_type = enum_value(error_type)
        self.error_code = error_code


def create_retry_test_table_session(test_name: str, source_table_id: int, batch_id: int):
    pipeline_run_id = f"retry_policy_{test_name}_{int(time.time() * 1000)}"
    session_id = start_pipeline_session(
        pipeline_name="nb_retry_policy_simulation_dev",
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        run_mode=RunMode.NEW,
    )
    table_session_id = start_table_layer(
        session_id=session_id,
        source_table_id=source_table_id,
        source_table_name=f"retry_policy_{test_name}",
        layer=Layer.BRONZE,
        batch_id=batch_id,
        load_type="SIMULATION",
    )
    return session_id, table_session_id


def retry_log_count(table_session_id: str) -> int:
    return spark.table(RETRY_LOG_TABLE).where(F.col("table_session_id") == F.lit(str(table_session_id))).count()


def table_session_row(table_session_id: str):
    return (
        spark.table(AUDIT_TABLE_SESSION_TABLE)
        .where(F.col("id") == F.lit(str(table_session_id)))
        .select("table_session_status", "bronze_status", "retry_count", "error_code", "error_message")
        .collect()[0]
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 1: retryable system error succeeds after retry.
session_id_1, table_session_id_1 = create_retry_test_table_session("system_success_after_retry", 4701, 4701001)
attempt_state = {"count": 0}


def fails_once_then_succeeds():
    attempt_state["count"] += 1
    if attempt_state["count"] == 1:
        raise SimulatedPipelineError(
            "Simulated transient system failure",
            ErrorType.SYSTEM,
            "SIM_SYSTEM_TRANSIENT",
        )
    return "SUCCESS_AFTER_RETRY"


result_1 = run_with_retry(
    operation_fn=fails_once_then_succeeds,
    table_session_id=table_session_id_1,
    layer=Layer.BRONZE,
    operation_name="system_success_after_retry",
    error_type=ErrorType.SYSTEM,
    policy=retry_policy,
    apply_retry_delay=False,
)

row_1 = table_session_row(table_session_id_1)
assert result_1 == "SUCCESS_AFTER_RETRY"
assert retry_log_count(table_session_id_1) == 1
assert row_1["retry_count"] == 1
assert row_1["table_session_status"] == AuditStatus.SUCCESS.value
finish_pipeline_session(session_id_1, AuditStatus.SUCCESS)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 2: retryable system error exhausts retries.
session_id_2, table_session_id_2 = create_retry_test_table_session("system_retry_exhausted", 4702, 4701002)


def always_fails_system():
    raise SimulatedPipelineError(
        "Simulated persistent system failure",
        ErrorType.SYSTEM,
        "SIM_SYSTEM_EXHAUSTED",
    )


try:
    run_with_retry(
        operation_fn=always_fails_system,
        table_session_id=table_session_id_2,
        layer=Layer.BRONZE,
        operation_name="system_retry_exhausted",
        error_type=ErrorType.SYSTEM,
        policy=retry_policy,
        apply_retry_delay=False,
    )
    raise AssertionError("Expected retry exhaustion to raise an exception")
except SimulatedPipelineError:
    pass

row_2 = table_session_row(table_session_id_2)
assert retry_log_count(table_session_id_2) == retry_policy["max_retry_count"]
assert row_2["retry_count"] == retry_policy["max_retry_count"]
assert row_2["table_session_status"] == AuditStatus.FAILED.value
assert row_2["error_code"] == "SIM_SYSTEM_EXHAUSTED"
finish_pipeline_session(session_id_2, AuditStatus.FAILED)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 3: non-retryable data error is not retried.
session_id_3, table_session_id_3 = create_retry_test_table_session("data_error_no_retry", 4703, 4701003)


def fails_data_validation():
    raise SimulatedPipelineError(
        "Simulated data validation failure",
        ErrorType.DATA,
        "SIM_DATA_VALIDATION",
    )


try:
    run_with_retry(
        operation_fn=fails_data_validation,
        table_session_id=table_session_id_3,
        layer=Layer.BRONZE,
        operation_name="data_error_no_retry",
        error_type=ErrorType.DATA,
        policy=retry_policy,
        apply_retry_delay=False,
    )
    raise AssertionError("Expected non-retryable data error to raise an exception")
except SimulatedPipelineError:
    pass

row_3 = table_session_row(table_session_id_3)
assert retry_log_count(table_session_id_3) == 0
assert row_3["retry_count"] == 0
assert row_3["table_session_status"] == AuditStatus.FAILED.value
assert row_3["error_code"] == "SIM_DATA_VALIDATION"
finish_pipeline_session(session_id_3, AuditStatus.FAILED)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print({
    "simulation": "nb_retry_policy_simulation_dev",
    "retry_policy": retry_policy,
    "tests": {
        "system_success_after_retry": {
            "table_session_id": table_session_id_1,
            "retry_log_count": retry_log_count(table_session_id_1),
            "table_status": table_session_row(table_session_id_1)["table_session_status"],
        },
        "system_retry_exhausted": {
            "table_session_id": table_session_id_2,
            "retry_log_count": retry_log_count(table_session_id_2),
            "table_status": table_session_row(table_session_id_2)["table_session_status"],
        },
        "data_error_no_retry": {
            "table_session_id": table_session_id_3,
            "retry_log_count": retry_log_count(table_session_id_3),
            "table_status": table_session_row(table_session_id_3)["table_session_status"],
        },
    },
})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
