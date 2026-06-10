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

%run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

pipeline_run_id = f"manual_success_run_{int(time.time())}"
batch_id = 1001


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_session_id = start_pipeline_session(
    pipeline_name="pl_audit_mvp_test",
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode=RunMode.NEW,
    sla_target_ms=30 * 60 * 1000,
)

reused_audit_session_id = start_pipeline_session(
    pipeline_name="pl_audit_mvp_test",
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode=RunMode.NEW,
    sla_target_ms=30 * 60 * 1000,
)

assert reused_audit_session_id == audit_session_id


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=1,
    source_table_name="customer",
    layer=Layer.BRONZE,
    batch_id=batch_id,
    load_type="FULL",
)

file_session_id = start_file_session(
    session_id=audit_session_id,
    table_session_id=audit_table_session_id,
    source_table_id=1,
    batch_id=batch_id,
    source_file="Files/test/customer_success.json",
    file_row_count=100,
)

finish_file_session(
    file_session_id=file_session_id,
    status=AuditStatus.SUCCESS,
    processed_row_count=100,
    rejected_row_count=0,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_retry_attempt(
    table_session_id=audit_table_session_id,
    file_session_id=file_session_id,
    layer=Layer.BRONZE,
    status=AuditStatus.FAILED,
    error_code="TEST_RETRY",
    error_message="Simulated retry test",
    error_type=ErrorType.SYSTEM,
    is_retryable=True,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

finish_table_layer(
    table_session_id=audit_table_session_id,
    layer=Layer.BRONZE,
    status=AuditStatus.SUCCESS,
    is_final_table_step=True,
    source_row_count=100,
    target_row_count=100,
    inserted_row=100,
    updated_row=0,
    deleted_row=0,
    rejected_row=0,
)

finish_pipeline_session(
    session_id=audit_session_id,
    final_status=AuditStatus.SUCCESS,
)

print("SUCCESS test completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
