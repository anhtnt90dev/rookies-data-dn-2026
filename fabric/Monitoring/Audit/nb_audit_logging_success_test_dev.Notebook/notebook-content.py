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
import time

# ============================================================
# TEST 1: Successful pipeline/table/layer execution
# Purpose:
#   Prove that the helper can log a successful pipeline run.
# ============================================================

pipeline_run_id = f"manual_success_run_{int(time.time())}"
batch_id = 1001

audit_session_id = start_pipeline_session(
    pipeline_name="pl_audit_mvp_test",
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode="NEW",
    sla_target_ms=30 * 60 * 1000
)

audit_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=1,
    source_table_name="customer",
    target_table_name="bronze_customer",
    layer="BRONZE",
    batch_id=batch_id,
    load_type="FULL"
)

finish_table_layer(
    table_session_id=audit_table_session_id,
    layer="BRONZE",
    status="SUCCESS",
    is_final_table_step=True,
    source_row_count=100,
    inserted_row=100,
    updated_row=0,
    deleted_row=0,
    rejected_row=0
)

finish_pipeline_session(
    session_id=audit_session_id,
    final_status="SUCCESS"
)

print("SUCCESS test completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
