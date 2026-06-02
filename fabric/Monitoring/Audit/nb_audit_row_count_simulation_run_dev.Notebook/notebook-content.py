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

pipeline_run_id = f"manual_row_count_test_{int(time.time())}"

audit_session_id = start_pipeline_session(
    pipeline_name="pl_row_count_mvp_test",
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode="NEW"
)

bronze_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=1,
    source_table_name="test_crm_customer_source",
    target_table_name="bronze_customer",
    layer="BRONZE",
    batch_id=batch_id,
    load_type="FULL"
)

bronze_row_count_result = capture_row_counts({
    "table_session_id": bronze_table_session_id,
    "layer": "BRONZE",
    "source_table": "test_crm_customer_source",
    "target_table": "bronze_customer",
    "batch_id": batch_id,
    "source_use_batch_filter": False,
    "target_use_batch_filter": True
})

finish_table_layer(
    table_session_id=bronze_table_session_id,
    layer="BRONZE",
    status=bronze_row_count_result["status"],
    is_final_table_step=True
)

silver_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=2,
    source_table_name="bronze_customer",
    target_table_name="silver_customer",
    layer="SILVER",
    batch_id=batch_id,
    load_type="FULL"
)

silver_row_count_result = capture_row_counts({
    "table_session_id": silver_table_session_id,
    "layer": "SILVER",
    "source_table": "bronze_customer",
    "target_table": "silver_customer",
    "batch_id": batch_id,
    "source_use_batch_filter": True,
    "target_use_batch_filter": True,
    "rejected_row": rejected_row_count
})

finish_table_layer(
    table_session_id=silver_table_session_id,
    layer="SILVER",
    status=silver_row_count_result["status"],
    is_final_table_step=True
)

gold_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=3,
    source_table_name="silver_customer",
    target_table_name="gold_dim_customer",
    layer="GOLD",
    batch_id=batch_id,
    load_type="FULL"
)

gold_row_count_result = capture_row_counts({
    "table_session_id": gold_table_session_id,
    "layer": "GOLD",
    "source_table": "silver_customer",
    "target_table": "gold_dim_customer",
    "batch_id": batch_id,
    "source_use_batch_filter": True,
    "target_use_batch_filter": True
})

finish_table_layer(
    table_session_id=gold_table_session_id,
    layer="GOLD",
    status=gold_row_count_result["status"],
    is_final_table_step=True
)

final_status = "SUCCESS"

if (
    bronze_row_count_result["status"] == "FAILED"
    or silver_row_count_result["status"] == "FAILED"
    or gold_row_count_result["status"] == "FAILED"
):
    final_status = "FAILED"

finish_pipeline_session(
    session_id=audit_session_id,
    final_status=final_status
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
