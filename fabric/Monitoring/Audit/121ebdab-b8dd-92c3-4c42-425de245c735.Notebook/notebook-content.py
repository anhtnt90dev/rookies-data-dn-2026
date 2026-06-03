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

%run nb_audit_row_count_dev


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

pipeline_name = "pl_row_count_mvp_test"
pipeline_run_id = f"manual_row_count_test_{int(time.time())}"

source_table = globals().get("source_table", "test_crm_customer_source")
bronze_table = globals().get("bronze_table", "bronze_customer")
silver_table = globals().get("silver_table", "silver_customer")
gold_table = globals().get("gold_table", "gold_dim_customer")
batch_id = globals().get("batch_id", 2001)
if "rejected_row_count" in globals():
    rejected_row_count = globals()["rejected_row_count"]
else:
    rejected_row_count = spark.table(bronze_table).where((F.col("_batch_id") == F.lit(batch_id)) & F.col("customer_name").isNull()).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_session_id = start_pipeline_session(
    pipeline_name=pipeline_name,
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode=RunMode.NEW,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=1,
    source_table_name=source_table,
    target_table_name=bronze_table,
    layer=Layer.BRONZE,
    batch_id=batch_id,
    load_type="FULL",
)

bronze_row_count_result = capture_row_counts({
    "table_session_id": bronze_table_session_id,
    "layer": Layer.BRONZE.value,
    "source_table": source_table,
    "target_table": bronze_table,
    "batch_id": batch_id,
    "source_use_batch_filter": False,
    "target_use_batch_filter": True,
})

finish_table_layer(
    table_session_id=bronze_table_session_id,
    layer=Layer.BRONZE,
    status=bronze_row_count_result["status"],
    is_final_table_step=True,
    write_detail=False,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=2,
    source_table_name=bronze_table,
    target_table_name=silver_table,
    layer=Layer.SILVER,
    batch_id=batch_id,
    load_type="FULL",
)

silver_row_count_result = capture_row_counts({
    "table_session_id": silver_table_session_id,
    "layer": Layer.SILVER.value,
    "source_table": bronze_table,
    "target_table": silver_table,
    "batch_id": batch_id,
    "source_use_batch_filter": True,
    "target_use_batch_filter": True,
    "rejected_row": rejected_row_count,
})

finish_table_layer(
    table_session_id=silver_table_session_id,
    layer=Layer.SILVER,
    status=silver_row_count_result["status"],
    is_final_table_step=True,
    write_detail=False,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_table_session_id = start_table_layer(
    session_id=audit_session_id,
    source_table_id=3,
    source_table_name=silver_table,
    target_table_name=gold_table,
    layer=Layer.GOLD,
    batch_id=batch_id,
    load_type="FULL",
)

gold_row_count_result = capture_row_counts({
    "table_session_id": gold_table_session_id,
    "layer": Layer.GOLD.value,
    "source_table": silver_table,
    "target_table": gold_table,
    "batch_id": batch_id,
    "source_use_batch_filter": True,
    "target_use_batch_filter": True,
})

finish_table_layer(
    table_session_id=gold_table_session_id,
    layer=Layer.GOLD,
    status=gold_row_count_result["status"],
    is_final_table_step=True,
    write_detail=False,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

layer_results = [bronze_row_count_result, silver_row_count_result, gold_row_count_result]
final_status = AuditStatus.SUCCESS.value

if any(result["status"] == AuditStatus.FAILED.value for result in layer_results):
    final_status = AuditStatus.FAILED.value

finish_pipeline_session(
    session_id=audit_session_id,
    final_status=final_status,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print({
    "audit_session_id": audit_session_id,
    "final_status": final_status,
    "bronze": bronze_row_count_result,
    "silver": silver_row_count_result,
    "gold": gold_row_count_result,
})

spark.table(AUDIT_DETAIL_TABLE).where(F.col("table_session_id").isin([
    bronze_table_session_id,
    silver_table_session_id,
    gold_table_session_id,
])).select(
    "table_session_id",
    "layer",
    "detail_status",
    "source_row_count",
    "target_row_count",
    "inserted_row",
    "rejected_row",
    "created_at",
).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
