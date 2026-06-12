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
batch_id = globals().get("batch_id", 2003)
source_table_id = 1
source_table_name = source_table
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
    source_table_id=source_table_id,
    source_table_name=source_table_name,
    layer=Layer.BRONZE,
    batch_id=batch_id,
    load_type="FULL",
)
source_file = "Files/landing/crm_system/customer/test_customer_file.json"

file_session_id = start_file_session(
    session_id=audit_session_id,
    table_session_id=bronze_table_session_id,
    source_table_id=source_table_id,
    batch_id=batch_id,
    source_file=source_file,
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

finish_file_session(
    file_session_id=file_session_id,
    status=bronze_row_count_result["status"],
    processed_row_count=bronze_row_count_result.get("target_row_count"),
    rejected_row_count=0,
    error_code=bronze_row_count_result.get("error_code"),
    error_message=bronze_row_count_result.get("error_message"),
    error_type=bronze_row_count_result.get("error_type"),
    is_retryable=bronze_row_count_result.get("is_retryable"),
)

finish_table_layer(
    table_session_id=bronze_table_session_id,
    layer=Layer.BRONZE,
    status=bronze_row_count_result["status"],
    is_final_table_step=False,
    error_code=bronze_row_count_result.get("error_code"),
    error_message=bronze_row_count_result.get("error_message"),
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
    source_table_id=source_table_id,
    source_table_name=source_table_name,
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
invalid_rows = (
    spark.table(bronze_table)
    .where((F.col("_batch_id") == F.lit(batch_id)) & F.col("customer_name").isNull())
    .collect()
)

for invalid_row in invalid_rows:
    log_invalid_record(
        table_session_id=silver_table_session_id,
        file_session_id=file_session_id,
        layer=Layer.SILVER,
        target_table=silver_table,
        record_key=str(invalid_row["customer_id"]),
        raw_data=str(invalid_row.asDict()),
        error_column="customer_name",
        error_reason="customer_name is null",
        error_type=ErrorType.DATA,
        is_retryable=False,
    )

finish_table_layer(
    table_session_id=silver_table_session_id,
    layer=Layer.SILVER,
    status=silver_row_count_result["status"],
    is_final_table_step=False,
    error_code=silver_row_count_result.get("error_code"),
    error_message=silver_row_count_result.get("error_message"),
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
    source_table_id=source_table_id,
    source_table_name=source_table_name,
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
    error_code=gold_row_count_result.get("error_code"),
    error_message=gold_row_count_result.get("error_message"),
    write_detail=False,
)

assert bronze_table_session_id == silver_table_session_id
assert silver_table_session_id == gold_table_session_id


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
