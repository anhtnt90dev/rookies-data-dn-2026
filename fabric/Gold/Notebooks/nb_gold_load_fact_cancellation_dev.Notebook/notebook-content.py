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

# MAGIC %run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

session_id = ""
batch_id = 0
run_mode = "NEW"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()

FACT_TABLE_NAME = "gold.fact_cancellation"
DIM_FACT_TABLE_ID = 19

# Check if table should process in this run
if not should_process_table_layer(batch_id, DIM_FACT_TABLE_ID, "GOLD"):
    print(f"[BYPASS] Table {FACT_TABLE_NAME} was already successfully processed in this batch. Skipping.")
    mssparkutils.notebook.exit("Bypassed")

# Start Table Ingestion Session
table_session_id = start_table_layer(
    session_id=session_id,
    source_table_id=DIM_FACT_TABLE_ID,
    source_table_name="fact_cancellation",
    layer="GOLD",
    batch_id=batch_id,
    load_type="INCREMENTAL"
)

try:
    # 1. Read silver.cancellation filtered by batch_id
    c_df = spark.table("silver.cancellation").where(F.col("_batch_id") == F.lit(str(batch_id)))

    # 2. Join with silver.policy for parent context
    p_df = spark.table("silver.policy").select("policy_id", "customer_id", "provider_code")
    c_with_p = c_df.alias("c").join(p_df.alias("p"), on="policy_id", how="left")

    # 3. Resolve vehicle_id from silver.vehicle using customer_id
    vehicle_df = spark.table("silver.vehicle").select("customer_id", "vehicle_id").distinct()
    c_with_veh_id = c_with_p.join(vehicle_df, on=F.col("p.customer_id") == vehicle_df["customer_id"], how="left").drop(vehicle_df["customer_id"])

    # 4. Read conformed dimensions
    dim_policy = spark.table("gold.dim_policy").select("policy_key", "policy_id")
    dim_creason = spark.table("gold.dim_cancellation_reason").select("cancellation_reason_key", "cancellation_reason")

    dim_customer = spark.table("gold.dim_customer").select("customer_key", "customer_id", "effective_from", "effective_to")
    dim_provider = spark.table("gold.dim_provider").select("provider_key", "provider_code", "effective_from", "effective_to")
    dim_vehicle = spark.table("gold.dim_vehicle").select("vehicle_key", "vehicle_id", "effective_from", "effective_to")

    # 5. Perform joins to resolve keys
    joined_df = c_with_veh_id.alias("j") \
        .join(dim_policy.alias("dpol"), on=F.col("j.policy_id") == F.col("dpol.policy_id"), how="left") \
        .join(dim_creason.alias("dcr"), on=F.col("j.cancellation_reason") == F.col("dcr.cancellation_reason"), how="left") \
        .join(dim_customer.alias("dc"), on=(F.col("j.customer_id") == F.col("dc.customer_id")) & (F.col("j.cancellation_at").between(F.col("dc.effective_from"), F.col("dc.effective_to"))), how="left") \
        .join(dim_provider.alias("dpr"), on=(F.col("j.provider_code") == F.col("dpr.provider_code")) & (F.col("j.cancellation_at").between(F.col("dpr.effective_from"), F.col("dpr.effective_to"))), how="left") \
        .join(dim_vehicle.alias("dv"), on=(F.col("j.vehicle_id") == F.col("dv.vehicle_id")) & (F.col("j.cancellation_at").between(F.col("dv.effective_from"), F.col("dv.effective_to"))), how="left")

    # 6. Format final dataset
    final_df = joined_df.select(
        F.col("j.cancellation_id"),
        F.col("j.policy_id"),
        F.coalesce(F.col("dpol.policy_key"), F.lit(-1)).alias("policy_key"),
        F.coalesce(F.col("dcr.cancellation_reason_key"), F.lit(-1)).alias("cancellation_reason_key"),
        F.coalesce(F.date_format(F.col("j.cancellation_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("cancellation_date_key"),
        F.coalesce(F.col("dc.customer_key"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("dpr.provider_key"), F.lit(-1)).alias("provider_key"),
        F.coalesce(F.col("dv.vehicle_key"), F.lit(-1)).alias("vehicle_key"),
        # Soft delete: zero refund if deleted
        F.when(F.col("j.is_deleted") == True, F.lit(0.00)).otherwise(F.coalesce(F.col("j.refund_amount"), F.lit(0.00))).alias("refund_amount"),
        # Metadata / Lineage columns
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
        F.col("j._batch_id").alias("_batch_id"),
        F.col("j._source_system").alias("_source_system"),
        F.lit(session_id).alias("pipeline_run_id"),
        F.coalesce(F.col("j.is_deleted"), F.lit(False)).alias("is_deleted"),
        F.when(F.col("j.is_deleted") == True, F.current_timestamp()).alias("deleted_at"),
        F.when(F.col("j.is_deleted") == True, F.lit(str(batch_id))).alias("delete_batch_id")
    )

    # 7. Merge into Target Delta Table on cancellation_id
    delta_table = DeltaTable.forName(spark, FACT_TABLE_NAME)
    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.cancellation_id = source.cancellation_id"
    ).whenMatchedUpdate(
        set={
            "policy_id": "source.policy_id",
            "policy_key": "source.policy_key",
            "cancellation_reason_key": "source.cancellation_reason_key",
            "cancellation_date_key": "source.cancellation_date_key",
            "customer_key": "source.customer_key",
            "provider_key": "source.provider_key",
            "vehicle_key": "source.vehicle_key",
            "refund_amount": "source.refund_amount",
            "updated_at": "current_timestamp()",
            "_batch_id": "source._batch_id",
            "_source_system": "source._source_system",
            "pipeline_run_id": "source.pipeline_run_id",
            "is_deleted": "source.is_deleted",
            "deleted_at": "source.deleted_at",
            "delete_batch_id": "source.delete_batch_id"
        }
    ).whenNotMatchedInsertAll().execute()

    # Log metrics
    total_count = spark.table(FACT_TABLE_NAME).count()
    source_count = final_df.count()

    finish_table_layer(
        table_session_id=table_session_id,
        layer="GOLD",
        status="SUCCESS",
        is_final_table_step=True,
        source_row_count=source_count,
        target_row_count=total_count,
        inserted_row=source_count,
        updated_row=0,
        deleted_row=0,
        rejected_row=0
    )
    print(f"[SUCCESS] Loaded {FACT_TABLE_NAME} successfully. Total count: {total_count}")

except Exception as err:
    print(f"[ERROR] Failed to load {FACT_TABLE_NAME}: {err}")
    finish_table_layer(
        table_session_id=table_session_id,
        layer="GOLD",
        status="FAILED",
        error_code="FACT_CANCELLATION_LOAD_FAILED",
        error_message=str(err)[:1000]
    )
    raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
