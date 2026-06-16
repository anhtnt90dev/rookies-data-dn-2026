# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf1b63ae-986e-4368-a13e-ed5eed5fd990",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "82a15c8e-ce8d-4f2c-827e-94b17659ecd8",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf1b63ae-986e-4368-a13e-ed5eed5fd990"
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

%run nb_gold_audit_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

session_id = ""
batch_id = ""
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

FACT_TABLE_NAME = "gold.fact_policy"
DIM_FACT_TABLE_ID = 17

# Check if table should process in this run
if not should_process_table_layer(batch_id, DIM_FACT_TABLE_ID, "GOLD"):
    print(f"[BYPASS] Table {FACT_TABLE_NAME} was already successfully processed in this batch. Skipping.")
    mssparkutils.notebook.exit("Bypassed")

# Start Table Ingestion Session
table_session_id = start_table_layer(
    session_id=session_id,
    source_table_id=DIM_FACT_TABLE_ID,
    source_table_name="fact_policy",
    layer="GOLD",
    batch_id=batch_id,
    load_type="INCREMENTAL"
)

try:
    # 1. Read silver.policy filtered by batch_id
    p_df = spark.table("silver.policy").where(F.col("_batch_id") == F.lit(str(batch_id)))

    # 2. Join with silver.quotation for parent context
    q_df = spark.table("silver.quotation").select("quotation_id", "quotation_at", "agent_id", "package_code")
    p_with_q = p_df.alias("p").join(q_df.alias("q"), on="quotation_id", how="left")

    # 3. Resolve vehicle_id from silver.vehicle using customer_id (1-to-1 assumption)
    vehicle_df = spark.table("silver.vehicle").select("customer_id", "vehicle_id").dropDuplicates(["customer_id"])
    p_with_veh_id = p_with_q.join(vehicle_df, on=F.col("p.customer_id") == vehicle_df["customer_id"], how="left").drop(vehicle_df["customer_id"])

    # 4. Read conformed dimensions
    dim_policy = spark.table("gold.dim_policy").select("policy_key", "policy_id")
    dim_quotation = spark.table("gold.dim_quotation").select("quotation_key", "quotation_id")
    dim_package = spark.table("gold.dim_package").select("package_key", "package_code")
    dim_pstatus = spark.table("gold.dim_policy_status").select("policy_status_key", "policy_status_code")

    dim_customer = spark.table("gold.dim_customer").select("customer_key", "customer_id", "effective_from", "effective_to")
    dim_agent = spark.table("gold.dim_agent").select("agent_key", "agent_id", "effective_from", "effective_to")
    dim_provider = spark.table("gold.dim_provider").select("provider_key", "provider_code", "effective_from", "effective_to")
    dim_vehicle = spark.table("gold.dim_vehicle").select("vehicle_key", "vehicle_id", "effective_from", "effective_to")

    # 5. Perform joins to resolve keys
    joined_df = p_with_veh_id.alias("j") \
        .join(dim_policy.alias("dpol"), on=F.col("j.policy_id") == F.col("dpol.policy_id"), how="left") \
        .join(dim_quotation.alias("dq"), on=F.col("j.quotation_id") == F.col("dq.quotation_id"), how="left") \
        .join(dim_package.alias("dpk"), on=F.col("j.package_code") == F.col("dpk.package_code"), how="left") \
        .join(dim_pstatus.alias("dps"), on=F.col("j.policy_status") == F.col("dps.policy_status_code"), how="left") \
        .join(dim_customer.alias("dc"), on=(F.col("j.customer_id") == F.col("dc.customer_id")) & (F.col("j.issued_at").between(F.col("dc.effective_from"), F.col("dc.effective_to"))), how="left") \
        .join(dim_agent.alias("da"), on=(F.col("j.agent_id") == F.col("da.agent_id")) & (F.col("j.quotation_at").between(F.col("da.effective_from"), F.col("da.effective_to"))), how="left") \
        .join(dim_provider.alias("dpr"), on=(F.col("j.provider_code") == F.col("dpr.provider_code")) & (F.col("j.issued_at").between(F.col("dpr.effective_from"), F.col("dpr.effective_to"))), how="left") \
        .join(dim_vehicle.alias("dv"), on=(F.col("j.vehicle_id") == F.col("dv.vehicle_id")) & (F.col("j.issued_at").between(F.col("dv.effective_from"), F.col("dv.effective_to"))), how="left")

    # 6. Format final dataset
    final_df = joined_df.select(
        F.col("j.policy_id"),
        F.col("j.policy_number"),
        F.col("j.quotation_id"),
        F.col("j.customer_id"),
        F.col("j.provider_code"),
        F.coalesce(F.col("dpol.policy_key"), F.lit(-1)).alias("policy_key"),
        F.coalesce(F.col("dq.quotation_key"), F.lit(-1)).alias("quotation_key"),
        F.coalesce(F.col("dc.customer_key"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("dpr.provider_key"), F.lit(-1)).alias("provider_key"),
        F.coalesce(F.col("da.agent_key"), F.lit(-1)).alias("agent_key"),
        F.coalesce(F.col("dpk.package_key"), F.lit(-1)).alias("package_key"),
        F.coalesce(F.col("dps.policy_status_key"), F.lit(-1)).alias("policy_status_key"),
        F.coalesce(F.date_format(F.col("j.issued_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("issued_date_key"),
        F.coalesce(F.date_format(F.col("j.policy_start_date"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("policy_start_date_key"),
        F.coalesce(F.date_format(F.col("j.policy_end_date"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("policy_end_date_key"),
        F.coalesce(F.col("dv.vehicle_key"), F.lit(-1)).alias("vehicle_key"),
        # Soft delete: zero premium if deleted
        F.when(F.col("j.is_deleted") == True, F.lit(0.00)).otherwise(F.coalesce(F.col("j.premium_amount"), F.lit(0.00))).alias("premium_amount"),
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

    # 7. Merge into Target Delta Table on policy_id
    delta_table = DeltaTable.forName(spark, FACT_TABLE_NAME)
    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.policy_id = source.policy_id"
    ).whenMatchedUpdate(
        set={
            "policy_number": "source.policy_number",
            "quotation_id": "source.quotation_id",
            "customer_id": "source.customer_id",
            "provider_code": "source.provider_code",
            "policy_key": "source.policy_key",
            "quotation_key": "source.quotation_key",
            "customer_key": "source.customer_key",
            "provider_key": "source.provider_key",
            "agent_key": "source.agent_key",
            "package_key": "source.package_key",
            "policy_status_key": "source.policy_status_key",
            "issued_date_key": "source.issued_date_key",
            "policy_start_date_key": "source.policy_start_date_key",
            "policy_end_date_key": "source.policy_end_date_key",
            "vehicle_key": "source.vehicle_key",
            "premium_amount": "source.premium_amount",
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
        error_code="FACT_POLICY_LOAD_FAILED",
        error_message=str(err)[:1000]
    )
    raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
