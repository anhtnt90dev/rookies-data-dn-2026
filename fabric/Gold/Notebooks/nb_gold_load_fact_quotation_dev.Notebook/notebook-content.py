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

FACT_TABLE_NAME = "gold.fact_quotation"
DIM_FACT_TABLE_ID = 15

# Check if table should process in this run
if not should_process_table_layer(batch_id, DIM_FACT_TABLE_ID, "GOLD"):
    print(f"[BYPASS] Table {FACT_TABLE_NAME} was already successfully processed in this batch. Skipping.")
    mssparkutils.notebook.exit("Bypassed")

# Start Table Ingestion Session
table_session_id = start_table_layer(
    session_id=session_id,
    source_table_id=DIM_FACT_TABLE_ID,
    source_table_name="fact_quotation",
    layer="GOLD",
    batch_id=batch_id,
    load_type="INCREMENTAL"
)

try:
    # 1. Read silver.quotation filtered by batch_id
    q_df = spark.table("silver.quotation").where(F.col("_batch_id") == F.lit(str(batch_id)))

    # 2. Get converted_flag from silver.policy
    policy_df = spark.table("silver.policy").select("quotation_id").distinct().withColumn("has_policy", F.lit(True))
    q_with_conv = q_df.join(policy_df, on="quotation_id", how="left")

    # 3. Resolve vehicle_id from silver.vehicle using customer_id (1-to-1 assumption)
    vehicle_df = spark.table("silver.vehicle").select("customer_id", "vehicle_id").dropDuplicates(["customer_id"])
    q_with_veh_id = q_with_conv.join(vehicle_df, on="customer_id", how="left")

    # 4. Read target dimension tables
    dim_quotation = spark.table("gold.dim_quotation").select("quotation_key", "quotation_id")
    dim_package = spark.table("gold.dim_package").select("package_key", "package_code")
    dim_qstatus = spark.table("gold.dim_quotation_status").select("quotation_status_key", "quotation_status_code")

    dim_customer = spark.table("gold.dim_customer").select("customer_key", "customer_id", "effective_from", "effective_to")
    dim_agent = spark.table("gold.dim_agent").select("agent_key", "agent_id", "effective_from", "effective_to")
    dim_provider = spark.table("gold.dim_provider").select("provider_key", "provider_code", "effective_from", "effective_to")
    dim_vehicle = spark.table("gold.dim_vehicle").select("vehicle_key", "vehicle_id", "effective_from", "effective_to")

    # 5. Perform joins to resolve keys
    joined_df = q_with_veh_id.alias("q") \
        .join(dim_quotation.alias("dq"), on=F.col("q.quotation_id") == F.col("dq.quotation_id"), how="left") \
        .join(dim_package.alias("dp"), on=F.col("q.package_code") == F.col("dp.package_code"), how="left") \
        .join(dim_qstatus.alias("dqs"), on=F.col("q.quotation_status") == F.col("dqs.quotation_status_code"), how="left") \
        .join(dim_customer.alias("dc"), on=(F.col("q.customer_id") == F.col("dc.customer_id")) & (F.col("q.quotation_at").between(F.col("dc.effective_from"), F.col("dc.effective_to"))), how="left") \
        .join(dim_agent.alias("da"), on=(F.col("q.agent_id") == F.col("da.agent_id")) & (F.col("q.quotation_at").between(F.col("da.effective_from"), F.col("da.effective_to"))), how="left") \
        .join(dim_provider.alias("dpr"), on=(F.col("q.provider_code") == F.col("dpr.provider_code")) & (F.col("q.quotation_at").between(F.col("dpr.effective_from"), F.col("dpr.effective_to"))), how="left") \
        .join(dim_vehicle.alias("dv"), on=(F.col("q.vehicle_id") == F.col("dv.vehicle_id")) & (F.col("q.quotation_at").between(F.col("dv.effective_from"), F.col("dv.effective_to"))), how="left")

    # 6. Format final dataset
    final_df = joined_df.select(
        F.col("q.quotation_id"),
        F.col("q.customer_id"),
        F.col("q.agent_id"),
        F.col("q.provider_code"),
        F.coalesce(F.col("dq.quotation_key"), F.lit(-1)).alias("quotation_key"),
        F.coalesce(F.col("dc.customer_key"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("da.agent_key"), F.lit(-1)).alias("agent_key"),
        F.coalesce(F.col("dpr.provider_key"), F.lit(-1)).alias("provider_key"),
        F.coalesce(F.col("dp.package_key"), F.lit(-1)).alias("package_key"),
        F.coalesce(F.col("dqs.quotation_status_key"), F.lit(-1)).alias("quotation_status_key"),
        F.coalesce(F.date_format(F.col("q.quotation_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("quotation_date_key"),
        F.coalesce(F.date_format(F.col("q.quotation_expiry_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("quotation_expiry_date_key"),
        F.coalesce(F.col("dv.vehicle_key"), F.lit(-1)).alias("vehicle_key"),
        # Soft delete: zero premium if deleted (defaults to False for quotation)
        F.coalesce(F.col("q.premium_amount"), F.lit(0.00)).alias("premium_amount"),
        F.coalesce(F.col("q.has_policy"), F.lit(False)).alias("converted_flag"),
        # Metadata / Lineage columns
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
        F.col("q._batch_id").alias("_batch_id"),
        F.col("q._source_system").alias("_source_system"),
        F.lit(session_id).alias("pipeline_run_id"),
        F.lit(False).alias("is_deleted"),
        F.lit(None).cast("timestamp").alias("deleted_at"),
        F.lit(None).cast("string").alias("delete_batch_id")
    )

    # 7. Merge into Target Delta Table
    delta_table = DeltaTable.forName(spark, FACT_TABLE_NAME)
    
    # Delta merge match on quotation_id
    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.quotation_id = source.quotation_id"
    ).whenMatchedUpdate(
        set={
            "customer_id": "source.customer_id",
            "agent_id": "source.agent_id",
            "provider_code": "source.provider_code",
            "quotation_key": "source.quotation_key",
            "customer_key": "source.customer_key",
            "agent_key": "source.agent_key",
            "provider_key": "source.provider_key",
            "package_key": "source.package_key",
            "quotation_status_key": "source.quotation_status_key",
            "quotation_date_key": "source.quotation_date_key",
            "quotation_expiry_date_key": "source.quotation_expiry_date_key",
            "vehicle_key": "source.vehicle_key",
            "premium_amount": "source.premium_amount",
            "converted_flag": "source.converted_flag",
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
        inserted_row=source_count, # Merge can insert or update, we track source rows as upper bound
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
        error_code="FACT_QUOTATION_LOAD_FAILED",
        error_message=str(err)[:1000]
    )
    raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
