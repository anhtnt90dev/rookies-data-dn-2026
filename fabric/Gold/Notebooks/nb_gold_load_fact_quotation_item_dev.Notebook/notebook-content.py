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

FACT_TABLE_NAME = "gold.fact_quotation_item"
DIM_FACT_TABLE_ID = 16

# Check if table should process in this run
if not should_process_table_layer(batch_id, DIM_FACT_TABLE_ID, "GOLD"):
    print(f"[BYPASS] Table {FACT_TABLE_NAME} was already successfully processed in this batch. Skipping.")
    mssparkutils.notebook.exit("Bypassed")

# Start Table Ingestion Session
table_session_id = start_table_layer(
    session_id=session_id,
    source_table_id=DIM_FACT_TABLE_ID,
    source_table_name="fact_quotation_item",
    layer="GOLD",
    batch_id=batch_id,
    load_type="INCREMENTAL"
)

try:
    # 1. Read silver.quotation_item filtered by batch_id
    qi_df = spark.table("silver.quotation_item").where(F.col("_batch_id") == F.lit(str(batch_id)))

    # 2. Join with silver.quotation for parent context (selecting specific columns to avoid ambiguous references)
    q_df = spark.table("silver.quotation").select(
        "quotation_id",
        "customer_id",
        "agent_id",
        "provider_code",
        "quotation_at",
        "quotation_status",
        "package_code"
    )
    qi_with_parent = qi_df.alias("qi").join(
        q_df.alias("q"),
        on="quotation_id",
        how="inner"
    )

    # 3. Resolve vehicle_id from silver.vehicle using customer_id (1-to-1 assumption)
    vehicle_df = spark.table("silver.vehicle").select("customer_id", "vehicle_id").dropDuplicates(["customer_id"])
    qi_with_veh_id = qi_with_parent.join(vehicle_df, on=F.col("q.customer_id") == vehicle_df["customer_id"], how="left").drop(vehicle_df["customer_id"])

    # 4. Read conformed dimensions
    dim_quotation = spark.table("gold.dim_quotation").select("quotation_key", "quotation_id")
    dim_package = spark.table("gold.dim_package").select("package_key", "package_code")
    dim_coverage = spark.table("gold.dim_coverage").select("coverage_key", "coverage_type")
    dim_qstatus = spark.table("gold.dim_quotation_status").select("quotation_status_key", "quotation_status_code")

    dim_customer = spark.table("gold.dim_customer").select("customer_key", "customer_id", "effective_from", "effective_to")
    dim_agent = spark.table("gold.dim_agent").select("agent_key", "agent_id", "effective_from", "effective_to")
    dim_provider = spark.table("gold.dim_provider").select("provider_key", "provider_code", "effective_from", "effective_to")
    dim_vehicle = spark.table("gold.dim_vehicle").select("vehicle_key", "vehicle_id", "effective_from", "effective_to")

    # 5. Perform joins to resolve keys
    joined_df = qi_with_veh_id.alias("j") \
        .join(dim_quotation.alias("dq"), on=F.col("j.quotation_id") == F.col("dq.quotation_id"), how="left") \
        .join(dim_package.alias("dp"), on=F.col("j.package_code") == F.col("dp.package_code"), how="left") \
        .join(dim_coverage.alias("dcov"), on=F.col("j.coverage_type") == F.col("dcov.coverage_type"), how="left") \
        .join(dim_qstatus.alias("dqs"), on=F.col("j.quotation_status") == F.col("dqs.quotation_status_code"), how="left") \
        .join(dim_customer.alias("dc"), on=(F.col("j.customer_id") == F.col("dc.customer_id")) & (F.col("j.quotation_at").between(F.col("dc.effective_from"), F.col("dc.effective_to"))), how="left") \
        .join(dim_agent.alias("da"), on=(F.col("j.agent_id") == F.col("da.agent_id")) & (F.col("j.quotation_at").between(F.col("da.effective_from"), F.col("da.effective_to"))), how="left") \
        .join(dim_provider.alias("dpr"), on=(F.col("j.provider_code") == F.col("dpr.provider_code")) & (F.col("j.quotation_at").between(F.col("dpr.effective_from"), F.col("dpr.effective_to"))), how="left") \
        .join(dim_vehicle.alias("dv"), on=(F.col("j.vehicle_id") == F.col("dv.vehicle_id")) & (F.col("j.quotation_at").between(F.col("dv.effective_from"), F.col("dv.effective_to"))), how="left")

    # 6. Format final dataset
    final_df = joined_df.select(
        F.col("j.quotation_item_id"),
        F.col("j.quotation_id"),
        F.coalesce(F.col("dq.quotation_key"), F.lit(-1)).alias("quotation_key"),
        F.coalesce(F.date_format(F.col("j.quotation_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("quotation_date_key"),
        F.coalesce(F.col("dc.customer_key"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("da.agent_key"), F.lit(-1)).alias("agent_key"),
        F.coalesce(F.col("dpr.provider_key"), F.lit(-1)).alias("provider_key"),
        F.coalesce(F.col("dp.package_key"), F.lit(-1)).alias("package_key"),
        F.coalesce(F.col("dqs.quotation_status_key"), F.lit(-1)).alias("quotation_status_key"),
        F.coalesce(F.col("dcov.coverage_key"), F.lit(-1)).alias("coverage_key"),
        F.coalesce(F.col("dv.vehicle_key"), F.lit(-1)).alias("vehicle_key"),
        # Soft delete: zero metrics if deleted (defaults to False for quotation items)
        F.coalesce(F.col("j.coverage_amount"), F.lit(0.00)).alias("coverage_amount"),
        F.coalesce(F.col("j.deductible_amount"), F.lit(0.00)).alias("deductible_amount"),
        # Metadata / Lineage columns
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
        F.col("j._batch_id").alias("_batch_id"),
        F.col("j._source_system").alias("_source_system"),
        F.lit(session_id).alias("pipeline_run_id"),
        F.lit(False).alias("is_deleted"),
        F.lit(None).cast("timestamp").alias("deleted_at"),
        F.lit(None).cast("string").alias("delete_batch_id")
    )

    # 7. Merge into Target Delta Table on (quotation_id, coverage_key)
    delta_table = DeltaTable.forName(spark, FACT_TABLE_NAME)
    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.quotation_id = source.quotation_id AND target.coverage_key = source.coverage_key"
    ).whenMatchedUpdate(
        set={
            "quotation_item_id": "source.quotation_item_id",
            "quotation_key": "source.quotation_key",
            "quotation_date_key": "source.quotation_date_key",
            "customer_key": "source.customer_key",
            "agent_key": "source.agent_key",
            "provider_key": "source.provider_key",
            "package_key": "source.package_key",
            "quotation_status_key": "source.quotation_status_key",
            "vehicle_key": "source.vehicle_key",
            "coverage_amount": "source.coverage_amount",
            "deductible_amount": "source.deductible_amount",
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
        error_code="FACT_QUOTATION_ITEM_LOAD_FAILED",
        error_message=str(err)[:1000]
    )
    raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
