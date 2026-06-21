# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f6154ec7-4dbf-44f7-a335-159149f2ae56",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "c86fdecc-7ed1-42f4-9ec0-4b0274a76958",
# META       "known_lakehouses": [
# META         {
# META           "id": "f6154ec7-4dbf-44f7-a335-159149f2ae56"
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
run_mode = ""
p_table_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()

DIM_FACT_TABLE_ID = int(p_table_id)
row = spark.table("cfg.dim_fact_table").filter(F.col("id") == F.lit(DIM_FACT_TABLE_ID)).select("table_name").collect()
if not row:
    raise ValueError(f"Table ID {DIM_FACT_TABLE_ID} not found in cfg.dim_fact_table")
FACT_TABLE_NAME = f"gold.{row[0]['table_name']}"

# Check if table should process in this run
if not should_process_table_layer(batch_id, DIM_FACT_TABLE_ID, "GOLD"):
    print(f"[BYPASS] Table {FACT_TABLE_NAME} was already successfully processed in this batch. Skipping.")
    mssparkutils.notebook.exit("Bypassed")

# Start Table Ingestion Session
table_session_id = start_table_layer(
    session_id=session_id,
    source_table_id=DIM_FACT_TABLE_ID,
    source_table_name=FACT_TABLE_NAME.split(".")[-1],
    layer="GOLD",
    batch_id=batch_id,
    load_type="INCREMENTAL"
)

try:
    # 1. Read silver.payment filtered by batch_id
    pay_df = spark.table("silver.payment").where(F.col("_batch_id") == F.lit(str(batch_id)))

    # 2. Join with silver.policy for parent context
    p_df = spark.table("silver.policy").select("policy_id", "issued_at", "customer_id", "provider_code")
    pay_with_p = pay_df.alias("pay").join(p_df.alias("p"), on="policy_id", how="left")

    # 3. Resolve vehicle_id from silver.vehicle using customer_id (1-to-1 assumption)
    vehicle_df = spark.table("silver.vehicle").select("customer_id", "vehicle_id").dropDuplicates(["customer_id"])
    pay_with_veh_id = pay_with_p.join(vehicle_df, on=F.col("p.customer_id") == vehicle_df["customer_id"], how="left").drop(vehicle_df["customer_id"])

    # 4. Conform payment method value
    pay_conformed_method = pay_with_veh_id.withColumn(
        "conformed_payment_method",
        F.when(F.col("pay.payment_method") == "Bank Transfer", "BANK_TRANSFER") \
         .when(F.col("pay.payment_method") == "Credit Card", "CREDIT_CARD") \
         .when(F.col("pay.payment_method") == "E-wallet", "E_WALLET") \
         .otherwise(F.upper(F.col("pay.payment_method")))
    )

    # 5. Read conformed dimensions
    dim_policy = spark.table("gold.dim_policy").select("policy_key", "policy_id")
    dim_pstatus = spark.table("gold.dim_payment_status").select("payment_status_key", "payment_status_code")
    dim_pmethod = spark.table("gold.dim_payment_method").select("payment_method_key", "payment_method_code")

    dim_customer = spark.table("gold.dim_customer").select("customer_key", "customer_id", "effective_from", "effective_to")
    dim_provider = spark.table("gold.dim_provider").select("provider_key", "provider_code", "effective_from", "effective_to")
    dim_vehicle = spark.table("gold.dim_vehicle").select("vehicle_key", "vehicle_id", "effective_from", "effective_to")

    # 6. Perform joins to resolve keys
    joined_df = pay_conformed_method.alias("j") \
        .join(dim_policy.alias("dpol"), on=F.col("j.policy_id") == F.col("dpol.policy_id"), how="left") \
        .join(dim_pstatus.alias("dps"), on=F.col("j.payment_status") == F.col("dps.payment_status_code"), how="left") \
        .join(dim_pmethod.alias("dpm"), on=F.col("j.conformed_payment_method") == F.col("dpm.payment_method_code"), how="left") \
        .join(dim_customer.alias("dc"), on=(F.col("j.customer_id") == F.col("dc.customer_id")) & (F.col("j.payment_at").between(F.col("dc.effective_from"), F.col("dc.effective_to"))), how="left") \
        .join(dim_provider.alias("dpr"), on=(F.col("j.provider_code") == F.col("dpr.provider_code")) & (F.col("j.payment_at").between(F.col("dpr.effective_from"), F.col("dpr.effective_to"))), how="left") \
        .join(dim_vehicle.alias("dv"), on=(F.col("j.vehicle_id") == F.col("dv.vehicle_id")) & (F.col("j.payment_at").between(F.col("dv.effective_from"), F.col("dv.effective_to"))), how="left")

    # 7. Format final dataset
    final_df = joined_df.select(
        F.col("j.payment_id"),
        F.col("j.policy_id"),
        F.col("j.transaction_reference"),
        F.coalesce(F.col("dpol.policy_key"), F.lit(-1)).alias("policy_key"),
        F.coalesce(F.col("dps.payment_status_key"), F.lit(-1)).alias("payment_status_key"),
        F.coalesce(F.col("dpm.payment_method_key"), F.lit(-1)).alias("payment_method_key"),
        F.coalesce(F.date_format(F.col("j.payment_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("payment_date_key"),
        F.coalesce(F.date_format(F.col("j.issued_at"), "yyyyMMdd").cast(IntegerType()), F.lit(-1)).alias("issued_date_key"),
        F.coalesce(F.col("dc.customer_key"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("dpr.provider_key"), F.lit(-1)).alias("provider_key"),
        F.coalesce(F.col("dv.vehicle_key"), F.lit(-1)).alias("vehicle_key"),
        # Soft delete: zero payment amount if deleted
        F.when(F.col("j.is_deleted") == True, F.lit(0.00)).otherwise(F.coalesce(F.col("j.payment_amount"), F.lit(0.00))).alias("payment_amount"),
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

    # 8. Merge into Target Delta Table on payment_id
    delta_table = DeltaTable.forName(spark, FACT_TABLE_NAME)
    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.payment_id = source.payment_id"
    ).whenMatchedUpdate(
        set={
            "policy_id": "source.policy_id",
            "transaction_reference": "source.transaction_reference",
            "policy_key": "source.policy_key",
            "payment_status_key": "source.payment_status_key",
            "payment_method_key": "source.payment_method_key",
            "payment_date_key": "source.payment_date_key",
            "issued_date_key": "source.issued_date_key",
            "customer_key": "source.customer_key",
            "provider_key": "source.provider_key",
            "vehicle_key": "source.vehicle_key",
            "payment_amount": "source.payment_amount",
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
        error_code=f"{FACT_TABLE_NAME.split('.')[-1].upper()}_LOAD_FAILED",
        error_message=str(err)[:1000]
    )
    raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
