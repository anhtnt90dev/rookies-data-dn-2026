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
run_mode = ""
p_table_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()
p_table_id = int(p_table_id) if p_table_id else None

# Load table ID mapping from cfg.dim_fact_table dynamically
dim_fact_lookup = {
    row["table_name"]: int(row["id"])
    for row in spark.table("cfg.dim_fact_table").select("id", "table_name").collect()
}

# Ensure Unknown Row (-1) helper
def ensure_unknown_row(table_name: str, pk_col: str):
    exists = spark.table(table_name).where(F.col(pk_col) == -1).count() > 0
    if not exists:
        schema = spark.table(table_name).schema
        row_dict = {}
        for field in schema.fields:
            col_name = field.name
            col_type = field.dataType
            if col_name == pk_col:
                row_dict[col_name] = -1
            elif col_name in ("created_at", "updated_at"):
                row_dict[col_name] = datetime.now()
            elif str(col_type).startswith("StringType"):
                row_dict[col_name] = "Unknown"
            else:
                row_dict[col_name] = None
        
        unknown_df = spark.createDataFrame([row_dict], schema)
        unknown_df.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"[INFO] Seeded Unknown row (-1) into {table_name}")

# SCD Type 1 Loader function
def load_scd1_dimension(
    target_table_name: str,
    source_df: DataFrame,
    business_key_col: str,
    surrogate_key_col: str,
    attr_cols: list[str],
    dim_fact_table_id: int
):
    # Check if table should process in this run
    if not should_process_table_layer(batch_id, dim_fact_table_id, "GOLD"):
        print(f"[BYPASS] Table {target_table_name} was already successfully processed in this batch. Skipping.")
        return

    table_session_id = start_table_layer(
        session_id=session_id,
        source_table_id=dim_fact_table_id,
        source_table_name=target_table_name.split(".")[-1],
        layer="GOLD",
        batch_id=batch_id,
        load_type="INCREMENTAL"
    )

    try:
        # 1. Ensure Unknown row (-1)
        ensure_unknown_row(target_table_name, surrogate_key_col)

        # 2. Get current maximum surrogate key
        max_key = spark.table(target_table_name).where(F.col(surrogate_key_col) != -1).agg(F.max(surrogate_key_col)).collect()[0][0]
        max_key = int(max_key) if max_key is not None else 0

        # 3. Clean and deduplicate source data
        clean_source_df = source_df.where(F.col(business_key_col).isNotNull() & (F.trim(F.col(business_key_col)) != ""))
        dedup_source_df = clean_source_df.dropDuplicates([business_key_col])

        # 4. Read target keys and attributes
        target_keys_df = spark.table(target_table_name).select(surrogate_key_col, business_key_col, *attr_cols)

        # 5. Mapped source
        source_mapped = dedup_source_df.select(
            F.col(business_key_col).alias("src_" + business_key_col),
            *[F.col(c).alias("src_" + c) for c in attr_cols]
        )

        # 6. Join target keys to source to find new vs existing
        merged_prep = source_mapped.join(
            target_keys_df,
            on=F.col("src_" + business_key_col) == F.col(business_key_col),
            how="left"
        )

        new_records_only = merged_prep.filter(F.col(surrogate_key_col).isNull())
        window_spec = Window.orderBy("src_" + business_key_col)
        new_records_with_keys = new_records_only.withColumn(
            "resolved_key",
            F.lit(max_key) + F.row_number().over(window_spec).cast("bigint")
        ).withColumn("is_new", F.lit(True))

        existing_records = merged_prep.filter(F.col(surrogate_key_col).isNotNull())\
                                      .withColumn("resolved_key", F.col(surrogate_key_col))\
                                      .withColumn("is_new", F.lit(False))

        union_prep = new_records_with_keys.unionByName(existing_records)

        final_merge_df = union_prep.select(
            F.col("resolved_key").alias(surrogate_key_col),
            F.col("src_" + business_key_col).alias(business_key_col),
            *[F.col("src_" + c).alias(c) for c in attr_cols],
            F.col("is_new")
        )

        # Delta Merge
        delta_table = DeltaTable.forName(spark, target_table_name)
        match_cond = f"target.{business_key_col} = source.{business_key_col}"

        merge_op = delta_table.alias("target").merge(
            final_merge_df.alias("source"),
            match_cond
        )

        if attr_cols:
            update_cond = " OR ".join([f"COALESCE(target.{c}, '') != COALESCE(source.{c}, '')" for c in attr_cols])
            merge_op = merge_op.whenMatchedUpdate(
                condition=update_cond,
                set={
                    **{c: f"source.{c}" for c in attr_cols},
                    "updated_at": "current_timestamp()"
                }
            )

        merge_op = merge_op.whenNotMatchedInsert(
            values={
                surrogate_key_col: f"source.{surrogate_key_col}",
                business_key_col: f"source.{business_key_col}",
                **{c: f"source.{c}" for c in attr_cols},
                "created_at": "current_timestamp()",
                "updated_at": "current_timestamp()"
            }
        ).execute()

        # Update stats
        total_count = spark.table(target_table_name).count()
        inserted_count = final_merge_df.filter(F.col("is_new") == True).count()
        updated_count = final_merge_df.filter(F.col("is_new") == False).count()

        finish_table_layer(
            table_session_id=table_session_id,
            layer="GOLD",
            status="SUCCESS",
            is_final_table_step=True,
            source_row_count=dedup_source_df.count(),
            target_row_count=total_count,
            inserted_row=inserted_count,
            updated_row=updated_count,
            deleted_row=0,
            rejected_row=0
        )
        print(f"[INFO] Completed load for {target_table_name}. Inserts: {inserted_count}, Updates upper bound: {updated_count}")

    except Exception as err:
        print(f"[ERROR] Failed to load {target_table_name}: {err}")
        finish_table_layer(
            table_session_id=table_session_id,
            layer="GOLD",
            status="FAILED",
            error_code="SCD1_LOAD_FAILED",
            error_message=str(err)[:1000]
        )
        raise err

# ---------------------------------------------------------------------------
# Dimension Loads Sequenced Execution
# ---------------------------------------------------------------------------

# 1. dim_package (Source: silver.quotation)
dim_package_id = dim_fact_lookup["dim_package"]
if p_table_id is None or p_table_id == dim_package_id:
    print("[LOAD] Processing dim_package...")
    package_source_df = spark.table("silver.quotation").select("package_code")
    load_scd1_dimension("gold.dim_package", package_source_df, "package_code", "package_key", [], dim_package_id)

# 2. dim_coverage (Source: silver.quotation_item)
dim_coverage_id = dim_fact_lookup["dim_coverage"]
if p_table_id is None or p_table_id == dim_coverage_id:
    print("[LOAD] Processing dim_coverage...")
    coverage_source_df = spark.table("silver.quotation_item").select("coverage_type")
    load_scd1_dimension("gold.dim_coverage", coverage_source_df, "coverage_type", "coverage_key", [], dim_coverage_id)

# 3. dim_quotation (Source: silver.quotation)
dim_quotation_id = dim_fact_lookup["dim_quotation"]
if p_table_id is None or p_table_id == dim_quotation_id:
    print("[LOAD] Processing dim_quotation...")
    quotation_source_df = spark.table("silver.quotation").select(
        "quotation_id",
        F.to_date("quotation_expiry_at").alias("quotation_expiry_date")
    )
    load_scd1_dimension("gold.dim_quotation", quotation_source_df, "quotation_id", "quotation_key", ["quotation_expiry_date"], dim_quotation_id)

# 4. dim_policy (Source: silver.policy)
dim_policy_id = dim_fact_lookup["dim_policy"]
if p_table_id is None or p_table_id == dim_policy_id:
    print("[LOAD] Processing dim_policy...")
    policy_source_df = spark.table("silver.policy").select("policy_id")
    load_scd1_dimension("gold.dim_policy", policy_source_df, "policy_id", "policy_key", [], dim_policy_id)

# 5. dim_quotation_status (Source: silver.quotation)
dim_qstatus_id = dim_fact_lookup["dim_quotation_status"]
if p_table_id is None or p_table_id == dim_qstatus_id:
    print("[LOAD] Processing dim_quotation_status...")
    qstatus_source_df = spark.table("silver.quotation").select(F.col("quotation_status").alias("quotation_status_code"))
    load_scd1_dimension("gold.dim_quotation_status", qstatus_source_df, "quotation_status_code", "quotation_status_key", [], dim_qstatus_id)

# 6. dim_policy_status (Source: silver.policy)
dim_pstatus_id = dim_fact_lookup["dim_policy_status"]
if p_table_id is None or p_table_id == dim_pstatus_id:
    print("[LOAD] Processing dim_policy_status...")
    pstatus_source_df = spark.table("silver.policy").select(F.col("policy_status").alias("policy_status_code"))
    load_scd1_dimension("gold.dim_policy_status", pstatus_source_df, "policy_status_code", "policy_status_key", [], dim_pstatus_id)

# 7. dim_payment_status (Source: silver.payment)
dim_paystatus_id = dim_fact_lookup["dim_payment_status"]
if p_table_id is None or p_table_id == dim_paystatus_id:
    print("[LOAD] Processing dim_payment_status...")
    paystatus_source_df = spark.table("silver.payment").select(F.col("payment_status").alias("payment_status_code"))
    load_scd1_dimension("gold.dim_payment_status", paystatus_source_df, "payment_status_code", "payment_status_key", [], dim_paystatus_id)

# 8. dim_payment_method (Source: silver.payment)
dim_paymethod_id = dim_fact_lookup["dim_payment_method"]
if p_table_id is None or p_table_id == dim_paymethod_id:
    print("[LOAD] Processing dim_payment_method...")
    raw_payment_method_df = spark.table("silver.payment").select("payment_method")
    payment_method_df = raw_payment_method_df.select(
        F.when(F.col("payment_method") == "Bank Transfer", "BANK_TRANSFER")
         .when(F.col("payment_method") == "Credit Card", "CREDIT_CARD")
         .when(F.col("payment_method") == "E-wallet", "E_WALLET")
         .otherwise(F.upper(F.col("payment_method"))).alias("payment_method_code")
    )
    load_scd1_dimension("gold.dim_payment_method", payment_method_df, "payment_method_code", "payment_method_key", [], dim_paymethod_id)

# 9. dim_cancellation_reason (Source: silver.cancellation)
dim_cancelreason_id = dim_fact_lookup["dim_cancellation_reason"]
if p_table_id is None or p_table_id == dim_cancelreason_id:
    print("[LOAD] Processing dim_cancellation_reason...")
    cancel_source_df = spark.table("silver.cancellation").select("cancellation_reason")
    load_scd1_dimension("gold.dim_cancellation_reason", cancel_source_df, "cancellation_reason", "cancellation_reason_key", [], dim_cancelreason_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
