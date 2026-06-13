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

# -------------------------------------------------------------------------
# CELL 1: IMPORTS AND PARAMETERS
# -------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, coalesce, lit, row_number, expr, to_date
from delta.tables import DeltaTable

# Parameters
batch_id = globals().get("batch_id", 1001)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 2: SCD TYPE 1 GENERIC LOAD FUNCTION
# -------------------------------------------------------------------------
def load_scd1_dimension(
    source_df,
    target_table_name,
    surrogate_key_col,
    business_key_col,
    attribute_cols=None
) -> None:
    """
    Loads data into a Gold SCD Type 1 dimension table idempotently.
    - source_df: Standardized, deduplicated source DataFrame containing business_key_col and attribute_cols.
    - target_table_name: Fully qualified target table name (e.g., 'gold.dim_package').
    - surrogate_key_col: Target surrogate key column name (e.g., 'package_key').
    - business_key_col: Target business key column name (e.g., 'package_code').
    - attribute_cols: List of other attributes to update/insert.
    """
    if attribute_cols is None:
        attribute_cols = []

    print(f"[INFO] Ingesting SCD Type 1 dimension: {target_table_name}")
    
    # 1. Get target Delta table
    if not spark.catalog.tableExists(target_table_name):
        raise ValueError(f"Target table '{target_table_name}' does not exist.")
        
    target_table = spark.table(target_table_name)

    # 2. Exclude -1 Unknown key for finding max surrogate key
    existing_records = target_table.filter(col(surrogate_key_col) != -1)

    # 3. Identify new records (present in source, not in target)
    new_records = source_df.join(
        existing_records.select(business_key_col),
        on=business_key_col,
        how="left_anti"
    )

    # 4. Generate surrogate keys for new records
    max_key_row = existing_records.agg({surrogate_key_col: "max"}).collect()[0][0]
    max_key = int(max_key_row) if max_key_row is not None else 0
    if max_key < 0:
        max_key = 0

    window_spec = Window.orderBy(business_key_col)
    new_records_with_keys = new_records.withColumn(
        surrogate_key_col,
        row_number().over(window_spec).cast("long") + max_key
    )

    # 5. Union new records with existing records schema-compatibly
    # Create the merge dataset source
    merge_source = source_df.join(
        target_table.select(business_key_col, surrogate_key_col),
        on=business_key_col,
        how="left"
    ).join(
        new_records_with_keys.select(business_key_col, col(surrogate_key_col).alias("gen_key")),
        on=business_key_col,
        how="left"
    ).withColumn(
        surrogate_key_col,
        coalesce(col(surrogate_key_col), col("gen_key"))
    ).drop("gen_key")

    # 6. Execute Delta Merge
    delta_target = DeltaTable.forName(spark, target_table_name)

    # Build expressions for Merge
    update_expr = {surrogate_key_col: f"source.{surrogate_key_col}"}
    for attr in attribute_cols:
        update_expr[attr] = f"source.{attr}"
    update_expr["updated_at"] = "current_timestamp()"

    insert_expr = {
        surrogate_key_col: f"source.{surrogate_key_col}",
        business_key_col: f"source.{business_key_col}"
    }
    for attr in attribute_cols:
        insert_expr[attr] = f"source.{attr}"
    insert_expr["created_at"] = "current_timestamp()"
    insert_expr["updated_at"] = "current_timestamp()"

    merge_builder = delta_target.alias("target").merge(
        merge_source.alias("source"),
        f"target.{business_key_col} = source.{business_key_col}"
    )

    if attribute_cols:
        # Build change detection condition for Type 1 updates
        changed_cond = " OR ".join([
            f"coalesce(target.{attr}, '') != coalesce(source.{attr}, '')"
            for attr in attribute_cols
        ])
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=changed_cond,
            set=update_expr
        )
    
    merge_builder = merge_builder.whenNotMatchedInsert(
        values=insert_expr
    )

    merge_builder.execute()
    print(f"[SUCCESS] Ingested {target_table_name} successfully.\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 3: RUN SCD TYPE 1 LOADS
# -------------------------------------------------------------------------
def run_all_scd1_dimensions() -> None:
    # --- 1. dim_package ---
    package_src = (
        spark.table("silver.quotation")
        .select(col("package_code"))
        .filter(col("package_code").isNotNull() & (col("package_code") != "UNKNOWN"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=package_src,
        target_table_name="gold.dim_package",
        surrogate_key_col="package_key",
        business_key_col="package_code"
    )

    # --- 2. dim_coverage ---
    coverage_src = (
        spark.table("silver.quotation_item")
        .select(col("coverage_type"))
        .filter(col("coverage_type").isNotNull() & (col("coverage_type") != "UNKNOWN"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=coverage_src,
        target_table_name="gold.dim_coverage",
        surrogate_key_col="coverage_key",
        business_key_col="coverage_type"
    )

    # --- 3. dim_quotation ---
    # Retrieve the latest version of each quotation
    quotation_w = Window.partitionBy("quotation_id").orderBy(col("updated_at").desc(), col("created_at").desc())
    quotation_src = (
        spark.table("silver.quotation")
        .filter(col("quotation_id").isNotNull() & (col("quotation_id") != "UNKNOWN"))
        .withColumn("row_num", row_number().over(quotation_w))
        .filter(col("row_num") == 1)
        .select(
            col("quotation_id"),
            to_date(col("quotation_expiry_at")).alias("quotation_expiry_date")
        )
    )
    load_scd1_dimension(
        source_df=quotation_src,
        target_table_name="gold.dim_quotation",
        surrogate_key_col="quotation_key",
        business_key_col="quotation_id",
        attribute_cols=["quotation_expiry_date"]
    )

    # --- 4. dim_policy ---
    policy_df = spark.table("silver.policy")
    if "last_updated_at" not in policy_df.columns:
        raise ValueError("silver.policy must include last_updated_at for dim_policy ordering.")
    # silver.policy DDL uses last_updated_at, not updated_at; use load/issue time only as tie-breakers.
    policy_w = Window.partitionBy("policy_id").orderBy(
        coalesce(col("last_updated_at"), col("_loaded_at"), col("issued_at")).desc_nulls_last()
    )
    policy_src = (
        policy_df
        .filter(col("policy_id").isNotNull() & (col("policy_id") != "UNKNOWN"))
        .withColumn("row_num", row_number().over(policy_w))
        .filter(col("row_num") == 1)
        .select(col("policy_id"))
    )
    load_scd1_dimension(
        source_df=policy_src,
        target_table_name="gold.dim_policy",
        surrogate_key_col="policy_key",
        business_key_col="policy_id"
    )

    # --- 5. dim_quotation_status ---
    q_status_src = (
        spark.table("silver.quotation")
        .select(col("quotation_status").alias("quotation_status_code"))
        .filter(col("quotation_status_code").isNotNull() & (col("quotation_status_code") != "UNKNOWN"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=q_status_src,
        target_table_name="gold.dim_quotation_status",
        surrogate_key_col="quotation_status_key",
        business_key_col="quotation_status_code"
    )

    # --- 6. dim_policy_status ---
    p_status_src = (
        spark.table("silver.policy")
        .select(col("policy_status").alias("policy_status_code"))
        .filter(col("policy_status_code").isNotNull() & (col("policy_status_code") != "UNKNOWN"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=p_status_src,
        target_table_name="gold.dim_policy_status",
        surrogate_key_col="policy_status_key",
        business_key_col="policy_status_code"
    )

    # --- 7. dim_payment_status ---
    pay_status_src = (
        spark.table("silver.payment")
        .select(col("payment_status").alias("payment_status_code"))
        .filter(col("payment_status_code").isNotNull() & (col("payment_status_code") != "UNKNOWN"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=pay_status_src,
        target_table_name="gold.dim_payment_status",
        surrogate_key_col="payment_status_key",
        business_key_col="payment_status_code"
    )

    # --- 8. dim_payment_method ---
    pay_method_src = (
        spark.table("silver.payment")
        .select(col("payment_method").alias("raw_method"))
        .filter(col("raw_method").isNotNull() & (col("raw_method") != "UNKNOWN"))
        .distinct()
        .withColumn("payment_method_code", 
            expr("""
                CASE 
                    WHEN upper(raw_method) = 'BANK TRANSFER' THEN 'BANK_TRANSFER'
                    WHEN upper(raw_method) = 'CREDIT CARD' THEN 'CREDIT_CARD'
                    WHEN upper(raw_method) = 'E-WALLET' THEN 'E_WALLET'
                    ELSE upper(replace(raw_method, ' ', '_'))
                END
            """)
        )
        .select(col("payment_method_code"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=pay_method_src,
        target_table_name="gold.dim_payment_method",
        surrogate_key_col="payment_method_key",
        business_key_col="payment_method_code"
    )

    # --- 9. dim_cancellation_reason ---
    cancel_reason_src = (
        spark.table("silver.cancellation")
        .select(col("cancellation_reason"))
        .filter(col("cancellation_reason").isNotNull() & (col("cancellation_reason") != "UNKNOWN"))
        .distinct()
    )
    load_scd1_dimension(
        source_df=cancel_reason_src,
        target_table_name="gold.dim_cancellation_reason",
        surrogate_key_col="cancellation_reason_key",
        business_key_col="cancellation_reason"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 4: EXECUTION RUNNER
# -------------------------------------------------------------------------
run_all_scd1_dimensions()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
