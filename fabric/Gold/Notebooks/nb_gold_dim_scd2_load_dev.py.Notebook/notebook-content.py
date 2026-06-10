# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 1: IMPORTS AND PARAMETERS
# -------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, coalesce, lit, row_number, expr, md5, concat_ws
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
# CELL 2: SCD TYPE 2 HYBRID LOAD FUNCTION
# -------------------------------------------------------------------------
def load_scd2_dimension(
    source_df,
    target_table_name,
    surrogate_key_col,
    business_key_col,
    tracked_cols,
    type1_cols,
    hash_col_name
) -> None:
    """
    Loads data into a Gold SCD Type 2 table using hybrid SCD Type 1/2 logic.
    - source_df: Source DataFrame containing business_key_col, tracked_cols, type1_cols, and effective_from.
    - target_table_name: Fully qualified target table name (e.g. 'gold.dim_customer').
    - surrogate_key_col: Target surrogate key column (e.g. 'customer_key').
    - business_key_col: Target business key column (e.g. 'customer_id').
    - tracked_cols: List of Type 2 attributes that trigger a history change.
    - type1_cols: List of Type 1 attributes updated in-place on active row.
    - hash_col_name: Temporary column name for change detection hash.
    """
    print(f"[INFO] Ingesting SCD Type 2 dimension: {target_table_name}")
    
    # 1. Get target Delta table
    if not spark.catalog.tableExists(target_table_name):
        raise ValueError(f"Target table '{target_table_name}' does not exist.")
        
    target_table = spark.table(target_table_name)
    
    # 2. Exclude -1 Unknown key for finding max surrogate key
    existing_records = target_table.filter(col(surrogate_key_col) != -1)

    # 3. Calculate hashes for change detection
    def build_hash_expr(cols):
        coalesced_cols = [coalesce(col(c).cast("string"), lit("")) for c in cols]
        return md5(concat_ws("||", *coalesced_cols))
        
    source_with_hash = source_df.withColumn(hash_col_name, build_hash_expr([business_key_col] + tracked_cols))
    
    # 4. Filter target active records and compute target hash dynamically
    active_target = existing_records.filter(col("is_current") == True)
    target_with_hash = active_target.withColumn(hash_col_name, build_hash_expr([business_key_col] + tracked_cols))

    # 5. Join source and active target records
    target_attribute_cols = list(dict.fromkeys(tracked_cols + type1_cols))
    joined_df = source_with_hash.join(
        target_with_hash.select(
            col(business_key_col).alias("t_" + business_key_col),
            col(surrogate_key_col).alias("t_" + surrogate_key_col),
            col(hash_col_name).alias("t_" + hash_col_name),
            col("effective_from").alias("t_effective_from"),
            col("effective_to").alias("t_effective_to"),
            col("is_current").alias("t_is_current"),
            *[col(c).alias("t_" + c) for c in target_attribute_cols]
        ),
        on=col(business_key_col) == col("t_" + business_key_col),
        how="left"
    )

    # 6. Separate rows by path
    # Path A: Rows to insert (Action = 'INSERT')
    insert_rows = joined_df.filter(
        col("t_" + surrogate_key_col).isNull() | 
        (col("t_" + surrogate_key_col).isNotNull() & (col(hash_col_name) != col("t_" + hash_col_name)))
    ).select(
        lit("INSERT").alias("action_type"),
        lit(None).cast("long").alias(surrogate_key_col),
        col(business_key_col),
        *[col(c) for c in tracked_cols],
        *[col(c) for c in type1_cols],
        col("effective_from"),
        lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_to"),
        lit(True).alias("is_current")
    )

    # Path B: Target active rows to expire (Action = 'EXPIRE')
    expire_rows = joined_df.filter(
        col("t_" + surrogate_key_col).isNotNull() & (col(hash_col_name) != col("t_" + hash_col_name))
    ).select(
        lit("EXPIRE").alias("action_type"),
        col("t_" + surrogate_key_col).alias(surrogate_key_col),
        col(business_key_col),
        *[col("t_" + c) for c in tracked_cols],
        *[col("t_" + c) for c in type1_cols],
        col("t_effective_from").alias("effective_from"),
        col("effective_from").alias("effective_to"), # expired at new version's effective_from
        lit(False).alias("is_current")
    )

    # Path C: Type 1 updates on active versions (Action = 'UPDATE_TYPE1')
    type1_changed_cond = lit(False)
    for c in type1_cols:
        type1_changed_cond = type1_changed_cond | (
            coalesce(col(c).cast("string"), lit("")) != coalesce(col("t_" + c).cast("string"), lit(""))
        )

    type1_rows = joined_df.filter(
        col("t_" + surrogate_key_col).isNotNull() & 
        (col(hash_col_name) == col("t_" + hash_col_name)) & 
        type1_changed_cond
    ).select(
        lit("UPDATE_TYPE1").alias("action_type"),
        col("t_" + surrogate_key_col).alias(surrogate_key_col),
        col(business_key_col),
        *[col(c) for c in tracked_cols],
        *[col(c) for c in type1_cols],
        col("t_effective_from").alias("effective_from"),
        col("t_effective_to").alias("effective_to"),
        col("t_is_current").alias("is_current")
    )

    # 7. Generate new surrogate keys for INSERT rows
    max_key_row = existing_records.agg({surrogate_key_col: "max"}).collect()[0][0]
    max_key = int(max_key_row) if max_key_row is not None else 0
    if max_key < 0:
        max_key = 0

    if insert_rows.count() > 0:
        window_spec = Window.orderBy(business_key_col)
        insert_rows_with_keys = insert_rows.withColumn(
            surrogate_key_col,
            row_number().over(window_spec).cast("long") + max_key
        )
    else:
        insert_rows_with_keys = insert_rows

    # 8. Combine all source changes
    merge_source = insert_rows_with_keys.unionByName(expire_rows).unionByName(type1_rows)

    # 9. Execute Delta Merge
    delta_target = DeltaTable.forName(spark, target_table_name)

    expire_update_expr = {
        "is_current": "source.is_current",
        "effective_to": "source.effective_to",
        "updated_at": "current_timestamp()"
    }

    type1_update_expr = {}
    for c in type1_cols:
        type1_update_expr[c] = f"source.{c}"
    type1_update_expr["updated_at"] = "current_timestamp()"

    insert_expr = {
        surrogate_key_col: f"source.{surrogate_key_col}",
        business_key_col: f"source.{business_key_col}",
        "effective_from": "source.effective_from",
        "effective_to": "source.effective_to",
        "is_current": "source.is_current",
        "created_at": "current_timestamp()",
        "updated_at": "current_timestamp()"
    }
    for c in tracked_cols:
        insert_expr[c] = f"source.{c}"
    for c in type1_cols:
        insert_expr[c] = f"source.{c}"

    merge_builder = (
        delta_target.alias("target")
        .merge(
            merge_source.alias("source"),
            f"target.{surrogate_key_col} = source.{surrogate_key_col}"
        )
        .whenMatchedUpdate(
            condition="source.action_type = 'EXPIRE'",
            set=expire_update_expr
        )
    )

    if type1_cols:
        merge_builder = merge_builder.whenMatchedUpdate(
            condition="source.action_type = 'UPDATE_TYPE1'",
            set=type1_update_expr
        )

    merge_builder = merge_builder.whenNotMatchedInsert(
        condition="source.action_type = 'INSERT'",
        values=insert_expr
    )

    merge_builder.execute()

    duplicate_current_count = (
        spark.table(target_table_name)
        .where(col("is_current") == True)
        .groupBy(business_key_col)
        .count()
        .where(col(business_key_col).isNotNull() & (col("count") > 1))
        .limit(1)
        .count()
    )
    if duplicate_current_count > 0:
        raise ValueError(f"SCD2 current-row validation failed for {target_table_name}: duplicate current business keys found.")

    print(f"[SUCCESS] Ingested {target_table_name} successfully.\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 3: RUN SCD TYPE 2 LOADS
# -------------------------------------------------------------------------
def run_all_scd2_dimensions() -> None:
    # --- 1. dim_customer ---
    customer_w = Window.partitionBy("customer_id").orderBy(col("updated_at").desc(), col("created_at").desc())
    customer_src = (
        spark.table("silver.customer")
        .filter(col("customer_id").isNotNull() & (col("customer_id") != "UNKNOWN"))
        .withColumn("row_num", row_number().over(customer_w))
        .filter(col("row_num") == 1)
        .withColumn("effective_from", coalesce(col("updated_at"), col("created_at"), F.current_timestamp()))
        .select(
            "customer_id",
            "full_name",
            "gender",
            "dob",
            "phone_number",
            "email",
            "city",
            "district",
            "effective_from"
        )
    )
    load_scd2_dimension(
        source_df=customer_src,
        target_table_name="gold.dim_customer",
        surrogate_key_col="customer_key",
        business_key_col="customer_id",
        tracked_cols=["city", "district"],
        type1_cols=["full_name", "gender", "dob", "phone_number", "email"],
        hash_col_name="customer_scd_hash"
    )

    # --- 2. dim_agent ---
    agent_w = Window.partitionBy("agent_id").orderBy(col("updated_at").desc(), col("created_at").desc())
    agent_src = (
        spark.table("silver.agent")
        .filter(col("agent_id").isNotNull() & (col("agent_id") != "UNKNOWN"))
        .withColumn("row_num", row_number().over(agent_w))
        .filter(col("row_num") == 1)
        .withColumn("effective_from", coalesce(col("updated_at"), col("created_at"), F.current_timestamp()))
        .select(
            "agent_id",
            "agent_name",
            "region",
            "branch",
            "manager_name",
            "effective_from"
        )
    )
    load_scd2_dimension(
        source_df=agent_src,
        target_table_name="gold.dim_agent",
        surrogate_key_col="agent_key",
        business_key_col="agent_id",
        tracked_cols=["region", "branch", "manager_name"],
        type1_cols=["agent_name"],
        hash_col_name="agent_scd_hash"
    )

    # --- 3. dim_provider ---
    provider_w = Window.partitionBy("provider_code").orderBy(col("updated_at").desc(), col("created_at").desc())
    provider_src = (
        spark.table("silver.provider")
        .filter(col("provider_code").isNotNull() & (col("provider_code") != "UNKNOWN"))
        .withColumn("row_num", row_number().over(provider_w))
        .filter(col("row_num") == 1)
        .withColumn("effective_from", coalesce(col("updated_at"), col("created_at"), F.current_timestamp()))
        .withColumn("active_flag", expr("CASE WHEN is_active THEN 1 ELSE 0 END"))
        .select(
            "provider_code",
            "provider_name",
            "provider_group",
            "active_flag",
            "effective_from"
        )
    )
    load_scd2_dimension(
        source_df=provider_src,
        target_table_name="gold.dim_provider",
        surrogate_key_col="provider_key",
        business_key_col="provider_code",
        tracked_cols=["provider_group", "active_flag"],
        type1_cols=["provider_name"],
        hash_col_name="provider_scd_hash"
    )

    # --- 4. dim_vehicle ---
    vehicle_w = Window.partitionBy("vehicle_id").orderBy(col("updated_at").desc(), col("created_at").desc())
    vehicle_src = (
        spark.table("silver.vehicle")
        .filter(col("vehicle_id").isNotNull() & (col("vehicle_id") != "UNKNOWN"))
        .withColumn("row_num", row_number().over(vehicle_w))
        .filter(col("row_num") == 1)
        .withColumn("effective_from", coalesce(col("updated_at"), col("created_at"), F.current_timestamp()))
        .select(
            "vehicle_id",
            "customer_id",
            "plate_number",
            "vehicle_brand",
            "vehicle_model",
            "manufacture_year",
            "vehicle_value",
            "effective_from"
        )
    )
    load_scd2_dimension(
        source_df=vehicle_src,
        target_table_name="gold.dim_vehicle",
        surrogate_key_col="vehicle_key",
        business_key_col="vehicle_id",
        tracked_cols=["vehicle_value"],
        type1_cols=["customer_id", "plate_number", "vehicle_brand", "vehicle_model", "manufacture_year"],
        hash_col_name="vehicle_scd_hash"
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
run_all_scd2_dimensions()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
