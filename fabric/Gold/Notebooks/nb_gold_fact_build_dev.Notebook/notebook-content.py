# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b883e6d2-ee4b-4338-a694-4b81d338dd49",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "ddc0f61e-f221-421b-a87b-f80ffce2c8df",
# META       "known_lakehouses": [
# META         {
# META           "id": "b883e6d2-ee4b-4338-a694-4b81d338dd49"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run nb_gold_fact_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _notebook_param(name: str, default=None):
    return globals()[name] if name in globals() else default


p_fact_table = _notebook_param("p_fact_table", DEFAULT_FACT_TABLE)
p_pipeline_name = _notebook_param("p_pipeline_name", DEFAULT_PIPELINE_NAME)
p_pipeline_run_id = _notebook_param("p_pipeline_run_id", None)
p_batch_id = _notebook_param("p_batch_id", None)
p_run_mode = _notebook_param("p_run_mode", "NEW")
p_enable_audit = as_bool(_notebook_param("p_enable_audit", True), True)

# Internal parameter used by the Gold driver so one audit session can cover
# preflight, build, and validation.
p_audit_session_id = _notebook_param("p_audit_session_id", None)

if is_blank(p_pipeline_run_id):
    p_pipeline_run_id = make_manual_pipeline_run_id(p_pipeline_name)

if is_blank(p_batch_id):
    p_batch_id = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FACT_POLICY_TARGET_COLUMNS = [
    "policy_id",
    "policy_number",
    "quotation_id",
    "customer_id",
    "provider_code",
    "policy_key",
    "quotation_key",
    "customer_key",
    "provider_key",
    "agent_key",
    "package_key",
    "policy_status_key",
    "issued_date_key",
    "policy_start_date_key",
    "policy_end_date_key",
    "vehicle_key",
    "premium_amount",
    "created_at",
    "updated_at",
    "_batch_id",
    "_source_system",
    "pipeline_run_id",
    "is_deleted",
    "deleted_at",
    "delete_batch_id",
]

FACT_POLICY_LOOKUP_KEY_COLUMNS = [
    "policy_key",
    "quotation_key",
    "customer_key",
    "provider_key",
    "agent_key",
    "package_key",
    "policy_status_key",
    "vehicle_key",
]

FACT_POLICY_DATE_KEY_COLUMNS = [
    "issued_date_key",
    "policy_start_date_key",
    "policy_end_date_key",
]

FACT_POLICY_RAW_COLUMNS = [
    "policy_id",
    "policy_number",
    "quotation_id",
    "customer_id",
    "provider_code",
    "policy_status",
    "issued_at",
    "policy_start_date",
    "policy_end_date",
    "premium_amount",
    "operation_type",
    "is_deleted",
    "_batch_id",
    "_source_system",
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_fact_policy_dataframe(batch_id, pipeline_run_id: str, table_session_id: str = None) -> Dict:
    spec = get_fact_spec("fact_policy")
    policy_df = filter_by_batch(spark.table(spec["source_table"]), batch_id)
    source_row_count = int(policy_df.count())

    if source_row_count == 0:
        return {
            "fact_df": empty_target_dataframe(spec["target_table"]),
            "source_row_count": 0,
            "rejected_row_count": 0,
            "unresolved_lookup_count": 0,
        }

    policy_latest_df = dedupe_latest(
        policy_df,
        key_columns=["policy_id"],
        order_columns=["last_updated_at", "_loaded_at", "issued_at"],
    )

    invalid_key_df = policy_latest_df.where(F.col("policy_id").isNull() | (F.trim(F.col("policy_id")) == F.lit("")))
    rejected_row_count = log_invalid_rows(
        df=invalid_key_df,
        table_session_id=table_session_id,
        target_table=spec["target_table"],
        record_key_column="policy_id",
        error_column="policy_id",
        error_reason="Missing required business key policy_id; row excluded from Gold fact_policy merge",
        raw_columns=FACT_POLICY_RAW_COLUMNS,
    )

    valid_policy_df = policy_latest_df.where(F.col("policy_id").isNotNull() & (F.trim(F.col("policy_id")) != F.lit("")))
    if valid_policy_df.limit(1).count() == 0:
        return {
            "fact_df": empty_target_dataframe(spec["target_table"]),
            "source_row_count": source_row_count,
            "rejected_row_count": int(rejected_row_count),
            "unresolved_lookup_count": 0,
        }

    quotation_latest_df = dedupe_latest(
        spark.table("silver.quotation"),
        key_columns=["quotation_id"],
        order_columns=["updated_at", "_loaded_at", "quotation_at"],
    )
    quotation_context_df = quotation_latest_df.select(
        F.col("quotation_id").alias("__quotation_id"),
        F.col("agent_id").alias("quotation_agent_id"),
        F.col("package_code").alias("quotation_package_code"),
        F.col("quotation_at").alias("quotation_context_at"),
    )

    base_df = (
        valid_policy_df
        .join(quotation_context_df, valid_policy_df["quotation_id"] == quotation_context_df["__quotation_id"], "left")
        .drop("__quotation_id")
        .withColumn(
            "__policy_event_at",
            F.coalesce(
                F.col("issued_at").cast("timestamp"),
                F.to_timestamp(F.col("policy_start_date")),
                F.col("last_updated_at").cast("timestamp"),
                F.col("_loaded_at").cast("timestamp"),
            ),
        )
        .withColumn(
            "__quotation_event_at",
            F.coalesce(
                F.col("quotation_context_at").cast("timestamp"),
                F.col("issued_at").cast("timestamp"),
                F.col("_loaded_at").cast("timestamp"),
            ),
        )
        .withColumn("issued_date_key", date_key_expr("issued_at"))
        .withColumn("policy_start_date_key", date_key_expr("policy_start_date"))
        .withColumn("policy_end_date_key", date_key_expr("policy_end_date"))
    )

    assert_fact_date_keys_exist(base_df, FACT_POLICY_DATE_KEY_COLUMNS, "gold.dim_date")

    resolved_df = lookup_type1_key(
        base_df,
        dim_table="gold.dim_policy",
        business_column="policy_id",
        dim_business_column="policy_id",
        key_column="policy_key",
        output_column="policy_key",
    )
    resolved_df = lookup_type1_key(
        resolved_df,
        dim_table="gold.dim_quotation",
        business_column="quotation_id",
        dim_business_column="quotation_id",
        key_column="quotation_key",
        output_column="quotation_key",
    )
    resolved_df = lookup_scd2_key(
        resolved_df,
        dim_table="gold.dim_customer",
        business_column="customer_id",
        dim_business_column="customer_id",
        event_timestamp_column="__policy_event_at",
        key_column="customer_key",
        output_column="customer_key",
    )
    resolved_df = lookup_scd2_key(
        resolved_df,
        dim_table="gold.dim_provider",
        business_column="provider_code",
        dim_business_column="provider_code",
        event_timestamp_column="__policy_event_at",
        key_column="provider_key",
        output_column="provider_key",
    )
    resolved_df = lookup_scd2_key(
        resolved_df,
        dim_table="gold.dim_agent",
        business_column="quotation_agent_id",
        dim_business_column="agent_id",
        event_timestamp_column="__quotation_event_at",
        key_column="agent_key",
        output_column="agent_key",
    )
    resolved_df = lookup_type1_key(
        resolved_df,
        dim_table="gold.dim_package",
        business_column="quotation_package_code",
        dim_business_column="package_code",
        key_column="package_key",
        output_column="package_key",
    )
    resolved_df = lookup_type1_key(
        resolved_df,
        dim_table="gold.dim_policy_status",
        business_column="policy_status",
        dim_business_column="policy_status_code",
        key_column="policy_status_key",
        output_column="policy_status_key",
    )
    resolved_df = lookup_scd2_key(
        resolved_df,
        dim_table="gold.dim_vehicle",
        business_column="customer_id",
        dim_business_column="customer_id",
        event_timestamp_column="__policy_event_at",
        key_column="vehicle_key",
        output_column="vehicle_key",
    )

    source_deleted_expr = (
        (F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(True))
        | (F.upper(F.coalesce(F.col("operation_type"), F.lit(""))) == F.lit("D"))
    )

    fact_df = (
        resolved_df
        .withColumn("__source_is_deleted", source_deleted_expr)
        .withColumn(
            "__target_updated_at",
            F.coalesce(
                F.col("last_updated_at").cast("timestamp"),
                F.col("_loaded_at").cast("timestamp"),
                F.current_timestamp(),
            ),
        )
        .withColumn(
            "__delete_batch_id",
            F.when(
                F.col("__source_is_deleted"),
                F.coalesce(F.col("_batch_id"), F.lit(str(batch_id)) if not is_blank(batch_id) else F.lit(None).cast("string")),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "__deleted_at",
            F.when(F.col("__source_is_deleted"), F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")),
        )
        .select(
            F.col("policy_id").cast("string").alias("policy_id"),
            F.col("policy_number").cast("string").alias("policy_number"),
            F.col("quotation_id").cast("string").alias("quotation_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("provider_code").cast("string").alias("provider_code"),
            F.col("policy_key").cast("bigint").alias("policy_key"),
            F.col("quotation_key").cast("bigint").alias("quotation_key"),
            F.col("customer_key").cast("bigint").alias("customer_key"),
            F.col("provider_key").cast("bigint").alias("provider_key"),
            F.col("agent_key").cast("bigint").alias("agent_key"),
            F.col("package_key").cast("bigint").alias("package_key"),
            F.col("policy_status_key").cast("bigint").alias("policy_status_key"),
            F.col("issued_date_key").cast("int").alias("issued_date_key"),
            F.col("policy_start_date_key").cast("int").alias("policy_start_date_key"),
            F.col("policy_end_date_key").cast("int").alias("policy_end_date_key"),
            F.col("vehicle_key").cast("bigint").alias("vehicle_key"),
            F.coalesce(F.col("premium_amount"), F.lit(0)).cast("decimal(18,2)").alias("premium_amount"),
            F.current_timestamp().alias("created_at"),
            F.col("__target_updated_at").alias("updated_at"),
            F.col("_batch_id").cast("string").alias("_batch_id"),
            F.col("_source_system").cast("string").alias("_source_system"),
            F.lit(str(pipeline_run_id)).alias("pipeline_run_id"),
            F.col("__source_is_deleted").cast("boolean").alias("is_deleted"),
            F.col("__deleted_at").alias("deleted_at"),
            F.col("__delete_batch_id").cast("string").alias("delete_batch_id"),
        )
    )

    unresolved_lookup_count = log_unresolved_lookup_rows(
        df=fact_df,
        table_session_id=table_session_id,
        target_table=spec["target_table"],
        record_key_column="policy_id",
        lookup_key_columns=FACT_POLICY_LOOKUP_KEY_COLUMNS,
        raw_columns=FACT_POLICY_RAW_COLUMNS,
    )

    return {
        "fact_df": fact_df.select(*FACT_POLICY_TARGET_COLUMNS),
        "source_row_count": source_row_count,
        "rejected_row_count": int(rejected_row_count),
        "unresolved_lookup_count": int(unresolved_lookup_count),
    }


def build_fact_quotation_dataframe(batch_id, pipeline_run_id: str, table_session_id: str = None) -> Dict:
    spec = get_fact_spec("fact_quotation")
    src_df = filter_by_batch(spark.table(spec["source_table"]), batch_id)
    source_row_count = int(src_df.count())
    
    if source_row_count == 0:
        return {
            "fact_df": empty_target_dataframe(spec["target_table"]),
            "source_row_count": 0,
            "rejected_row_count": 0,
            "unresolved_lookup_count": 0
        }
        
    latest_df = dedupe_latest(src_df, key_columns=["quotation_id"], order_columns=["updated_at", "_loaded_at", "quotation_at"])
    
    # Exclude missing business key rows
    valid_df = latest_df.where(F.col("quotation_id").isNotNull() & (F.trim(F.col("quotation_id")) != F.lit("")))
    rejected_row_count = source_row_count - valid_df.count()
    
    # Establish Converted Flag by checking policies
    policies_df = spark.table("silver.policy").select("quotation_id").distinct().withColumn("__has_policy", F.lit(True))
    
    base_df = (
        valid_df
        .join(policies_df, on="quotation_id", how="left_outer")
        .withColumn("converted_flag", F.coalesce(F.col("__has_policy"), F.lit(False)))
        .withColumn("__event_at", F.coalesce(F.col("quotation_at").cast("timestamp"), F.col("_loaded_at").cast("timestamp")))
        .withColumn("quotation_date_key", date_key_expr("quotation_at"))
        .withColumn("quotation_expiry_date_key", date_key_expr("quotation_expiry_at"))
    )
    
    assert_fact_date_keys_exist(base_df, ["quotation_date_key", "quotation_expiry_date_key"], "gold.dim_date")
    
    # Resolve Keys
    res_df = lookup_type1_key(base_df, "gold.dim_quotation", "quotation_id", "quotation_id", "quotation_key", "quotation_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_customer", "customer_id", "customer_id", "__event_at", "customer_key", "customer_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_agent", "agent_id", "agent_id", "__event_at", "agent_key", "agent_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_provider", "provider_code", "provider_code", "__event_at", "provider_key", "provider_key")
    res_df = lookup_type1_key(res_df, "gold.dim_package", "package_code", "package_code", "package_key", "package_key")
    res_df = lookup_type1_key(res_df, "gold.dim_quotation_status", "quotation_status", "quotation_status_code", "quotation_status_key", "quotation_status_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_vehicle", "customer_id", "customer_id", "__event_at", "vehicle_key", "vehicle_key")
    
    source_deleted_expr = F.lit(False)
    
    fact_df = (
        res_df
        .withColumn("is_deleted", source_deleted_expr)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.coalesce(F.col("updated_at").cast("timestamp"), F.current_timestamp()))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("deleted_at", F.when(F.col("is_deleted"), F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("delete_batch_id", F.when(F.col("is_deleted"), F.lit(str(batch_id))).otherwise(F.lit(None).cast("string")))
        .withColumn("premium_amount", F.coalesce(F.col("premium_amount"), F.lit(0)).cast("decimal(18,2)"))
    )
    
    unresolved_lookup_count = log_unresolved_lookup_rows(
        df=fact_df, table_session_id=table_session_id, target_table=spec["target_table"],
        record_key_column="quotation_id", lookup_key_columns=list(spec["required_dimensions"].keys()), raw_columns=spec["source_required_columns"]
    )
    
    return {
        "fact_df": fact_df.select(*spec["target_required_columns"]),
        "source_row_count": source_row_count,
        "rejected_row_count": int(rejected_row_count),
        "unresolved_lookup_count": int(unresolved_lookup_count)
    }


def build_fact_quotation_item_dataframe(batch_id, pipeline_run_id: str, table_session_id: str = None) -> Dict:
    spec = get_fact_spec("fact_quotation_item")
    src_df = filter_by_batch(spark.table(spec["source_table"]), batch_id)
    source_row_count = int(src_df.count())
    
    if source_row_count == 0:
        return {
            "fact_df": empty_target_dataframe(spec["target_table"]),
            "source_row_count": 0,
            "rejected_row_count": 0,
            "unresolved_lookup_count": 0
        }
        
    latest_df = dedupe_latest(src_df, key_columns=["quotation_item_id"], order_columns=["_loaded_at"])
    valid_df = latest_df.where(F.col("quotation_item_id").isNotNull() & (F.trim(F.col("quotation_item_id")) != F.lit("")))
    rejected_row_count = source_row_count - valid_df.count()
    
    # Retrieve quotation header details
    quotation_latest = dedupe_latest(spark.table("silver.quotation"), ["quotation_id"], ["updated_at", "_loaded_at"])
    header_df = quotation_latest.select(
        F.col("quotation_id").alias("__h_quotation_id"),
        F.col("quotation_at").alias("quotation_at"),
        F.col("customer_id").alias("customer_id"),
        F.col("agent_id").alias("agent_id"),
        F.col("provider_code").alias("provider_code"),
        F.col("package_code").alias("package_code"),
        F.col("quotation_status").alias("quotation_status")
    )
    
    base_df = (
        valid_df
        .join(header_df, valid_df["quotation_id"] == header_df["__h_quotation_id"], "left")
        .drop("__h_quotation_id")
        .withColumn("__event_at", F.coalesce(F.col("quotation_at").cast("timestamp"), F.col("_loaded_at").cast("timestamp")))
        .withColumn("quotation_date_key", date_key_expr("quotation_at"))
    )
    
    assert_fact_date_keys_exist(base_df, ["quotation_date_key"], "gold.dim_date")
    
    # Resolve Keys
    res_df = lookup_type1_key(base_df, "gold.dim_quotation", "quotation_id", "quotation_id", "quotation_key", "quotation_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_customer", "customer_id", "customer_id", "__event_at", "customer_key", "customer_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_agent", "agent_id", "agent_id", "__event_at", "agent_key", "agent_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_provider", "provider_code", "provider_code", "__event_at", "provider_key", "provider_key")
    res_df = lookup_type1_key(res_df, "gold.dim_package", "package_code", "package_code", "package_key", "package_key")
    res_df = lookup_type1_key(res_df, "gold.dim_quotation_status", "quotation_status", "quotation_status_code", "quotation_status_key", "quotation_status_key")
    res_df = lookup_type1_key(res_df, "gold.dim_coverage", "coverage_type", "coverage_type", "coverage_key", "coverage_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_vehicle", "customer_id", "customer_id", "__event_at", "vehicle_key", "vehicle_key")
    
    source_deleted_expr = F.lit(False)
    
    fact_df = (
        res_df
        .withColumn("is_deleted", source_deleted_expr)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("deleted_at", F.when(F.col("is_deleted"), F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("delete_batch_id", F.when(F.col("is_deleted"), F.lit(str(batch_id))).otherwise(F.lit(None).cast("string")))
        .withColumn("coverage_amount", F.coalesce(F.col("coverage_amount"), F.lit(0)).cast("decimal(18,2)"))
        .withColumn("deductible_amount", F.coalesce(F.col("deductible_amount"), F.lit(0)).cast("decimal(18,2)"))
    )
    
    unresolved_lookup_count = log_unresolved_lookup_rows(
        df=fact_df, table_session_id=table_session_id, target_table=spec["target_table"],
        record_key_column="quotation_item_id", lookup_key_columns=list(spec["required_dimensions"].keys()), raw_columns=spec["source_required_columns"]
    )
    
    return {
        "fact_df": fact_df.select(*spec["target_required_columns"]),
        "source_row_count": source_row_count,
        "rejected_row_count": int(rejected_row_count),
        "unresolved_lookup_count": int(unresolved_lookup_count)
    }


def build_fact_payment_dataframe(batch_id, pipeline_run_id: str, table_session_id: str = None) -> Dict:
    spec = get_fact_spec("fact_payment")
    src_df = filter_by_batch(spark.table(spec["source_table"]), batch_id)
    source_row_count = int(src_df.count())
    
    if source_row_count == 0:
        return {
            "fact_df": empty_target_dataframe(spec["target_table"]),
            "source_row_count": 0,
            "rejected_row_count": 0,
            "unresolved_lookup_count": 0
        }
        
    latest_df = dedupe_latest(src_df, key_columns=["payment_id"], order_columns=["_loaded_at"])
    valid_df = latest_df.where(F.col("payment_id").isNotNull() & (F.trim(F.col("payment_id")) != F.lit("")))
    rejected_row_count = source_row_count - valid_df.count()
    
    # Retrieve conformed policy contexts
    policy_latest = dedupe_latest(spark.table("silver.policy"), ["policy_id"], ["last_updated_at", "_loaded_at"])
    policy_context = policy_latest.select(
        F.col("policy_id").alias("__p_policy_id"),
        F.col("customer_id").alias("customer_id"),
        F.col("provider_code").alias("provider_code"),
        F.col("issued_at").alias("policy_issued_at")
    )
    
    # Standardize payment method strings
    standardized_df = (
        valid_df
        .withColumn("payment_method_code", 
            F.expr("""
                CASE 
                    WHEN upper(payment_method) = 'BANK TRANSFER' THEN 'BANK_TRANSFER'
                    WHEN upper(payment_method) = 'CREDIT CARD' THEN 'CREDIT_CARD'
                    WHEN upper(payment_method) = 'E-WALLET' THEN 'E_WALLET'
                    ELSE upper(replace(payment_method, ' ', '_'))
                END
            """)
        )
    )
    
    base_df = (
        standardized_df
        .join(policy_context, standardized_df["policy_id"] == policy_context["__p_policy_id"], "left")
        .drop("__p_policy_id")
        .withColumn("__event_at", F.coalesce(F.col("payment_at").cast("timestamp"), F.col("_loaded_at").cast("timestamp")))
        .withColumn("payment_date_key", date_key_expr("payment_at"))
        .withColumn("issued_date_key", date_key_expr("policy_issued_at"))
    )
    
    assert_fact_date_keys_exist(base_df, ["payment_date_key", "issued_date_key"], "gold.dim_date")
    
    # Resolve Keys
    res_df = lookup_type1_key(base_df, "gold.dim_policy", "policy_id", "policy_id", "policy_key", "policy_key")
    res_df = lookup_type1_key(res_df, "gold.dim_payment_status", "payment_status", "payment_status_code", "payment_status_key", "payment_status_key")
    res_df = lookup_type1_key(res_df, "gold.dim_payment_method", "payment_method_code", "payment_method_code", "payment_method_key", "payment_method_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_customer", "customer_id", "customer_id", "__event_at", "customer_key", "customer_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_provider", "provider_code", "provider_code", "__event_at", "provider_key", "provider_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_vehicle", "customer_id", "customer_id", "__event_at", "vehicle_key", "vehicle_key")
    
    source_deleted_expr = (F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(True)) | (F.upper(F.coalesce(F.col("operation_type"), F.lit(""))) == F.lit("D"))
    
    fact_df = (
        res_df
        .withColumn("is_deleted", source_deleted_expr)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("deleted_at", F.when(F.col("is_deleted"), F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("delete_batch_id", F.when(F.col("is_deleted"), F.lit(str(batch_id))).otherwise(F.lit(None).cast("string")))
        .withColumn("payment_amount", F.coalesce(F.col("payment_amount"), F.lit(0)).cast("decimal(18,2)"))
    )
    
    unresolved_lookup_count = log_unresolved_lookup_rows(
        df=fact_df, table_session_id=table_session_id, target_table=spec["target_table"],
        record_key_column="payment_id", lookup_key_columns=list(spec["required_dimensions"].keys()), raw_columns=spec["source_required_columns"]
    )
    
    return {
        "fact_df": fact_df.select(*spec["target_required_columns"]),
        "source_row_count": source_row_count,
        "rejected_row_count": int(rejected_row_count),
        "unresolved_lookup_count": int(unresolved_lookup_count)
    }


def build_fact_cancellation_dataframe(batch_id, pipeline_run_id: str, table_session_id: str = None) -> Dict:
    spec = get_fact_spec("fact_cancellation")
    src_df = filter_by_batch(spark.table(spec["source_table"]), batch_id)
    source_row_count = int(src_df.count())
    
    if source_row_count == 0:
        return {
            "fact_df": empty_target_dataframe(spec["target_table"]),
            "source_row_count": 0,
            "rejected_row_count": 0,
            "unresolved_lookup_count": 0
        }
        
    latest_df = dedupe_latest(src_df, key_columns=["cancellation_id"], order_columns=["_loaded_at"])
    valid_df = latest_df.where(F.col("cancellation_id").isNotNull() & (F.trim(F.col("cancellation_id")) != F.lit("")))
    rejected_row_count = source_row_count - valid_df.count()
    
    # Retrieve conformed policy contexts
    policy_latest = dedupe_latest(spark.table("silver.policy"), ["policy_id"], ["last_updated_at", "_loaded_at"])
    policy_context = policy_latest.select(
        F.col("policy_id").alias("__p_policy_id"),
        F.col("customer_id").alias("customer_id"),
        F.col("provider_code").alias("provider_code")
    )
    
    base_df = (
        valid_df
        .join(policy_context, valid_df["policy_id"] == policy_context["__p_policy_id"], "left")
        .drop("__p_policy_id")
        .withColumn("__event_at", F.coalesce(F.col("cancellation_at").cast("timestamp"), F.col("_loaded_at").cast("timestamp")))
        .withColumn("cancellation_date_key", date_key_expr("cancellation_at"))
    )
    
    assert_fact_date_keys_exist(base_df, ["cancellation_date_key"], "gold.dim_date")
    
    # Resolve Keys
    res_df = lookup_type1_key(base_df, "gold.dim_policy", "policy_id", "policy_id", "policy_key", "policy_key")
    res_df = lookup_type1_key(res_df, "gold.dim_cancellation_reason", "cancellation_reason", "cancellation_reason", "cancellation_reason_key", "cancellation_reason_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_customer", "customer_id", "customer_id", "__event_at", "customer_key", "customer_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_provider", "provider_code", "provider_code", "__event_at", "provider_key", "provider_key")
    res_df = lookup_scd2_key(res_df, "gold.dim_vehicle", "customer_id", "customer_id", "__event_at", "vehicle_key", "vehicle_key")
    
    source_deleted_expr = (F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(True)) | (F.upper(F.coalesce(F.col("operation_type"), F.lit(""))) == F.lit("D"))
    
    fact_df = (
        res_df
        .withColumn("is_deleted", source_deleted_expr)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("deleted_at", F.when(F.col("is_deleted"), F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("delete_batch_id", F.when(F.col("is_deleted"), F.lit(str(batch_id))).otherwise(F.lit(None).cast("string")))
        .withColumn("refund_amount", F.coalesce(F.col("refund_amount"), F.lit(0)).cast("decimal(18,2)"))
    )
    
    unresolved_lookup_count = log_unresolved_lookup_rows(
        df=fact_df, table_session_id=table_session_id, target_table=spec["target_table"],
        record_key_column="cancellation_id", lookup_key_columns=list(spec["required_dimensions"].keys()), raw_columns=spec["source_required_columns"]
    )
    
    return {
        "fact_df": fact_df.select(*spec["target_required_columns"]),
        "source_row_count": source_row_count,
        "rejected_row_count": int(rejected_row_count),
        "unresolved_lookup_count": int(unresolved_lookup_count)
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_gold_fact_build(
    fact_table: str = DEFAULT_FACT_TABLE,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
    pipeline_run_id: str = None,
    batch_id = None,
    run_mode: str = "NEW",
    enable_audit: bool = True,
    audit_session_id: str = None,
) -> Dict:
    fact_name = normalize_fact_name(fact_table)
    
    supported_builds = {
        "fact_policy": build_fact_policy_dataframe,
        "fact_quotation": build_fact_quotation_dataframe,
        "fact_quotation_item": build_fact_quotation_item_dataframe,
        "fact_payment": build_fact_payment_dataframe,
        "fact_cancellation": build_fact_cancellation_dataframe,
    }
    
    if fact_name not in supported_builds:
        raise NotImplementedError(f"Build for fact table {fact_name} is not implemented.")

    spec = get_fact_spec(fact_name)
    pipeline_run_id = pipeline_run_id or make_manual_pipeline_run_id(pipeline_name)
    enable_audit = as_bool(enable_audit, True)

    run_preflight_for_fact(fact_name, enable_audit=enable_audit)

    started_pipeline_session = False
    session_id = audit_session_id
    table_session_id = None

    try:
        if enable_audit and is_blank(session_id):
            session_id = start_gold_pipeline_audit(
                pipeline_name=pipeline_name,
                pipeline_run_id=pipeline_run_id,
                batch_id=batch_id,
                run_mode=run_mode,
                enable_audit=enable_audit,
            )
            started_pipeline_session = True

        table_session_id = start_gold_table_audit(
            session_id=session_id,
            fact_table=fact_name,
            batch_id=batch_id,
            load_type="INCREMENTAL" if not is_blank(batch_id) else "FULL",
            enable_audit=enable_audit,
        )

        build_result = supported_builds[fact_name](
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            table_session_id=table_session_id,
        )
        merge_result = merge_fact_table(build_result["fact_df"], spec["target_table"], spec["upsert_key"])

        audit_source_count = build_result["source_row_count"]
        finish_gold_table_audit(
            table_session_id=table_session_id,
            status=AuditStatus.SUCCESS,
            source_row_count=audit_source_count,
            target_row_count=merge_result["target_row_count"],
            inserted_row=merge_result["inserted_row"],
            updated_row=merge_result["updated_row"],
            deleted_row=merge_result["deleted_row"],
            rejected_row=build_result["rejected_row_count"],
            enable_audit=enable_audit,
        )

        if enable_audit and started_pipeline_session:
            finish_pipeline_session(session_id, AuditStatus.SUCCESS)

        return {
            "fact_table": spec["target_table"],
            "pipeline_run_id": pipeline_run_id,
            "batch_id": batch_id,
            "source_row_count": audit_source_count,
            "target_row_count": merge_result["target_row_count"],
            "inserted_row": merge_result["inserted_row"],
            "updated_row": merge_result["updated_row"],
            "deleted_row": merge_result["deleted_row"],
            "rejected_row": build_result["rejected_row_count"],
            "unresolved_lookup_count": build_result["unresolved_lookup_count"],
            "status": AuditStatus.SUCCESS.value,
        }
    except Exception as exc:
        finish_gold_table_audit(
            table_session_id=table_session_id,
            status=AuditStatus.FAILED,
            error_code="GOLD_FACT_BUILD_FAILED",
            error_message=str(exc),
            error_type=ErrorType.UNKNOWN,
            is_retryable=False,
            enable_audit=enable_audit,
        )
        if enable_audit and started_pipeline_session:
            finish_pipeline_session(session_id, AuditStatus.FAILED)
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

build_result = run_gold_fact_build(
    fact_table=p_fact_table,
    pipeline_name=p_pipeline_name,
    pipeline_run_id=p_pipeline_run_id,
    batch_id=p_batch_id,
    run_mode=p_run_mode,
    enable_audit=p_enable_audit,
    audit_session_id=p_audit_session_id,
)

print(build_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
