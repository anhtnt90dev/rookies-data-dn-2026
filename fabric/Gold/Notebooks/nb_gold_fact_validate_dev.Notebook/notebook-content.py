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
p_pipeline_run_id = _notebook_param("p_pipeline_run_id", None)
p_batch_id = _notebook_param("p_batch_id", None)
p_enable_audit = as_bool(_notebook_param("p_enable_audit", True), True)
p_fail_on_validation_error = as_bool(_notebook_param("p_fail_on_validation_error", True), True)

if is_blank(p_batch_id):
    p_batch_id = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Specs for validating fact tables
VALIDATION_SPECS = {
    "fact_policy": {
        "source_entity": "silver.policy",
        "target_entity": "gold.fact_policy",
        "key_column": "policy_id",
        "date_keys": ["issued_date_key", "policy_start_date_key", "policy_end_date_key"],
        "fk_validations": {
            "policy_key": ("gold.dim_policy", "policy_key"),
            "quotation_key": ("gold.dim_quotation", "quotation_key"),
            "customer_key": ("gold.dim_customer", "customer_key"),
            "provider_key": ("gold.dim_provider", "provider_key"),
            "agent_key": ("gold.dim_agent", "agent_key"),
            "package_key": ("gold.dim_package", "package_key"),
            "policy_status_key": ("gold.dim_policy_status", "policy_status_key"),
            "vehicle_key": ("gold.dim_vehicle", "vehicle_key"),
        },
        "amount_column": "premium_amount",
        "source_dedupe_order": ["last_updated_at", "_loaded_at", "issued_at"],
    },
    "fact_quotation": {
        "source_entity": "silver.quotation",
        "target_entity": "gold.fact_quotation",
        "key_column": "quotation_id",
        "date_keys": ["quotation_date_key", "quotation_expiry_date_key"],
        "fk_validations": {
            "quotation_key": ("gold.dim_quotation", "quotation_key"),
            "customer_key": ("gold.dim_customer", "customer_key"),
            "agent_key": ("gold.dim_agent", "agent_key"),
            "provider_key": ("gold.dim_provider", "provider_key"),
            "package_key": ("gold.dim_package", "package_key"),
            "quotation_status_key": ("gold.dim_quotation_status", "quotation_status_key"),
            "vehicle_key": ("gold.dim_vehicle", "vehicle_key")
        },
        "amount_column": "premium_amount",
        "source_dedupe_order": ["updated_at", "_loaded_at", "quotation_at"],
    },
    "fact_quotation_item": {
        "source_entity": "silver.quotation_item",
        "target_entity": "gold.fact_quotation_item",
        "key_column": "quotation_item_id",
        "date_keys": ["quotation_date_key"],
        "fk_validations": {
            "quotation_key": ("gold.dim_quotation", "quotation_key"),
            "customer_key": ("gold.dim_customer", "customer_key"),
            "agent_key": ("gold.dim_agent", "agent_key"),
            "provider_key": ("gold.dim_provider", "provider_key"),
            "package_key": ("gold.dim_package", "package_key"),
            "quotation_status_key": ("gold.dim_quotation_status", "quotation_status_key"),
            "coverage_key": ("gold.dim_coverage", "coverage_key"),
            "vehicle_key": ("gold.dim_vehicle", "vehicle_key")
        },
        "amount_column": "coverage_amount",
        "source_dedupe_order": ["_loaded_at"],
    },
    "fact_payment": {
        "source_entity": "silver.payment",
        "target_entity": "gold.fact_payment",
        "key_column": "payment_id",
        "date_keys": ["payment_date_key", "issued_date_key"],
        "fk_validations": {
            "policy_key": ("gold.dim_policy", "policy_key"),
            "payment_status_key": ("gold.dim_payment_status", "payment_status_key"),
            "payment_method_key": ("gold.dim_payment_method", "payment_method_key"),
            "customer_key": ("gold.dim_customer", "customer_key"),
            "provider_key": ("gold.dim_provider", "provider_key"),
            "vehicle_key": ("gold.dim_vehicle", "vehicle_key")
        },
        "amount_column": "payment_amount",
        "source_dedupe_order": ["_loaded_at"],
    },
    "fact_cancellation": {
        "source_entity": "silver.cancellation",
        "target_entity": "gold.fact_cancellation",
        "key_column": "cancellation_id",
        "date_keys": ["cancellation_date_key"],
        "fk_validations": {
            "policy_key": ("gold.dim_policy", "policy_key"),
            "cancellation_reason_key": ("gold.dim_cancellation_reason", "cancellation_reason_key"),
            "customer_key": ("gold.dim_customer", "customer_key"),
            "provider_key": ("gold.dim_provider", "provider_key"),
            "vehicle_key": ("gold.dim_vehicle", "vehicle_key")
        },
        "amount_column": "refund_amount",
        "source_dedupe_order": ["_loaded_at"],
    }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_validation_result(results: List[Dict], check_name: str, status: str, expected, actual, details: str = None) -> None:
    results.append({
        "check_name": check_name,
        "status": status,
        "expected": str(expected) if expected is not None else None,
        "actual": str(actual) if actual is not None else None,
        "details": details,
    })


def decimal_sum(df: DataFrame, column_name: str) -> float:
    result = df.agg(F.coalesce(F.sum(F.col(column_name)), F.lit(0)).alias("total")).collect()[0]["total"]
    return float(result or 0)


def source_data_for_validation(fact_name: str, batch_id):
    spec = VALIDATION_SPECS[fact_name]
    source_df = filter_by_batch(spark.table(spec["source_entity"]), batch_id)
    if source_df.limit(1).count() == 0:
        return source_df
    
    key_col = spec["key_column"]
    latest_df = dedupe_latest(source_df, [key_col], spec["source_dedupe_order"])
    return latest_df.where(F.col(key_col).isNotNull() & (F.trim(F.col(key_col)) != F.lit("")))


def target_fact_for_validation(fact_name: str, batch_id):
    spec = VALIDATION_SPECS[fact_name]
    return filter_by_batch(spark.table(spec["target_entity"]), batch_id)


def validation_result_dataframe(results: List[Dict]) -> DataFrame:
    return (
        spark.createDataFrame(results)
        .withColumn("checked_at", F.current_timestamp())
        .select("check_name", "status", "expected", "actual", "details", "checked_at")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_fact_table_generic(fact_name: str, batch_id=None, pipeline_run_id: str = None, enable_audit: bool = True) -> Dict:
    spec = get_fact_spec(fact_name)
    val_spec = VALIDATION_SPECS[fact_name]
    results = []

    run_preflight_for_fact(fact_name, enable_audit=enable_audit)
    add_validation_result(results, "preflight_dependencies", "PASS", "all dependencies ready", "all dependencies ready")

    source_df = source_data_for_validation(fact_name, batch_id)
    target_df = target_fact_for_validation(fact_name, batch_id)

    source_count = int(source_df.count())
    target_count = int(target_df.count())
    add_validation_result(
        results,
        "row_count_reconciliation",
        "PASS" if source_count == target_count else "FAIL",
        source_count,
        target_count,
        f"Deduped, valid {val_spec['source_entity']} rows compared with {val_spec['target_entity']} rows for the selected batch.",
    )

    key_col = val_spec["key_column"]
    null_key_count = int(target_df.where(F.col(key_col).isNull() | (F.trim(F.col(key_col)) == F.lit(""))).count())
    add_validation_result(
        results,
        f"{key_col}_required",
        "PASS" if null_key_count == 0 else "FAIL",
        0,
        null_key_count,
        f"Fact table must retain non-null {key_col} as the degenerate identifier and merge key.",
    )

    duplicate_count = int(
        target_df
        .groupBy(key_col)
        .count()
        .where((F.col(key_col).isNotNull()) & (F.col("count") > 1))
        .count()
    )
    add_validation_result(
        results,
        f"no_duplicate_{key_col}",
        "PASS" if duplicate_count == 0 else "FAIL",
        0,
        duplicate_count,
        f"Reruns must not create duplicate {key_col} rows.",
    )

    missing_date_counts = count_missing_date_key_values(target_df, val_spec["date_keys"], "gold.dim_date")
    for column_name, missing_count in missing_date_counts.items():
        add_validation_result(
            results,
            f"{column_name}_exists_in_dim_date",
            "PASS" if missing_count == 0 else "FAIL",
            0,
            missing_count,
            f"{column_name} must be non-null and exist in gold.dim_date.date_key.",
        )

    for fact_key_column, (dimension_table, dimension_key_column) in val_spec["fk_validations"].items():
        invalid_count = count_invalid_fk_values(target_df, fact_key_column, dimension_table, dimension_key_column)
        add_validation_result(
            results,
            f"{fact_key_column}_valid_or_unknown",
            "PASS" if invalid_count == 0 else "FAIL",
            0,
            invalid_count,
            f"{fact_key_column} must exist in {dimension_table}.{dimension_key_column} or equal {UNKNOWN_KEY}.",
        )

    unknown_counts = count_unknown_keys(target_df, val_spec["fk_validations"].keys())
    for key_column, unknown_count in unknown_counts.items():
        add_validation_result(
            results,
            f"{key_column}_unknown_count",
            "INFO",
            "reported",
            unknown_count,
            f"Rows assigned {UNKNOWN_KEY} because the dimension lookup was missing or invalid.",
        )

    # Reconcile values if amount column is configured
    amt_col = val_spec.get("amount_column")
    if amt_col and amt_col in source_df.columns and amt_col in target_df.columns:
        source_with_delete_flag_df = source_df.withColumn(
            "__source_is_deleted",
            (F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(True))
            | (F.upper(F.coalesce(F.col("operation_type"), F.lit(""))) == F.lit("D")),
        )
        source_amount = decimal_sum(
            source_with_delete_flag_df.where(~F.col("__source_is_deleted")),
            amt_col,
        )
        target_amount = decimal_sum(
            target_df.where(~F.coalesce(F.col("is_deleted"), F.lit(False))),
            amt_col,
        )
        amount_diff = round(abs(source_amount - target_amount), 2)
        add_validation_result(
            results,
            f"{amt_col}_reconciliation",
            "PASS" if amount_diff <= 0.01 else "FAIL",
            round(source_amount, 2),
            round(target_amount, 2),
            f"Non-deleted Silver {amt_col} total should reconcile to non-deleted Gold {amt_col} total.",
        )

    invalid_soft_delete_count = int(
        target_df
        .where(
            (F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(True))
            & (F.col("deleted_at").isNull() | F.col("delete_batch_id").isNull())
        )
        .count()
    )
    add_validation_result(
        results,
        "soft_delete_metadata",
        "PASS" if invalid_soft_delete_count == 0 else "FAIL",
        0,
        invalid_soft_delete_count,
        "Soft-deleted facts must keep the row and populate deleted_at and delete_batch_id.",
    )

    if as_bool(enable_audit, True) and not is_blank(pipeline_run_id):
        audit_session_df = spark.table(AUDIT_SESSION_TABLE).where(F.col("pipeline_run_id") == F.lit(str(pipeline_run_id)))
        audit_session_count = int(audit_session_df.count())
        add_validation_result(
            results,
            "audit_session_exists",
            "PASS" if audit_session_count >= 1 else "FAIL",
            ">=1",
            audit_session_count,
            f"log.audit_session must contain pipeline_run_id={pipeline_run_id}.",
        )

        table_session_df = (
            spark.table(AUDIT_TABLE_SESSION_TABLE)
            .join(audit_session_df.select(F.col("id").alias("__session_id")), F.col("session_id") == F.col("__session_id"), "inner")
            .where(F.col("source_table_id") == F.lit(get_cfg_fact_table_id(fact_name)))
        )
        audit_detail_count = int(
            spark.table(AUDIT_DETAIL_TABLE)
            .join(table_session_df.select(F.col("id").alias("__table_session_id")), F.col("table_session_id") == F.col("__table_session_id"), "inner")
            .where(F.col("layer") == F.lit(GOLD_LAYER.value))
            .count()
        )
        add_validation_result(
            results,
            "audit_detail_exists",
            "PASS" if audit_detail_count >= 1 else "FAIL",
            ">=1",
            audit_detail_count,
            f"log.audit_detail must contain a Gold detail row for {fact_name}.",
        )
    else:
        add_validation_result(
            results,
            "audit_rows",
            "INFO",
            "skipped",
            "skipped",
            "Audit row validation skipped because audit is disabled or p_pipeline_run_id was not supplied.",
        )

    validation_df = validation_result_dataframe(results)
    display(validation_df)

    failed_checks = [row for row in results if row["status"] == "FAIL"]
    if failed_checks:
        failed_names = ", ".join([row["check_name"] for row in failed_checks])
        if p_fail_on_validation_error:
            raise Exception(f"Gold fact validation failed for {fact_name}: {failed_names}")

    return {
        "fact_table": spec["target_table"],
        "batch_id": batch_id,
        "pipeline_run_id": pipeline_run_id,
        "failed_check_count": len(failed_checks),
        "status": "SUCCESS" if not failed_checks else "FAILED",
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_gold_fact_validation(
    fact_table: str = DEFAULT_FACT_TABLE,
    batch_id = None,
    pipeline_run_id: str = None,
    enable_audit: bool = True,
) -> Dict:
    fact_name = normalize_fact_name(fact_table)
    if fact_name not in VALIDATION_SPECS:
        raise NotImplementedError(f"Validation specs for {fact_name} are not implemented.")
    return validate_fact_table_generic(
        fact_name=fact_name,
        batch_id=batch_id,
        pipeline_run_id=pipeline_run_id,
        enable_audit=enable_audit,
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_result = run_gold_fact_validation(
    fact_table=p_fact_table,
    batch_id=p_batch_id,
    pipeline_run_id=p_pipeline_run_id,
    enable_audit=p_enable_audit,
)

print(validation_result)
