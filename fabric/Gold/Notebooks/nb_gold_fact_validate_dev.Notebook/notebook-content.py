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

FACT_POLICY_FK_VALIDATIONS = {
    "policy_key": ("gold.dim_policy", "policy_key"),
    "quotation_key": ("gold.dim_quotation", "quotation_key"),
    "customer_key": ("gold.dim_customer", "customer_key"),
    "provider_key": ("gold.dim_provider", "provider_key"),
    "agent_key": ("gold.dim_agent", "agent_key"),
    "package_key": ("gold.dim_package", "package_key"),
    "policy_status_key": ("gold.dim_policy_status", "policy_status_key"),
    "vehicle_key": ("gold.dim_vehicle", "vehicle_key"),
}

FACT_POLICY_DATE_KEY_COLUMNS = [
    "issued_date_key",
    "policy_start_date_key",
    "policy_end_date_key",
]

FACT_POLICY_LOOKUP_KEY_COLUMNS = list(FACT_POLICY_FK_VALIDATIONS.keys())


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


def source_policy_for_validation(batch_id):
    source_df = filter_by_batch(spark.table("silver.policy"), batch_id)
    if source_df.limit(1).count() == 0:
        return source_df
    latest_df = dedupe_latest(source_df, ["policy_id"], ["last_updated_at", "_loaded_at", "issued_at"])
    return latest_df.where(F.col("policy_id").isNotNull() & (F.trim(F.col("policy_id")) != F.lit("")))


def target_fact_policy_for_validation(batch_id):
    return filter_by_batch(spark.table("gold.fact_policy"), batch_id)


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

def validate_fact_policy(batch_id=None, pipeline_run_id: str = None, enable_audit: bool = True) -> Dict:
    spec = get_fact_spec("fact_policy")
    results = []

    run_preflight_for_fact("fact_policy", enable_audit=enable_audit)
    add_validation_result(results, "preflight_dependencies", "PASS", "all dependencies ready", "all dependencies ready")

    source_df = source_policy_for_validation(batch_id)
    target_df = target_fact_policy_for_validation(batch_id)

    source_count = int(source_df.count())
    target_count = int(target_df.count())
    add_validation_result(
        results,
        "row_count_reconciliation",
        "PASS" if source_count == target_count else "FAIL",
        source_count,
        target_count,
        "Deduped, valid silver.policy rows compared with gold.fact_policy rows for the selected batch.",
    )

    null_policy_id_count = int(target_df.where(F.col("policy_id").isNull() | (F.trim(F.col("policy_id")) == F.lit(""))).count())
    add_validation_result(
        results,
        "policy_id_required",
        "PASS" if null_policy_id_count == 0 else "FAIL",
        0,
        null_policy_id_count,
        "fact_policy must retain non-null policy_id as the degenerate identifier and merge key.",
    )

    duplicate_count = int(
        target_df
        .groupBy("policy_id")
        .count()
        .where((F.col("policy_id").isNotNull()) & (F.col("count") > 1))
        .count()
    )
    add_validation_result(
        results,
        "no_duplicate_policy_id",
        "PASS" if duplicate_count == 0 else "FAIL",
        0,
        duplicate_count,
        "Reruns must not create duplicate policy_id rows.",
    )

    missing_date_counts = count_missing_date_key_values(target_df, FACT_POLICY_DATE_KEY_COLUMNS, "gold.dim_date")
    for column_name, missing_count in missing_date_counts.items():
        add_validation_result(
            results,
            f"{column_name}_exists_in_dim_date",
            "PASS" if missing_count == 0 else "FAIL",
            0,
            missing_count,
            f"{column_name} must be non-null and exist in gold.dim_date.date_key.",
        )

    for fact_key_column, (dimension_table, dimension_key_column) in FACT_POLICY_FK_VALIDATIONS.items():
        invalid_count = count_invalid_fk_values(target_df, fact_key_column, dimension_table, dimension_key_column)
        add_validation_result(
            results,
            f"{fact_key_column}_valid_or_unknown",
            "PASS" if invalid_count == 0 else "FAIL",
            0,
            invalid_count,
            f"{fact_key_column} must exist in {dimension_table}.{dimension_key_column} or equal {UNKNOWN_KEY}.",
        )

    unknown_counts = count_unknown_keys(target_df, FACT_POLICY_LOOKUP_KEY_COLUMNS)
    for key_column, unknown_count in unknown_counts.items():
        add_validation_result(
            results,
            f"{key_column}_unknown_count",
            "INFO",
            "reported",
            unknown_count,
            f"Rows assigned {UNKNOWN_KEY} because the dimension lookup was missing or invalid.",
        )

    source_with_delete_flag_df = source_df.withColumn(
        "__source_is_deleted",
        (F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(True))
        | (F.upper(F.coalesce(F.col("operation_type"), F.lit(""))) == F.lit("D")),
    )
    source_premium_amount = decimal_sum(
        source_with_delete_flag_df.where(~F.col("__source_is_deleted")),
        "premium_amount",
    )
    target_premium_amount = decimal_sum(
        target_df.where(~F.coalesce(F.col("is_deleted"), F.lit(False))),
        "premium_amount",
    )
    premium_diff = round(abs(source_premium_amount - target_premium_amount), 2)
    add_validation_result(
        results,
        "premium_amount_reconciliation",
        "PASS" if premium_diff <= 0.01 else "FAIL",
        round(source_premium_amount, 2),
        round(target_premium_amount, 2),
        "Non-deleted Silver premium_amount total should reconcile to non-deleted Gold premium_amount total.",
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
            .where(F.col("source_table_id") == F.lit(spec["cfg_dim_fact_table_id"]))
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
            "log.audit_detail must contain a Gold detail row for fact_policy.",
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
            raise Exception(f"Gold fact validation failed for fact_policy: {failed_names}")

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
    if fact_name != "fact_policy":
        raise NotImplementedError("This notebook currently validates the proven pattern for fact_policy only.")
    return validate_fact_policy(
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

