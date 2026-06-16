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
from pyspark.sql import functions as F

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()

# Helper to retrieve table_session_id for a given target table
def get_table_session_id(dim_fact_table_id: int) -> str:
    # Map conformed ID to source ID dynamically under Option 2
    try:
        df_tables = spark.table("cfg.dim_fact_table").select("id", "table_name").collect()
        mappings = spark.table("cfg.source_dim_fact").select("dim_fact_table_id", "source_table_id").collect()
        src_tables = spark.table("cfg.source_table").select("id", "source_name").collect()
        
        df_names = {row["id"]: row["table_name"] for row in df_tables}
        src_names = {row["id"]: row["source_name"] for row in src_tables}
        
        from collections import defaultdict
        df_to_srcs = defaultdict(list)
        for row in mappings:
            df_id = int(row["dim_fact_table_id"])
            src_id = int(row["source_table_id"])
            df_to_srcs[df_id].append(src_id)
            
        dim_fact_to_source = {}
        for df_id, src_ids in df_to_srcs.items():
            df_name = df_names.get(df_id, "").lower()
            matched_id = None
            for s_id in src_ids:
                s_name = src_names.get(s_id, "").lower()
                s_norm = s_name[:-1] if s_name.endswith('s') else s_name
                if s_norm in df_name:
                    if matched_id is None or len(s_norm) > len(src_names.get(matched_id, "")):
                        matched_id = s_id
            dim_fact_to_source[df_id] = matched_id if matched_id is not None else src_ids[0]

        source_id = dim_fact_to_source.get(dim_fact_table_id)
    except Exception:
        source_id = None

    if not source_id:
        return None
    try:
        rows = spark.table("log.audit_table_session") \
            .where((F.col("batch_id") == F.lit(batch_id)) & (F.col("source_table_id") == F.lit(source_id))) \
            .orderBy(F.col("created_at").desc()) \
            .select("id") \
            .limit(1) \
            .collect()
        return rows[0]["id"] if rows else None
    except Exception:
        return None

# Validation runner for a fact table
def validate_fact_table(
    table_name: str,
    dim_fact_table_id: int,
    grain_cols: list[str],
    fk_mappings: dict, # col_name -> dimension_table
    date_keys: list[str],
    silver_table_name: str,
    metric_cols: list[str] # col_name in gold vs expression/col_name in silver
):
    table_session_id = get_table_session_id(dim_fact_table_id)
    is_temp_session = False
    if not table_session_id:
        print(f"[INFO] No table session found for {table_name} in batch {batch_id}. Using a temporary session ID.")
        table_session_id = new_audit_id()
        is_temp_session = True

    print(f"[VALIDATE] Starting QA check suite for {table_name}...")
    gold_df = spark.table(table_name)

    # 1. Grain Uniqueness Check
    # We only check records inserted/updated in the current batch
    batch_gold_df = gold_df.where(F.col("_batch_id") == F.lit(str(batch_id)))
    grain_dups = batch_gold_df.groupBy(*grain_cols).count().filter("count > 1")
    dup_count = grain_dups.count()
    if dup_count > 0:
        err_msg = f"Grain Uniqueness Check Failed: Found {dup_count} duplicate rows at grain {grain_cols}."
        print(f"[ERROR] {err_msg}")
        # Log to invalid_record
        sample_dup = grain_dups.limit(1).collect()[0]
        log_invalid_record(
            table_session_id=table_session_id,
            layer="GOLD",
            target_table=table_name,
            record_key=str(sample_dup[grain_cols[0]]),
            raw_data=str(sample_dup.asDict()),
            error_reason=err_msg,
            error_column=grain_cols[0],
            error_type=ErrorType.RULE
        )
        if not is_temp_session:
            finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="GRAIN_UNIQUENESS_FAILED", error_message=err_msg)
        raise Exception(err_msg)

    # 2. Foreign Key Integrity Check
    for fk_col, dim_table in fk_mappings.items():
        # Join to find records where fk is not -1 and does not resolve in dimension
        dim_pk = dim_table.split(".")[-1].replace("dim_", "") + "_key"
        # Handles special key names
        if dim_table == "gold.dim_cancellation_reason":
            dim_pk = "cancellation_reason_key"
        
        dim_df = spark.table(dim_table).select(dim_pk)
        orphaned = batch_gold_df.alias("g").join(
            dim_df.alias("d"),
            on=F.col("g." + fk_col) == F.col("d." + dim_pk),
            how="left"
        ).filter((F.col("g." + fk_col) != -1) & F.col("d." + dim_pk).isNull())

        orphaned_count = orphaned.count()
        if orphaned_count > 0:
            err_msg = f"Foreign Key Integrity Check Failed: Column {fk_col} has {orphaned_count} orphaned keys mapping to {dim_table}."
            print(f"[ERROR] {err_msg}")
            sample_orphaned = orphaned.limit(1).collect()[0]
            log_invalid_record(
                table_session_id=table_session_id,
                layer="GOLD",
                target_table=table_name,
                record_key=str(sample_orphaned[grain_cols[0]]),
                raw_data=str(sample_orphaned.asDict()),
                error_reason=err_msg,
                error_column=fk_col,
                error_type=ErrorType.RULE
            )
            if not is_temp_session:
                finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="FK_INTEGRITY_FAILED", error_message=err_msg)
            raise Exception(err_msg)

    # 3. Date Key Validity Check
    dim_date_keys = spark.table("gold.dim_date").select("date_key")
    for date_key_col in date_keys:
        orphaned_dates = batch_gold_df.alias("g").join(
            dim_date_keys.alias("d"),
            on=F.col("g." + date_key_col) == F.col("d.date_key"),
            how="left"
        ).filter((F.col("g." + date_key_col) != -1) & F.col("d.date_key").isNull())

        orphaned_date_count = orphaned_dates.count()
        if orphaned_date_count > 0:
            err_msg = f"Date Key Validity Check Failed: Column {date_key_col} has {orphaned_date_count} date keys not resolving in dim_date."
            print(f"[ERROR] {err_msg}")
            sample_orphaned_date = orphaned_dates.limit(1).collect()[0]
            log_invalid_record(
                table_session_id=table_session_id,
                layer="GOLD",
                target_table=table_name,
                record_key=str(sample_orphaned_date[grain_cols[0]]),
                raw_data=str(sample_orphaned_date.asDict()),
                error_reason=err_msg,
                error_column=date_key_col,
                error_type=ErrorType.RULE
            )
            if not is_temp_session:
                finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="DATE_KEY_VALIDITY_FAILED", error_message=err_msg)
            raise Exception(err_msg)

    # 4. Row Count Reconciliation Check
    # Count of active batch records in silver table (deduplicated)
    silver_df = spark.table(silver_table_name).where(F.col("_batch_id") == F.lit(str(batch_id)))
    # For count, we reconcile against deduplicated silver table on business keys
    silver_grain_cols = [c if c != "coverage_key" else "coverage_type" for c in grain_cols]
    silver_count = silver_df.dropDuplicates(silver_grain_cols).count()
    gold_count = batch_gold_df.count()
    
    if gold_count != silver_count:
        err_msg = f"Row Count Reconciliation Failed: Gold count ({gold_count}) does not match deduplicated Silver count ({silver_count}) for batch_id={batch_id}."
        print(f"[ERROR] {err_msg}")
        log_invalid_record(
            table_session_id=table_session_id,
            layer="GOLD",
            target_table=table_name,
            record_key="N/A",
            raw_data=f"{{'gold_count': {gold_count}, 'silver_count': {silver_count}}}",
            error_reason=err_msg,
            error_column="row_count",
            error_type=ErrorType.RULE
        )
        if not is_temp_session:
            finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="ROW_COUNT_RECONCILIATION_FAILED", error_message=err_msg)
        raise Exception(err_msg)

    # 5. Metric Reconciliation Check
    for metric_col in metric_cols:
        # Reconcile sum of metrics, zeroing out metrics for soft-deleted records in Silver if column exists
        if "is_deleted" in silver_df.columns:
            silver_metric_sum_col = F.sum(F.when(F.col("is_deleted") == True, F.lit(0.00)).otherwise(F.coalesce(F.col(metric_col), F.lit(0.00))))
        else:
            silver_metric_sum_col = F.sum(F.coalesce(F.col(metric_col), F.lit(0.00)))
            
        silver_metric_sum = silver_df.select(silver_metric_sum_col.alias("metric_sum")).collect()[0]["metric_sum"]
        silver_metric_sum = float(silver_metric_sum) if silver_metric_sum is not None else 0.00

        gold_metric_sum = batch_gold_df.select(F.sum(metric_col).alias("metric_sum")).collect()[0]["metric_sum"]
        gold_metric_sum = float(gold_metric_sum) if gold_metric_sum is not None else 0.00

        variance = abs(silver_metric_sum - gold_metric_sum)
        if variance > 0.01:
            err_msg = f"Metric Reconciliation Failed for {metric_col}: Silver sum = {silver_metric_sum:.2f}, Gold sum = {gold_metric_sum:.2f}, variance = {variance:.4f} exceeds threshold 0.01."
            print(f"[ERROR] {err_msg}")
            log_invalid_record(
                table_session_id=table_session_id,
                layer="GOLD",
                target_table=table_name,
                record_key="N/A",
                raw_data=f"{{'gold_sum': {gold_metric_sum}, 'silver_sum': {silver_metric_sum}, 'variance': {variance}}}",
                error_reason=err_msg,
                error_column=metric_col,
                error_type=ErrorType.RULE
            )
            if not is_temp_session:
                finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="METRIC_RECONCILIATION_FAILED", error_message=err_msg)
            raise Exception(err_msg)

    # 6. Soft Delete Auditing Check
    # Check that deleted records have valid metadata fields
    corrupt_deletes = batch_gold_df.filter((F.col("is_deleted") == True) & (F.col("deleted_at").isNull() | F.col("delete_batch_id").isNull()))
    corrupt_delete_count = corrupt_deletes.count()
    if corrupt_delete_count > 0:
        err_msg = f"Soft Delete Auditing Check Failed: Found {corrupt_delete_count} deleted rows with missing delete metadata."
        print(f"[ERROR] {err_msg}")
        sample_corrupt = corrupt_deletes.limit(1).collect()[0]
        log_invalid_record(
            table_session_id=table_session_id,
            layer="GOLD",
            target_table=table_name,
            record_key=str(sample_corrupt[grain_cols[0]]),
            raw_data=str(sample_corrupt.asDict()),
            error_reason=err_msg,
            error_column="deleted_at/delete_batch_id",
            error_type=ErrorType.RULE
        )
        if not is_temp_session:
            finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="SOFT_DELETE_AUDITING_FAILED", error_message=err_msg)
        raise Exception(err_msg)

    print(f"[SUCCESS] {table_name} passed all QA audits successfully.")

# ---------------------------------------------------------------------------
# Sequenced Execution of Audits
# ---------------------------------------------------------------------------

# 1. fact_quotation (ID: 15)
validate_fact_table(
    table_name="gold.fact_quotation",
    dim_fact_table_id=15,
    grain_cols=["quotation_id"],
    fk_mappings={
        "quotation_key": "gold.dim_quotation",
        "customer_key": "gold.dim_customer",
        "agent_key": "gold.dim_agent",
        "provider_key": "gold.dim_provider",
        "package_key": "gold.dim_package",
        "quotation_status_key": "gold.dim_quotation_status",
        "vehicle_key": "gold.dim_vehicle"
    },
    date_keys=["quotation_date_key", "quotation_expiry_date_key"],
    silver_table_name="silver.quotation",
    metric_cols=["premium_amount"]
)

# 2. fact_quotation_item (ID: 16)
validate_fact_table(
    table_name="gold.fact_quotation_item",
    dim_fact_table_id=16,
    grain_cols=["quotation_id", "coverage_key"], # logic check using coverage_key on gold side as business key
    fk_mappings={
        "quotation_key": "gold.dim_quotation",
        "customer_key": "gold.dim_customer",
        "agent_key": "gold.dim_agent",
        "provider_key": "gold.dim_provider",
        "package_key": "gold.dim_package",
        "quotation_status_key": "gold.dim_quotation_status",
        "coverage_key": "gold.dim_coverage",
        "vehicle_key": "gold.dim_vehicle"
    },
    date_keys=["quotation_date_key"],
    silver_table_name="silver.quotation_item",
    metric_cols=["coverage_amount", "deductible_amount"]
)

# 3. fact_policy (ID: 17)
validate_fact_table(
    table_name="gold.fact_policy",
    dim_fact_table_id=17,
    grain_cols=["policy_id"],
    fk_mappings={
        "policy_key": "gold.dim_policy",
        "quotation_key": "gold.dim_quotation",
        "customer_key": "gold.dim_customer",
        "provider_key": "gold.dim_provider",
        "agent_key": "gold.dim_agent",
        "package_key": "gold.dim_package",
        "policy_status_key": "gold.dim_policy_status",
        "vehicle_key": "gold.dim_vehicle"
    },
    date_keys=["issued_date_key", "policy_start_date_key", "policy_end_date_key"],
    silver_table_name="silver.policy",
    metric_cols=["premium_amount"]
)

# 4. fact_payment (ID: 18)
validate_fact_table(
    table_name="gold.fact_payment",
    dim_fact_table_id=18,
    grain_cols=["payment_id"],
    fk_mappings={
        "policy_key": "gold.dim_policy",
        "payment_status_key": "gold.dim_payment_status",
        "payment_method_key": "gold.dim_payment_method",
        "customer_key": "gold.dim_customer",
        "provider_key": "gold.dim_provider",
        "vehicle_key": "gold.dim_vehicle"
    },
    date_keys=["payment_date_key", "issued_date_key"],
    silver_table_name="silver.payment",
    metric_cols=["payment_amount"]
)

# 5. fact_cancellation (ID: 19)
validate_fact_table(
    table_name="gold.fact_cancellation",
    dim_fact_table_id=19,
    grain_cols=["cancellation_id"],
    fk_mappings={
        "policy_key": "gold.dim_policy",
        "cancellation_reason_key": "gold.dim_cancellation_reason",
        "customer_key": "gold.dim_customer",
        "provider_key": "gold.dim_provider",
        "vehicle_key": "gold.dim_vehicle"
    },
    date_keys=["cancellation_date_key"],
    silver_table_name="silver.cancellation",
    metric_cols=["refund_amount"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
