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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()

# Validation thresholds
THRESHOLDS = {
    "unresolved_key": {
        "critical": 0.0,
        "important": 0.01,
        "optional": 0.05
    },
    "duplicate_grain": {
        "max_count": 0
    },
    "row_count_mismatch": {
        "max_ratio": 0.0
    },
    "metric_mismatch": {
        "premium_amount": 0.0001,
        "payment_amount": 0.0001,
        "refund_amount": 0.0001,
        "coverage_amount": 0.0001,
        "deductible_amount": 0.0001
    },
    "invalid_date_key": {
        "critical": 0.0,
        "optional": 0.05
    }
}

# Table-specific severity classifications and business keys
TABLE_METADATA = {
    "gold.fact_quotation": {
        "business_keys": ["quotation_id"],
        "fk_severity": {
            "quotation_key": "critical",
            "customer_key": "important",
            "provider_key": "important",
            "agent_key": "important",
            "package_key": "important",
            "quotation_status_key": "important",
            "vehicle_key": "optional"
        },
        "date_severity": {
            "quotation_date_key": "critical",
            "quotation_expiry_date_key": "optional"
        }
    },
    "gold.fact_quotation_item": {
        "business_keys": ["quotation_item_id", "quotation_id"],
        "fk_severity": {
            "quotation_key": "critical",
            "customer_key": "important",
            "agent_key": "important",
            "provider_key": "important",
            "package_key": "important",
            "quotation_status_key": "important",
            "coverage_key": "critical",
            "vehicle_key": "optional"
        },
        "date_severity": {
            "quotation_date_key": "critical"
        }
    },
    "gold.fact_policy": {
        "business_keys": ["policy_id", "policy_number"],
        "fk_severity": {
            "policy_key": "critical",
            "quotation_key": "critical",
            "customer_key": "important",
            "provider_key": "important",
            "agent_key": "important",
            "package_key": "important",
            "policy_status_key": "important",
            "vehicle_key": "optional"
        },
        "date_severity": {
            "issued_date_key": "critical",
            "policy_start_date_key": "critical",
            "policy_end_date_key": "optional"
        }
    },
    "gold.fact_payment": {
        "business_keys": ["payment_id"],
        "fk_severity": {
            "policy_key": "critical",
            "customer_key": "important",
            "provider_key": "important",
            "payment_status_key": "important",
            "payment_method_key": "important",
            "vehicle_key": "optional"
        },
        "date_severity": {
            "payment_date_key": "critical",
            "issued_date_key": "optional"
        }
    },
    "gold.fact_cancellation": {
        "business_keys": ["cancellation_id"],
        "fk_severity": {
            "policy_key": "critical",
            "customer_key": "important",
            "provider_key": "important",
            "cancellation_reason_key": "important",
            "vehicle_key": "optional"
        },
        "date_severity": {
            "cancellation_date_key": "critical"
        }
    }
}

# Map conformed ID to source ID dynamically (precomputed globally to avoid redundant Spark jobs)
GLOBAL_DIM_FACT_TO_SOURCE = {}
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
        
    for df_id, src_ids in df_to_srcs.items():
        df_name = df_names.get(df_id, "").lower()
        matched_id = None
        for s_id in src_ids:
            s_name = src_names.get(s_id, "").lower()
            s_norm = s_name[:-1] if s_name.endswith('s') else s_name
            if s_norm in df_name:
                if matched_id is None or len(s_norm) > len(src_names.get(matched_id, "")):
                    matched_id = s_id
        GLOBAL_DIM_FACT_TO_SOURCE[df_id] = matched_id if matched_id is not None else src_ids[0]
except Exception as e:
    print(f"[WARNING] Failed to precompute table mappings: {e}")

# Helper to retrieve table_session_id for a given target table
def get_table_session_id(dim_fact_table_id: int) -> str:
    source_id = GLOBAL_DIM_FACT_TO_SOURCE.get(dim_fact_table_id)

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

from pyspark.sql.window import Window

def latest_by_key(df, key_col: str, order_col: str = "updated_at"):
    # Check for actual ordering column dynamically
    actual_order_col = None
    for col in [order_col, "last_updated_at", "last_updated", "created_at"]:
        if col in df.columns:
            actual_order_col = col
            break
            
    if actual_order_col:
        w = Window.partitionBy(key_col).orderBy(F.col(actual_order_col).desc_nulls_last())
        return df.withColumn("_rn", F.row_number().over(w)) \
            .where(F.col("_rn") == 1) \
            .drop("_rn")
    return df.dropDuplicates([key_col])

# Validation runner for a fact table
# Validation runner for a fact table
def validate_fact_table(
    table_name: str,
    dim_fact_table_id: int,
    grain_cols: list[str],
    fk_mappings: dict, # col_name -> dimension_table
    date_keys: list[str],
    silver_table_name: str,
    metric_cols: list[str], # col_name in gold vs expression/col_name in silver
    fk_bk_mappings: dict = {}, # col_name -> business_key_col (e.g. "customer_key" -> "customer_id")
    scd2_temporal_mappings: dict = {}, # col_name -> (dim_table, tx_date_col)
    silver_grain_cols: list[str] = None
):
    table_session_id = get_table_session_id(dim_fact_table_id)
    is_temp_session = False
    if not table_session_id:
        print(f"\n[INFO] No table session found for {table_name} in batch {batch_id}. Using a temporary session ID.")
        table_session_id = new_audit_id()
        is_temp_session = True

    print("\n" + "=" * 80)
    print(f"[VALIDATE] Starting QA check suite for {table_name}...")
    print("=" * 80 + "\n")
    gold_df = spark.table(table_name)

    # We only check records inserted/updated in the current batch
    batch_gold_df = gold_df.where(F.col("_batch_id") == F.lit(str(batch_id)))

    # Enrich batch_gold_df with parent business keys if missing in Gold schema, sourcing from Silver lineage (uses cached Silver DataFrames)
    if table_name == "gold.fact_quotation":
        batch_gold_df = batch_gold_df.join(silver_vehicle_latest, on="customer_id", how="left")
    elif table_name == "gold.fact_quotation_item":
        parent_df = silver_quotation_latest.join(silver_vehicle_latest, on="customer_id", how="left")
        batch_gold_df = batch_gold_df.join(parent_df, on="quotation_id", how="left")
    elif table_name == "gold.fact_policy":
        q_df = silver_quotation_latest.select("quotation_id", "agent_id")
        batch_gold_df = batch_gold_df.join(q_df, on="quotation_id", how="left")
        batch_gold_df = batch_gold_df.join(silver_vehicle_latest, on="customer_id", how="left")
    elif table_name in ["gold.fact_payment", "gold.fact_cancellation"]:
        parent_df = silver_policy_latest.join(silver_vehicle_latest, on="customer_id", how="left")
        batch_gold_df = batch_gold_df.join(parent_df, on="policy_id", how="left")

    batch_gold_df = batch_gold_df.cache()

    # Track if any validation check fails the pipeline
    fail_pipeline = False

    try:
        # Retrieve table metadata
        meta = TABLE_METADATA.get(table_name, {})
        business_keys = meta.get("business_keys", [])

        # 1. Grain Uniqueness Check
        print("  --> 1. Grain Uniqueness Check")
        grain_dups = batch_gold_df.groupBy(*grain_cols).count().filter("count > 1")
        dup_stats = grain_dups.select(F.sum(F.col("count") - 1).alias("dup_rows")).collect()
        dup_count = dup_stats[0]["dup_rows"] if dup_stats and dup_stats[0]["dup_rows"] is not None else 0

        if dup_count > 0:
            err_msg = f"Grain Uniqueness Check Failed: Found {dup_count} duplicate rows at grain {grain_cols}."
            print(f"      [ERROR] {err_msg}")
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
            fail_pipeline = True
        else:
            print(f"      [SUCCESS] Grain Uniqueness Check passed at grain {grain_cols}.")

        # 2. Build joined dataset for the remaining validations (single-action execution)
        joined_df = batch_gold_df.alias("g")

        # Left join with dimensions for foreign keys validation
        for fk_col, dim_table in fk_mappings.items():
            dim_pk = dim_table.split(".")[-1].replace("dim_", "") + "_key"
            if dim_table == "gold.dim_cancellation_reason":
                dim_pk = "cancellation_reason_key"
            joined_df = joined_df.join(
                spark.table(dim_table).alias(f"d_{fk_col}"),
                on=F.col(f"g.{fk_col}") == F.col(f"d_{fk_col}.{dim_pk}"),
                how="left"
            )

        # Left join with date dimension for date keys validation
        dim_date_df = spark.table("gold.dim_date")
        for date_key_col in date_keys:
            joined_df = joined_df.join(
                dim_date_df.alias(f"dt_{date_key_col}"),
                on=F.col(f"g.{date_key_col}") == F.col(f"dt_{date_key_col}.date_key"),
                how="left"
            )

        # 3. Construct aggregation expressions
        agg_exprs = [F.count(F.lit(1)).alias("total_rows")]

        # Null business keys check
        for bk in business_keys:
            if bk in batch_gold_df.columns:
                agg_exprs.append(F.sum(F.when(F.col(f"g.{bk}").isNull() | (F.col(f"g.{bk}") == ""), 1).otherwise(0)).alias(f"null_bk_{bk}"))

        # Foreign key integrity (orphaned conformed keys)
        for fk_col, dim_table in fk_mappings.items():
            dim_pk = dim_table.split(".")[-1].replace("dim_", "") + "_key"
            if dim_table == "gold.dim_cancellation_reason":
                dim_pk = "cancellation_reason_key"
            agg_exprs.append(F.sum(F.when((F.col(f"g.{fk_col}") != -1) & F.col(f"d_{fk_col}.{dim_pk}").isNull(), 1).otherwise(0)).alias(f"orphaned_fk_{fk_col}"))

        # Unresolved keys (-1 despite valid BK present)
        for fk_col, bk_col in fk_bk_mappings.items():
            if bk_col in batch_gold_df.columns:
                agg_exprs.append(F.sum(F.when(
                    (F.col(f"g.{fk_col}") == -1) & 
                    F.col(f"g.{bk_col}").isNotNull() & 
                    (F.col(f"g.{bk_col}") != "Unknown") & 
                    (F.col(f"g.{bk_col}") != ""), 1
                ).otherwise(0)).alias(f"unresolved_fk_{fk_col}"))

        # Temporal integrity (SCD2 effective_from dates comparison)
        for fk_col, (dim_table, tx_date_col) in scd2_temporal_mappings.items():
            agg_exprs.append(F.sum(F.when(
                (F.col(f"g.{fk_col}") != -1) & 
                (F.col(f"g.{tx_date_col}") != -1) & 
                (F.col(f"dt_{tx_date_col}.full_date") < F.col(f"d_{fk_col}.effective_from").cast("date")), 1
            ).otherwise(0)).alias(f"temporal_viol_{fk_col}"))

        # Date key validity check
        for date_key_col in date_keys:
            agg_exprs.append(F.sum(F.when((F.col(f"g.{date_key_col}") != -1) & F.col(f"dt_{date_key_col}.date_key").isNull(), 1).otherwise(0)).alias(f"orphaned_date_{date_key_col}"))

        # Soft delete auditing metadata check
        if "is_deleted" in batch_gold_df.columns:
            agg_exprs.append(F.sum(F.when((F.col("g.is_deleted") == True) & (F.col("g.deleted_at").isNull() | F.col("g.delete_batch_id").isNull()), 1).otherwise(0)).alias("corrupt_deletes"))

        # Metrics values totals for gold
        for metric_col in metric_cols:
            if "is_deleted" in batch_gold_df.columns:
                gold_metric_sum_col = F.sum(F.when(F.col("g.is_deleted") == True, F.lit(0.00)).otherwise(F.coalesce(F.col(f"g.{metric_col}"), F.lit(0.00))))
            else:
                gold_metric_sum_col = F.sum(F.coalesce(F.col(f"g.{metric_col}"), F.lit(0.00)))
            agg_exprs.append(gold_metric_sum_col.alias(f"gold_metric_{metric_col}"))

        # Run the combined aggregation (1 Spark action)
        stats = joined_df.select(*agg_exprs).collect()[0].asDict()
        total_rows = stats.get("total_rows", 0)

        # 4. Process check results from stats
        # A. Null business keys check
        print("\n  --> 2. Null Business Keys Check")
        has_null_bk = False
        for bk in business_keys:
            null_count = stats.get(f"null_bk_{bk}") or 0
            if null_count > 0:
                err_msg = f"Missing Fact Business Key: Column {bk} has {null_count} null or empty values."
                print(f"      [ERROR] {err_msg}")
                sample = joined_df.filter(F.col(f"g.{bk}").isNull() | (F.col(f"g.{bk}") == "")).limit(1).collect()[0]
                log_invalid_record(
                    table_session_id=table_session_id,
                    layer="GOLD",
                    target_table=table_name,
                    record_key=str(sample[grain_cols[0]]),
                    raw_data=str(sample.asDict()),
                    error_reason=err_msg,
                    error_column=bk,
                    error_type=ErrorType.RULE
                )
                fail_pipeline = True
                has_null_bk = True
        if not has_null_bk:
            print(f"      [SUCCESS] Null Business Keys Check passed for keys {business_keys}.")

        # B. Foreign key integrity check (orphaned keys mapping to dimension pk)
        print("\n  --> 3. Foreign Key Integrity Check")
        has_orphaned_fk = False
        for fk_col, dim_table in fk_mappings.items():
            dim_pk = dim_table.split(".")[-1].replace("dim_", "") + "_key"
            if dim_table == "gold.dim_cancellation_reason":
                dim_pk = "cancellation_reason_key"
            orphaned_count = stats.get(f"orphaned_fk_{fk_col}") or 0
            if orphaned_count > 0:
                err_msg = f"Foreign Key Integrity Check Failed: Column {fk_col} has {orphaned_count} orphaned keys mapping to {dim_table}."
                print(f"      [ERROR] {err_msg}")
                sample = joined_df.filter((F.col(f"g.{fk_col}") != -1) & F.col(f"d_{fk_col}.{dim_pk}").isNull()).limit(1).collect()[0]
                log_invalid_record(
                    table_session_id=table_session_id,
                    layer="GOLD",
                    target_table=table_name,
                    record_key=str(sample[grain_cols[0]]),
                    raw_data=str(sample.asDict()),
                    error_reason=err_msg,
                    error_column=fk_col,
                    error_type=ErrorType.RULE
                )
                fail_pipeline = True
                has_orphaned_fk = True
        if not has_orphaned_fk:
            print(f"      [SUCCESS] Foreign Key Integrity Check passed for all conformed keys.")

        # C. Unresolved keys check (-1 when valid BK is present)
        print("\n  --> 4. Unresolved Keys Check")
        has_unresolved_fk = False
        for fk_col, bk_col in fk_bk_mappings.items():
            unresolved_count = stats.get(f"unresolved_fk_{fk_col}") or 0
            if unresolved_count > 0:
                sev = meta.get("fk_severity", {}).get(fk_col, "important")
                max_ratio = THRESHOLDS["unresolved_key"].get(sev, 0.01)
                ratio = unresolved_count / total_rows if total_rows > 0 else 0.0

                err_msg = f"Column {fk_col} has {unresolved_count} rows ({ratio:.2%}) resolved to -1 (severity: {sev}, threshold: {max_ratio:.2%})."
                if ratio > max_ratio:
                    print(f"      [ERROR] Unresolved Key Check: {err_msg} exceeds threshold!")
                    fail_pipeline = True
                else:
                    print(f"      [INFO] Unresolved Key Check: {err_msg}")

                sample = joined_df.filter((F.col(f"g.{fk_col}") == -1) & F.col(f"g.{bk_col}").isNotNull() & (F.col(f"g.{bk_col}") != "Unknown") & (F.col(f"g.{bk_col}") != "")).limit(1).collect()[0]
                log_invalid_record(
                    table_session_id=table_session_id,
                    layer="GOLD",
                    target_table=table_name,
                    record_key=str(sample[grain_cols[0]]),
                    raw_data=str(sample.asDict()),
                    error_reason=err_msg,
                    error_column=fk_col,
                    error_type=ErrorType.RULE
                )
                has_unresolved_fk = True
        if not has_unresolved_fk:
            print(f"      [SUCCESS] Unresolved Keys Check passed (no rows resolved to -1).")

        # D. Temporal Integrity check (SCD2 effective date range)
        print("\n  --> 5. Temporal Integrity Check")
        has_temporal_viol = False
        for fk_col, (dim_table, tx_date_col) in scd2_temporal_mappings.items():
            viol_count = stats.get(f"temporal_viol_{fk_col}") or 0
            if viol_count > 0:
                sev = meta.get("fk_severity", {}).get(fk_col, "important")
                max_ratio = THRESHOLDS["unresolved_key"].get(sev, 0.01)
                ratio = viol_count / total_rows if total_rows > 0 else 0.0

                err_msg = f"Column {fk_col} has {viol_count} rows ({ratio:.2%}) prior to dimension effective_from date (severity: {sev}, threshold: {max_ratio:.2%})."
                if ratio > max_ratio:
                    print(f"      [ERROR] Temporal Integrity Check: {err_msg} exceeds threshold!")
                    fail_pipeline = True
                else:
                    print(f"      [INFO] Temporal Integrity Check: {err_msg}")

                sample = joined_df.filter((F.col(f"g.{fk_col}") != -1) & (F.col(f"g.{tx_date_col}") != -1) & (F.col(f"dt_{tx_date_col}.full_date") < F.col(f"d_{fk_col}.effective_from").cast("date"))).limit(1).collect()[0]
                log_invalid_record(
                    table_session_id=table_session_id,
                    layer="GOLD",
                    target_table=table_name,
                    record_key=str(sample[grain_cols[0]]),
                    raw_data=str(sample.asDict()),
                    error_reason=err_msg,
                    error_column=fk_col,
                    error_type=ErrorType.RULE
                )
                has_temporal_viol = True
        if not has_temporal_viol:
            print(f"      [SUCCESS] Temporal Integrity Check passed for SCD2 dimensions.")

        # E. Date Key Validity Check
        print("\n  --> 6. Date Key Validity Check")
        has_invalid_date = False
        for date_key_col in date_keys:
            orphaned_date_count = stats.get(f"orphaned_date_{date_key_col}") or 0
            if orphaned_date_count > 0:
                sev = meta.get("date_severity", {}).get(date_key_col, "important")
                max_ratio = THRESHOLDS["invalid_date_key"].get(sev, 0.0)
                ratio = orphaned_date_count / total_rows if total_rows > 0 else 0.0

                err_msg = f"Column {date_key_col} has {orphaned_date_count} date keys ({ratio:.2%}) not resolving in dim_date (severity: {sev}, threshold: {max_ratio:.2%})."
                if ratio > max_ratio:
                    print(f"      [ERROR] Date Key Validity Check: {err_msg} exceeds threshold!")
                    fail_pipeline = True
                else:
                    print(f"      [INFO] Date Key Validity Check: {err_msg}")

                sample = joined_df.filter((F.col(f"g.{date_key_col}") != -1) & F.col(f"dt_{date_key_col}.date_key").isNull()).limit(1).collect()[0]
                log_invalid_record(
                    table_session_id=table_session_id,
                    layer="GOLD",
                    target_table=table_name,
                    record_key=str(sample[grain_cols[0]]),
                    raw_data=str(sample.asDict()),
                    error_reason=err_msg,
                    error_column=date_key_col,
                    error_type=ErrorType.RULE
                )
                has_invalid_date = True
        if not has_invalid_date:
            print(f"      [SUCCESS] Date Key Validity Check passed for conformed date keys.")

        # F. Soft Delete Auditing Check
        print("\n  --> 7. Soft Delete Auditing Check")
        if "is_deleted" in batch_gold_df.columns:
            corrupt_delete_count = stats.get("corrupt_deletes") or 0
            if corrupt_delete_count > 0:
                err_msg = f"Soft Delete Auditing Check Failed: Found {corrupt_delete_count} deleted rows with missing delete metadata."
                print(f"      [ERROR] {err_msg}")
                sample = joined_df.filter((F.col("g.is_deleted") == True) & (F.col("g.deleted_at").isNull() | F.col("g.delete_batch_id").isNull())).limit(1).collect()[0]
                log_invalid_record(
                    table_session_id=table_session_id,
                    layer="GOLD",
                    target_table=table_name,
                    record_key=str(sample[grain_cols[0]]),
                    raw_data=str(sample.asDict()),
                    error_reason=err_msg,
                    error_column="deleted_at/delete_batch_id",
                    error_type=ErrorType.RULE
                )
                fail_pipeline = True
            else:
                print(f"      [SUCCESS] Soft Delete Auditing Check passed.")
        else:
            print(f"      [SUCCESS] Soft Delete Auditing Check passed (no is_deleted column).")

        # G. Row Count Reconciliation Check
        print("\n  --> 8. Row Count Reconciliation Check")
        silver_df = spark.table(silver_table_name).where(F.col("_batch_id") == F.lit(str(batch_id)))
        resolved_silver_grain_cols = silver_grain_cols or grain_cols

        actual_order_col = None
        for col in ["updated_at", "last_updated_at", "last_updated", "created_at"]:
            if col in silver_df.columns:
                actual_order_col = col
                break

        if actual_order_col:
            w = Window.partitionBy(*resolved_silver_grain_cols).orderBy(F.col(actual_order_col).desc_nulls_last())
            silver_recon_df = silver_df.withColumn("_rn", F.row_number().over(w)) \
                .where(F.col("_rn") == 1) \
                .drop("_rn")
        else:
            silver_recon_df = silver_df.dropDuplicates(resolved_silver_grain_cols)

        silver_recon_df = silver_recon_df.cache()
        silver_count = silver_recon_df.count()
        gold_count = total_rows

        row_diff = abs(silver_count - gold_count)
        row_diff_ratio = row_diff / silver_count if silver_count > 0 else (1.0 if row_diff > 0 else 0.0)
        max_row_ratio = THRESHOLDS["row_count_mismatch"]["max_ratio"]

        if row_diff_ratio > max_row_ratio:
            err_msg = f"Row Count Reconciliation Failed: Gold count ({gold_count}) does not match deduplicated Silver count ({silver_count}) (diff: {row_diff_ratio:.2%}, threshold: {max_row_ratio:.2%})."
            print(f"      [ERROR] {err_msg}")
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
            fail_pipeline = True
        else:
            print(f"      [SUCCESS] Row Count Reconciliation passed. Gold count ({gold_count}) matches deduplicated Silver count ({silver_count}) (diff: {row_diff_ratio:.2%}).")

        # H. Metric Reconciliation Check
        print("\n  --> 9. Metric Reconciliation Check")
        has_metric_recon_fail = False
        for metric_col in metric_cols:
            if "is_deleted" in silver_recon_df.columns:
                silver_metric_sum_col = F.sum(F.when(F.col("is_deleted") == True, F.lit(0.00)).otherwise(F.coalesce(F.col(metric_col), F.lit(0.00))))
            else:
                silver_metric_sum_col = F.sum(F.coalesce(F.col(metric_col), F.lit(0.00)))

            silver_metric_sum = silver_recon_df.select(silver_metric_sum_col.alias("metric_sum")).collect()[0]["metric_sum"]
            silver_metric_sum = float(silver_metric_sum) if silver_metric_sum is not None else 0.00

            gold_metric_sum = stats.get(f"gold_metric_{metric_col}")
            gold_metric_sum = float(gold_metric_sum) if gold_metric_sum is not None else 0.0

            variance = abs(silver_metric_sum - gold_metric_sum)
            metric_threshold = THRESHOLDS["metric_mismatch"].get(metric_col, 0.0001)

            if silver_metric_sum > 0:
                diff_ratio = variance / silver_metric_sum
            else:
                diff_ratio = 1.0 if variance > 0.01 else 0.0

            if diff_ratio > metric_threshold and variance > 0.01:
                err_msg = f"Metric Reconciliation Failed for {metric_col}: Silver sum = {silver_metric_sum:.2f}, Gold sum = {gold_metric_sum:.2f}, variance = {variance:.4f} (ratio: {diff_ratio:.4%}, threshold: {metric_threshold:.4%}) exceeds threshold."
                print(f"      [ERROR] {err_msg}")
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
                fail_pipeline = True
                has_metric_recon_fail = True
            else:
                print(f"      [SUCCESS] Metric Reconciliation passed for {metric_col}. Silver sum = {silver_metric_sum:.2f}, Gold sum = {gold_metric_sum:.2f} (variance: {variance:.4f}, ratio: {diff_ratio:.4%}).")
        if not metric_cols:
            print(f"      [SUCCESS] Metric Reconciliation Check passed (no metrics configured).")

        # 5. Finalize table session and status output map
        print("\n" + "-" * 80)
        status_key = f"{dim_fact_table_id} ({table_name})"
        if fail_pipeline:
            validation_statuses[status_key] = "FAILED"
            if not is_temp_session:
                finish_table_layer(table_session_id, "GOLD", "FAILED", error_code="VALIDATION_FAILED", error_message=f"Validation failed for table {table_name}")
            print(f"[RESULT] ❌ {table_name} failed QA audits.")
        else:
            validation_statuses[status_key] = "SUCCESS"
            if not is_temp_session:
                finish_table_layer(table_session_id, "GOLD", "SUCCESS")
            print(f"[RESULT]  {table_name} passed all QA audits successfully.")
        print("=" * 80 + "\n")

    finally:
        batch_gold_df.unpersist()
        if 'silver_recon_df' in locals():
            silver_recon_df.unpersist()

# ---------------------------------------------------------------------------
# Parallel Execution of Audits
# ---------------------------------------------------------------------------

import json

# Query active fact tables from the configuration table dynamically
fact_table_df = spark.table("cfg.dim_fact_table").where(F.col("table_type") == "FACT")
fact_table_rows = fact_table_df.collect()

# Create table name to (id, is_active) mapping
fact_name_to_info = {row["table_name"].lower(): (int(row["id"]), bool(row["is_active"])) for row in fact_table_rows}

# Initialize validation statuses for active fact tables
validation_statuses = {f"{f_id} (gold.{f_name})": "SUCCESS" for f_name, (f_id, is_act) in fact_name_to_info.items() if is_act}
validation_errors = {}

# Precompute and cache shared silver datasets to optimize joins and avoid redundant shuffles
try:
    print("[INFO] Pre-deduplicating and caching shared Silver datasets...")
    silver_vehicle_latest = latest_by_key(spark.table("silver.vehicle"), "customer_id").select("customer_id", "vehicle_id").cache()
    silver_quotation_latest = latest_by_key(spark.table("silver.quotation"), "quotation_id").select("quotation_id", "customer_id", "agent_id", "provider_code").cache()
    silver_policy_latest = latest_by_key(spark.table("silver.policy"), "policy_id").select("policy_id", "customer_id", "provider_code").cache()
    
    # Eager caching
    silver_vehicle_latest.count()
    silver_quotation_latest.count()
    silver_policy_latest.count()
except Exception as e:
    print(f"[WARNING] Failed to pre-cache Silver datasets: {e}")

def run_validation_safely(
    table_name: str,
    grain_cols: list[str],
    fk_mappings: dict,
    date_keys: list[str],
    silver_table_name: str,
    metric_cols: list[str],
    fk_bk_mappings: dict = {},
    scd2_temporal_mappings: dict = {},
    silver_grain_cols: list[str] = None
):
    entity_name = table_name.split(".")[-1].lower()
    fact_info = fact_name_to_info.get(entity_name)
    if not fact_info:
        print(f"[WARNING] Table {table_name} is not registered in cfg.dim_fact_table. Skipping validation.")
        return
        
    fact_id, is_active = fact_info
    if not is_active:
        print(f"[INFO] Table {table_name} is inactive. Skipping validation.")
        return
        
    try:
        validate_fact_table(
            table_name=table_name,
            dim_fact_table_id=fact_id,
            grain_cols=grain_cols,
            fk_mappings=fk_mappings,
            date_keys=date_keys,
            silver_table_name=silver_table_name,
            metric_cols=metric_cols,
            fk_bk_mappings=fk_bk_mappings,
            scd2_temporal_mappings=scd2_temporal_mappings,
            silver_grain_cols=silver_grain_cols
        )
    except Exception as e:
        status_key = f"{fact_id} ({table_name})"
        validation_statuses[status_key] = "FAILED"
        validation_errors[fact_id] = str(e)

# Define parallel validation tasks
from concurrent.futures import ThreadPoolExecutor

validation_tasks = [
    {
        "table_name": "gold.fact_quotation",
        "grain_cols": ["quotation_id"],
        "fk_mappings": {
            "quotation_key": "gold.dim_quotation",
            "customer_key": "gold.dim_customer",
            "agent_key": "gold.dim_agent",
            "provider_key": "gold.dim_provider",
            "package_key": "gold.dim_package",
            "quotation_status_key": "gold.dim_quotation_status",
            "vehicle_key": "gold.dim_vehicle"
        },
        "date_keys": ["quotation_date_key", "quotation_expiry_date_key"],
        "silver_table_name": "silver.quotation",
        "metric_cols": ["premium_amount"],
        "fk_bk_mappings": {
            "quotation_key": "quotation_id",
            "customer_key": "customer_id",
            "agent_key": "agent_id",
            "provider_key": "provider_code",
            "vehicle_key": "vehicle_id"
        },
        "scd2_temporal_mappings": {
            "customer_key": ("gold.dim_customer", "quotation_date_key"),
            "agent_key": ("gold.dim_agent", "quotation_date_key"),
            "provider_key": ("gold.dim_provider", "quotation_date_key"),
            "vehicle_key": ("gold.dim_vehicle", "quotation_date_key")
        }
    },
    {
        "table_name": "gold.fact_quotation_item",
        "grain_cols": ["quotation_id", "coverage_key"],
        "silver_grain_cols": ["quotation_id", "coverage_type"],
        "fk_mappings": {
            "quotation_key": "gold.dim_quotation",
            "customer_key": "gold.dim_customer",
            "agent_key": "gold.dim_agent",
            "provider_key": "gold.dim_provider",
            "package_key": "gold.dim_package",
            "quotation_status_key": "gold.dim_quotation_status",
            "coverage_key": "gold.dim_coverage",
            "vehicle_key": "gold.dim_vehicle"
        },
        "date_keys": ["quotation_date_key"],
        "silver_table_name": "silver.quotation_item",
        "metric_cols": ["coverage_amount", "deductible_amount"],
        "fk_bk_mappings": {
            "quotation_key": "quotation_id",
            "customer_key": "customer_id",
            "agent_key": "agent_id",
            "provider_key": "provider_code",
            "vehicle_key": "vehicle_id"
        },
        "scd2_temporal_mappings": {
            "customer_key": ("gold.dim_customer", "quotation_date_key"),
            "agent_key": ("gold.dim_agent", "quotation_date_key"),
            "provider_key": ("gold.dim_provider", "quotation_date_key"),
            "vehicle_key": ("gold.dim_vehicle", "quotation_date_key")
        }
    },
    {
        "table_name": "gold.fact_policy",
        "grain_cols": ["policy_id"],
        "fk_mappings": {
            "policy_key": "gold.dim_policy",
            "quotation_key": "gold.dim_quotation",
            "customer_key": "gold.dim_customer",
            "provider_key": "gold.dim_provider",
            "agent_key": "gold.dim_agent",
            "package_key": "gold.dim_package",
            "policy_status_key": "gold.dim_policy_status",
            "vehicle_key": "gold.dim_vehicle"
        },
        "date_keys": ["issued_date_key", "policy_start_date_key", "policy_end_date_key"],
        "silver_table_name": "silver.policy",
        "metric_cols": ["premium_amount"],
        "fk_bk_mappings": {
            "policy_key": "policy_id",
            "quotation_key": "quotation_id",
            "customer_key": "customer_id",
            "provider_key": "provider_code",
            "agent_key": "agent_id",
            "vehicle_key": "vehicle_id"
        },
        "scd2_temporal_mappings": {
            "customer_key": ("gold.dim_customer", "policy_start_date_key"),
            "provider_key": ("gold.dim_provider", "policy_start_date_key"),
            "agent_key": ("gold.dim_agent", "policy_start_date_key"),
            "vehicle_key": ("gold.dim_vehicle", "policy_start_date_key")
        }
    },
    {
        "table_name": "gold.fact_payment",
        "grain_cols": ["payment_id"],
        "fk_mappings": {
            "policy_key": "gold.dim_policy",
            "payment_status_key": "gold.dim_payment_status",
            "payment_method_key": "gold.dim_payment_method",
            "customer_key": "gold.dim_customer",
            "provider_key": "gold.dim_provider",
            "vehicle_key": "gold.dim_vehicle"
        },
        "date_keys": ["payment_date_key", "issued_date_key"],
        "silver_table_name": "silver.payment",
        "metric_cols": ["payment_amount"],
        "fk_bk_mappings": {
            "policy_key": "policy_id",
            "customer_key": "customer_id",
            "provider_key": "provider_code",
            "vehicle_key": "vehicle_id"
        },
        "scd2_temporal_mappings": {
            "customer_key": ("gold.dim_customer", "payment_date_key"),
            "provider_key": ("gold.dim_provider", "payment_date_key"),
            "vehicle_key": ("gold.dim_vehicle", "payment_date_key")
        }
    },
    {
        "table_name": "gold.fact_cancellation",
        "grain_cols": ["cancellation_id"],
        "fk_mappings": {
            "policy_key": "gold.dim_policy",
            "cancellation_reason_key": "gold.dim_cancellation_reason",
            "customer_key": "gold.dim_customer",
            "provider_key": "gold.dim_provider",
            "vehicle_key": "gold.dim_vehicle"
        },
        "date_keys": ["cancellation_date_key"],
        "silver_table_name": "silver.cancellation",
        "metric_cols": ["refund_amount"],
        "fk_bk_mappings": {
            "policy_key": "policy_id",
            "customer_key": "customer_id",
            "provider_key": "provider_code",
            "vehicle_key": "vehicle_id"
        },
        "scd2_temporal_mappings": {
            "customer_key": ("gold.dim_customer", "cancellation_date_key"),
            "provider_key": ("gold.dim_provider", "cancellation_date_key"),
            "vehicle_key": ("gold.dim_vehicle", "cancellation_date_key")
        }
    }
]

# Run validation tasks in parallel using ThreadPoolExecutor
try:
    with ThreadPoolExecutor(max_workers=len(validation_tasks)) as executor:
        futures = [executor.submit(run_validation_safely, **task) for task in validation_tasks]
        for future in futures:
            future.result()
finally:
    # Always clean up cache
    print("[INFO] Cleaning up cached Silver datasets...")
    try:
        silver_vehicle_latest.unpersist()
        silver_quotation_latest.unpersist()
        silver_policy_latest.unpersist()
    except Exception as e:
        print(f"[WARNING] Failed to unpersist cached datasets: {e}")

# Print summary of all table validations
print("\n" + "=" * 80)
print("                  FINAL QA RECONCILIATION SUMMARY")
print("=" * 80)
for key, status in sorted(validation_statuses.items()):
    icon = "❌" if status == "FAILED" else "✅"
    print(f" {icon} {key:<45} : {status}")
if validation_errors:
    print("\nErrors encountered during execution:")
    for f_id, err in validation_errors.items():
        print(f" - Table ID {f_id}: {err}")
print("=" * 80 + "\n")

# Exit returning status map in JSON
mssparkutils.notebook.exit(json.dumps(validation_statuses))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
