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

# MAGIC %run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_session_id = ""
p_batch_id = ""
p_run_mode = "NEW"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
from pyspark.sql import functions as F

# Cast parameters safely
p_batch_id = int(p_batch_id) if p_batch_id else 0
p_session_id = str(p_session_id)
p_run_mode = str(p_run_mode).upper()

print(f"[MASTER] Gold Layer master load initiated. Session ID: {p_session_id}, Batch ID: {p_batch_id}, Run Mode: {p_run_mode}")

# Prepare parameters to propagate to child notebooks
common_args = {
    "session_id": p_session_id,
    "batch_id": p_batch_id,
    "run_mode": p_run_mode
}

def resolve_source_success(batch_id: int):
    # Query mappings between Gold tables and source table IDs
    mappings = spark.table("cfg.source_dim_fact").collect()
    
    # Group conformed dimension/fact table IDs by source_table_id
    from collections import defaultdict
    source_to_targets = defaultdict(list)
    for row in mappings:
        source_to_targets[row["source_table_id"]].append(row["dim_fact_table_id"])
    
    # Get all successful table sessions for this batch
    gold_statuses = spark.table("log.audit_table_session") \
                         .where(F.col("batch_id") == F.lit(batch_id)) \
                         .select("source_table_id", "gold_status") \
                         .collect()
    
    success_table_ids = {row["source_table_id"] for row in gold_statuses if row["gold_status"] == "SUCCESS"}

    # Evaluate the 9 active sources
    for src_id in range(1, 10):
        mapped_targets = source_to_targets[src_id]
        if mapped_targets and all(tgt_id in success_table_ids for tgt_id in mapped_targets):
            # All target conformed dimensions/facts processed successfully. Update source log.
            spark.sql(f"""
                UPDATE log.audit_table_session
                SET gold_status = 'SUCCESS',
                    table_session_status = 'SUCCESS',
                    gold_ended_at = current_timestamp(),
                    updated_at = current_timestamp()
                WHERE batch_id = {batch_id} AND source_table_id = {src_id}
            """)
            print(f"[MASTER] Resolved Source Table {src_id} success status: SUCCESS")
        else:
            # If not all mapped targets succeeded, flag source table as FAILED
            spark.sql(f"""
                UPDATE log.audit_table_session
                SET gold_status = 'FAILED',
                    table_session_status = 'FAILED',
                    gold_ended_at = current_timestamp(),
                    updated_at = current_timestamp()
                WHERE batch_id = {batch_id} AND source_table_id = {src_id}
            """)
            print(f"[MASTER] Resolved Source Table {src_id} success status: FAILED (one or more target lookups failed)")

try:
    # 1. Date Dimension Setup (ID: 1)
    print("[MASTER] Running Date Setup...")
    mssparkutils.notebook.run("nb_gold_load_dim_date_dev", 1800, common_args)

    # 2. SCD1 Dimensions Loading (IDs: 5, 6, 7, 8, 9, 10, 11, 12, 13)
    print("[MASTER] Running SCD1 Ingestions...")
    mssparkutils.notebook.run("nb_gold_load_scd1_dimensions_dev", 1800, common_args)

    # 3. SCD2 Dimensions Loading (IDs: 2, 3, 4, 14)
    print("[MASTER] Running SCD2 Ingestions...")
    mssparkutils.notebook.run("nb_gold_load_scd2_dimensions_dev", 1800, common_args)

    # 4. Fact Ingestions
    print("[MASTER] Running Fact Quotation Ingestion...")
    mssparkutils.notebook.run("nb_gold_load_fact_quotation_dev", 1800, common_args)

    print("[MASTER] Running Fact Quotation Item Ingestion...")
    mssparkutils.notebook.run("nb_gold_load_fact_quotation_item_dev", 1800, common_args)

    print("[MASTER] Running Fact Policy Ingestion...")
    mssparkutils.notebook.run("nb_gold_load_fact_policy_dev", 1800, common_args)

    print("[MASTER] Running Fact Payment Ingestion...")
    mssparkutils.notebook.run("nb_gold_load_fact_payment_dev", 1800, common_args)

    print("[MASTER] Running Fact Cancellation Ingestion...")
    mssparkutils.notebook.run("nb_gold_load_fact_cancellation_dev", 1800, common_args)

    # 5. Validation Check Suite
    print("[MASTER] Running Validation Checks...")
    mssparkutils.notebook.run("nb_gold_validate_reconciliation_dev", 1800, common_args)

    # 6. Post-Ingestion Source Status Resolution
    print("[MASTER] Running Source Success Matrix Resolution...")
    resolve_source_success(p_batch_id)

    # 7. Complete master session successfully
    print("[MASTER] Finishing pipeline session successfully...")
    finish_pipeline_session(p_session_id, "SUCCESS")

    # 8. Reset next run mode control table to NEW
    print("[MASTER] Resetting next_run_mode to NEW...")
    reset_next_run_mode()

    print("[MASTER] Gold Layer Master Ingestion completed successfully.")
    mssparkutils.notebook.exit("SUCCESS")

except Exception as err:
    print(f"[MASTER ERROR] Gold Layer pipeline execution failed: {err}")
    # Propagate exception to trigger handle_failed_gold downstream error handler
    raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
