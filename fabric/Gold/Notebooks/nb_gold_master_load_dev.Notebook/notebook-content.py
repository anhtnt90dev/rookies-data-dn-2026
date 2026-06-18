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

# PARAMETERS CELL ********************

p_session_id = ""
p_batch_id = ""
p_run_mode = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Build mappings for logging (ID to human-readable names)
try:
    id_to_table_name = {int(row["id"]): row["table_name"] for row in spark.table("cfg.dim_fact_table").select("id", "table_name").collect()}
except Exception:
    id_to_table_name = {}

try:
    source_id_to_name = {int(row["id"]): row["source_name"] for row in spark.table("cfg.source_table").select("id", "source_name").collect()}
except Exception:
    source_id_to_name = {}

def resolve_source_success(batch_id: int, conformed_statuses: dict):
    # Query mappings between Gold tables and source table IDs
    mappings = spark.table("cfg.source_dim_fact").collect()
    
    # Group conformed dimension/fact table IDs by source_table_id
    from collections import defaultdict
    source_to_targets = defaultdict(list)
    for row in mappings:
        source_to_targets[row["source_table_id"]].append(row["dim_fact_table_id"])
    
    # Evaluate the 9 active sources
    for src_id in range(1, 10):
        mapped_targets = source_to_targets[src_id]
        if mapped_targets and all(conformed_statuses.get(tgt_id) == "SUCCESS" for tgt_id in mapped_targets):
            # All target conformed dimensions/facts processed successfully. Update source log.
            spark.sql(f"""
                UPDATE log.audit_table_session
                SET gold_status = 'SUCCESS',
                    table_session_status = 'SUCCESS',
                    gold_ended_at = current_timestamp(),
                    updated_at = current_timestamp()
                WHERE batch_id = {batch_id} AND source_table_id = {src_id}
            """)
            src_name = source_id_to_name.get(src_id, f"ID {src_id}")
            print(f"[MASTER] Resolved Source Table {src_name} success status: SUCCESS")
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
            src_name = source_id_to_name.get(src_id, f"ID {src_id}")
            print(f"[MASTER] Resolved Source Table {src_name} success status: FAILED (one or more dependent tables failed)")

# Track conformed table load statuses dynamically based on the configuration table
all_table_ids = [int(row["id"]) for row in spark.table("cfg.dim_fact_table").select("id").collect()]
conformed_statuses = {table_id: "FAILED" for table_id in all_table_ids}
is_success = False

try:
    # Query mappings dynamically from the control configuration table
    dim_fact_config = spark.table("cfg.dim_fact_table").where(F.col("is_active") == True).collect()

    # 1. Run Dimensions Setup in Parallel (dynamically determined)
    dim_tasks = [
        (row["gold_transform_name"], {**common_args, "p_table_id": str(row["id"])}, int(row["id"]))
        for row in dim_fact_config if row["table_type"] == "DIM"
    ]

    print(f"[MASTER] Running {len(dim_tasks)} Dimensions in parallel (max_workers=15)...")
    dim_failures = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {
            executor.submit(mssparkutils.notebook.run, nb_name, 1800, args): (nb_name, table_id)
            for nb_name, args, table_id in dim_tasks
        }
        for future in as_completed(futures):
            nb_name, table_id = futures[future]
            try:
                future.result()
                conformed_statuses[table_id] = "SUCCESS"
                t_name = id_to_table_name.get(table_id, f"ID {table_id}")
                print(f"[MASTER] Dimension table {t_name} loaded successfully.")
            except Exception as e:
                t_name = id_to_table_name.get(table_id, f"ID {table_id}")
                print(f"[MASTER ERROR] Dimension table {t_name} ({nb_name}) failed: {e}")
                dim_failures.append((table_id, e))

    if dim_failures:
        failed_dim_names = [id_to_table_name.get(f[0], f"ID {f[0]}") for f in dim_failures]
        raise Exception(f"Failed to load dimensions: {failed_dim_names}")

    # 2. Run Fact Ingestions in Parallel (dynamically determined)
    fact_tasks = [
        (row["gold_transform_name"], {**common_args, "p_table_id": str(row["id"])}, int(row["id"]))
        for row in dim_fact_config if row["table_type"] == "FACT"
    ]

    print(f"[MASTER] Running {len(fact_tasks)} Facts in parallel (max_workers=5)...")
    fact_failures = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(mssparkutils.notebook.run, nb_name, 1800, args): (nb_name, table_id)
            for nb_name, args, table_id in fact_tasks
        }
        for future in as_completed(futures):
            nb_name, table_id = futures[future]
            try:
                future.result()
                conformed_statuses[table_id] = "SUCCESS"
                t_name = id_to_table_name.get(table_id, f"ID {table_id}")
                print(f"[MASTER] Fact table {t_name} loaded successfully.")
            except Exception as e:
                t_name = id_to_table_name.get(table_id, f"ID {table_id}")
                print(f"[MASTER ERROR] Fact table {t_name} ({nb_name}) failed: {e}")
                fact_failures.append((table_id, e))

    if fact_failures:
        failed_fact_names = [id_to_table_name.get(f[0], f"ID {f[0]}") for f in fact_failures]
        raise Exception(f"Failed to load facts: {failed_fact_names}")

    # 5. Validation Check Suite
    print("[MASTER] Running Validation Checks...")
    try:
        val_result_str = mssparkutils.notebook.run("nb_gold_validate_reconciliation_dev", 1800, common_args)
        import json
        try:
            val_results = json.loads(val_result_str)
            val_results_named = {id_to_table_name.get(int(fid), f"ID {fid}"): status for fid, status in val_results.items()}
            print(f"[MASTER] Validation results received: {val_results_named}")
            for fact_id_str, status in val_results.items():
                conformed_statuses[int(fact_id_str)] = status
            
            # If any validation failed, raise an Exception so that the master load notebook fails
            failed_facts = [fid for fid, status in val_results.items() if status == "FAILED"]
            if failed_facts:
                failed_fact_names = [id_to_table_name.get(int(fid), f"ID {fid}") for fid in failed_facts]
                raise Exception(f"Validation checks failed for facts: {failed_fact_names}")
        except json.JSONDecodeError:
            print("[MASTER WARNING] Could not parse validation results as JSON. Defaulting all active facts to FAILED.")
            active_fact_ids = [int(row["id"]) for row in dim_fact_config if row["table_type"] == "FACT"]
            for fact_id in active_fact_ids:
                conformed_statuses[fact_id] = "FAILED"
            raise Exception("Validation notebook returned invalid format.")
    except Exception as val_err:
        # Only overwrite conformed_statuses to FAILED if it wasn't a standard validation failure (e.g. timeout, run error, etc.)
        if "Validation checks failed for facts" not in str(val_err):
            active_fact_ids = [int(row["id"]) for row in dim_fact_config if row["table_type"] == "FACT"]
            for fact_id in active_fact_ids:
                conformed_statuses[fact_id] = "FAILED"
        raise val_err

    # 6. Post-Ingestion Source Status Resolution
    print("[MASTER] Running Source Success Matrix Resolution...")
    resolve_source_success(p_batch_id, conformed_statuses)

    # 7. Complete master session successfully
    print("[MASTER] Finishing pipeline session successfully...")
    finish_pipeline_session(p_session_id, "SUCCESS")

    # 8. Reset next run mode control table to NEW
    print("[MASTER] Resetting next_run_mode to NEW...")
    reset_next_run_mode()

    print("[MASTER] Gold Layer Master Ingestion completed successfully.")
    is_success = True

except Exception as err:
    print(f"[MASTER ERROR] Gold Layer pipeline execution failed: {err}")
    try:
        resolve_source_success(p_batch_id, conformed_statuses)
    except Exception as resolve_err:
        print(f"[MASTER ERROR] Failed to resolve source status post-failure: {resolve_err}")
    # Propagate exception to trigger handle_failed_gold downstream error handler
    raise err

if is_success:
    mssparkutils.notebook.exit("SUCCESS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
