# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c8fc794-e72d-4c37-8b73-1adf7e8c1529",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "a562f741-0da9-4508-be62-0c9caf763e5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "2c8fc794-e72d-4c37-8b73-1adf7e8c1529"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric notebook: nb_gold_phase1_orchestrator_dev
# Purpose: Run Gold Phase 1 safely from one notebook, sequentially, with preflight guards.

from datetime import datetime, timezone
import json
import traceback

# =============================================================================
# PARAMETERS
# =============================================================================

# Use BOOTSTRAP_ONLY first if Lakehouse is empty.
# Options:
# - BOOTSTRAP_ONLY: create cfg/log/gold tables + static dim setup only
# - DIMENSIONS_ONLY: run static + SCD1 + SCD2 if Silver exists
# - FULL_PHASE1: run setup/static/dimensions/fact_policy/validation where possible
p_execution_mode = "FULL_PHASE1"

p_timeout_seconds = 3600
p_stop_on_failure = True

# Keep True for first run in an empty/disposable dev Lakehouse.
# WARNING: gold_create_tables may DROP/CREATE Gold tables depending on current notebook logic.
p_run_gold_create_tables = False

p_batch_id = None
p_run_mode = "NEW"
p_layer = "GOLD"
p_fact_table = "fact_policy"
p_enable_audit = True

common_args = {
    "p_batch_id": p_batch_id,
    "p_run_mode": p_run_mode,
    "p_layer": p_layer,
    "p_fact_table": p_fact_table,
    "p_enable_audit": str(p_enable_audit).lower(),
    "useRootDefaultLakehouse": True,
}

try:
    nb = notebookutils.notebook
except NameError:
    nb = mssparkutils.notebook


# =============================================================================
# HELPERS
# =============================================================================

def table_exists(table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        try:
            spark.table(table_name).limit(1).count()
            return True
        except Exception:
            return False


def missing_tables(table_names):
    return [table_name for table_name in table_names if not table_exists(table_name)]


def run_step(step, step_no, total_steps):
    step_start = datetime.now(timezone.utc)

    print("\n" + "=" * 100)
    print(f"STEP {step_no}/{total_steps}: {step['name']}")
    print(f"Notebook: {step['notebook']}")
    print(f"Type: {step['type']}")
    print(f"Required: {step['required']}")
    print(f"Started: {step_start.isoformat()}")
    print("=" * 100)

    try:
        exit_value = nb.run(
            step["notebook"],
            int(step.get("timeout_seconds", p_timeout_seconds)),
            step.get("args", common_args),
        )

        step_end = datetime.now(timezone.utc)
        print(f"SUCCESS: {step['name']}")
        print(f"Finished: {step_end.isoformat()}")
        print(f"Exit value: {exit_value}")

        return {
            "step": step_no,
            "name": step["name"],
            "notebook": step["notebook"],
            "type": step["type"],
            "required": step["required"],
            "status": "SUCCESS",
            "started_at": step_start.isoformat(),
            "finished_at": step_end.isoformat(),
            "exit_value": exit_value,
            "error": None,
        }

    except Exception as exc:
        step_end = datetime.now(timezone.utc)
        error_text = traceback.format_exc()

        print(f"FAILED: {step['name']}")
        print(f"Finished: {step_end.isoformat()}")
        print("Error:")
        print(error_text)

        return {
            "step": step_no,
            "name": step["name"],
            "notebook": step["notebook"],
            "type": step["type"],
            "required": step["required"],
            "status": "FAILED",
            "started_at": step_start.isoformat(),
            "finished_at": step_end.isoformat(),
            "exit_value": None,
            "error": str(exc),
        }


# =============================================================================
# PREFLIGHT
# =============================================================================

REQUIRED_SILVER_FOR_SCD1 = [
    "silver.quotation",
    "silver.quotation_item",
    "silver.policy",
    "silver.payment",
    "silver.cancellation",
]

REQUIRED_SILVER_FOR_SCD2 = [
    "silver.customer",
    "silver.agent",
    "silver.provider",
    "silver.vehicle",
]

REQUIRED_GOLD_FOR_FACT_POLICY = [
    "gold.dim_date",
    "gold.dim_policy",
    "gold.dim_quotation",
    "gold.dim_customer",
    "gold.dim_provider",
    "gold.dim_agent",
    "gold.dim_package",
    "gold.dim_policy_status",
    "gold.dim_vehicle",
    "gold.fact_policy",
]

missing_scd1 = missing_tables(REQUIRED_SILVER_FOR_SCD1)
missing_scd2 = missing_tables(REQUIRED_SILVER_FOR_SCD2)
missing_fact = missing_tables(REQUIRED_GOLD_FOR_FACT_POLICY)

print("=" * 100)
print("Gold Phase 1 Orchestrator Preflight")
print("=" * 100)
print(f"Execution mode: {p_execution_mode}")
print(f"Batch ID: {p_batch_id}")
print(f"Run mode: {p_run_mode}")
print(f"Started at: {datetime.now(timezone.utc).isoformat()}")
print()
print(f"Missing SCD1 Silver tables: {missing_scd1}")
print(f"Missing SCD2 Silver tables: {missing_scd2}")
print(f"Missing fact_policy Gold dependencies: {missing_fact}")


# =============================================================================
# BUILD SAFE EXECUTION PLAN
# =============================================================================

steps = []

# Always safe setup candidates.
steps.extend([
    {
        "name": "config_control_setup",
        "notebook": "nb_cfg_etl_control_setup_dev",
        "required": True,
        "type": "setup",
    },
    {
        "name": "audit_log_setup",
        "notebook": "nb_audit_pipeline_log_dev",
        "required": True,
        "type": "setup",
    },
])

if p_run_gold_create_tables:
    steps.append({
        "name": "gold_create_tables",
        "notebook": "nb_gold_create_tables_dev",
        "required": True,
        "type": "setup",
    })

steps.append({
    "name": "gold_static_dimension_setup",
    "notebook": "nb_gold_static_dimension_setup_dev",
    "required": True,
    "type": "gold_static",
})

if p_execution_mode in {"DIMENSIONS_ONLY", "FULL_PHASE1"}:
    if missing_scd1:
        print("\nSkipping SCD1 because required Silver tables are missing.")
    else:
        steps.append({
            "name": "gold_scd1_dimension_load",
            "notebook": "nb_gold_dim_scd1_load_dev.py",
            "required": True,
            "type": "gold_dimension",
        })

    if missing_scd2:
        print("\nSkipping SCD2 because required Silver tables are missing.")
    else:
        steps.append({
            "name": "gold_scd2_dimension_load",
            "notebook": "nb_gold_dim_scd2_load_dev.py",
            "required": True,
            "type": "gold_dimension",
        })

if p_execution_mode == "FULL_PHASE1":
    if missing_scd1 or missing_scd2 or missing_fact:
        print("\nSkipping fact driver/validation because Silver or Gold dependencies are missing.")
    else:
        steps.extend([
            {
                "name": "gold_fact_driver_flow",
                "notebook": "nb_gold_driver_flow_dev",
                "required": False,
                "type": "gold_fact",
            }
        ])

print("\nExecution plan:")
for index, step in enumerate(steps, start=1):
    print(f"{index}. [{step['type']}] {step['name']} -> {step['notebook']}")


# =============================================================================
# RUN SEQUENTIALLY
# =============================================================================

results = []

for index, step in enumerate(steps, start=1):
    result = run_step(step, index, len(steps))
    results.append(result)

    failed = result["status"] == "FAILED"
    must_stop = failed and (p_stop_on_failure or result["required"])

    if must_stop:
        print("\nStopping orchestrator because this step failed.")
        break


# =============================================================================
# SUMMARY
# =============================================================================

failed = [r for r in results if r["status"] == "FAILED"]
succeeded = [r for r in results if r["status"] == "SUCCESS"]

summary = {
    "execution_mode": p_execution_mode,
    "batch_id": p_batch_id,
    "run_mode": p_run_mode,
    "total_planned_steps": len(steps),
    "succeeded": len(succeeded),
    "failed": len(failed),
    "missing_scd1_silver_tables": missing_scd1,
    "missing_scd2_silver_tables": missing_scd2,
    "missing_fact_policy_dependencies": missing_fact,
    "results": results,
}

print("\n" + "=" * 100)
print("Gold Phase 1 Orchestrator Summary")
print("=" * 100)

for r in results:
    print(
        f"{r['step']}. {r['name']} | {r['notebook']} | "
        f"{r['status']} | required={r['required']}"
    )
    if r["error"]:
        print(f"   Error: {r['error']}")

print("\nJSON summary:")
print(json.dumps(summary, indent=2))

if failed:
    raise Exception(
        f"Gold Phase 1 orchestration failed at step "
        f"{failed[0]['step']}: {failed[0]['name']} -> {failed[0]['notebook']}"
    )

print("\nGold Phase 1 orchestration completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
