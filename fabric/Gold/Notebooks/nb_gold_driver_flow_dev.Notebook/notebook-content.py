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

%run nb_gold_fact_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils


def _notebook_param(name: str, default=None):
    return globals()[name] if name in globals() else default


p_fact_table = _notebook_param("p_fact_table", "ALL")
p_pipeline_name = _notebook_param("p_pipeline_name", DEFAULT_PIPELINE_NAME)
p_pipeline_run_id = _notebook_param("p_pipeline_run_id", None)
p_batch_id = _notebook_param("p_batch_id", None)
p_run_mode = _notebook_param("p_run_mode", "NEW")
p_enable_audit = as_bool(_notebook_param("p_enable_audit", True), True)

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

SUPPORTED_FACT_TABLES = [
    "fact_quotation",
    "fact_quotation_item",
    "fact_policy",
    "fact_payment",
    "fact_cancellation"
]


def selected_fact_tables(fact_table: str) -> List[str]:
    if is_blank(fact_table) or str(fact_table).upper() == "ALL":
        return SUPPORTED_FACT_TABLES
    fact_name = normalize_fact_name(fact_table)
    if fact_name not in SUPPORTED_FACT_TABLES:
        supported = ", ".join(SUPPORTED_FACT_TABLES)
        raise NotImplementedError(
            f"Gold fact driver currently supports {supported}. "
            f"Implement and validate fact tables first before expanding to {fact_name}."
        )
    return [fact_name]


def notebook_args(fact_table: str, session_id: str = None) -> Dict[str, str]:
    args = {
        "p_fact_table": fact_table,
        "p_pipeline_name": p_pipeline_name,
        "p_pipeline_run_id": p_pipeline_run_id,
        "p_batch_id": "" if is_blank(p_batch_id) else str(p_batch_id),
        "p_run_mode": p_run_mode,
        "p_enable_audit": "true" if as_bool(p_enable_audit, True) else "false",
        "useRootDefaultLakehouse": True,
    }
    if not is_blank(session_id):
        args["p_audit_session_id"] = str(session_id)
    return args


def validation_args(fact_table: str) -> Dict[str, str]:
    return {
        "p_fact_table": fact_table,
        "p_pipeline_run_id": p_pipeline_run_id,
        "p_batch_id": "" if is_blank(p_batch_id) else str(p_batch_id),
        "p_enable_audit": "true" if as_bool(p_enable_audit, True) else "false",
        "p_fail_on_validation_error": "true",
        "useRootDefaultLakehouse": True,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_gold_driver_flow() -> Dict:
    facts_to_run = selected_fact_tables(p_fact_table)
    session_id = None
    results = []

    try:
        if as_bool(p_enable_audit, True):
            session_id = start_gold_pipeline_audit(
                pipeline_name=p_pipeline_name,
                pipeline_run_id=p_pipeline_run_id,
                batch_id=p_batch_id,
                run_mode=p_run_mode,
                enable_audit=p_enable_audit,
            )

        for fact_table in facts_to_run:
            dependency_report = run_preflight_for_fact(
                fact_table,
                enable_audit=p_enable_audit,
            )
            print(f"Preflight passed for {fact_table}: {dependency_report}")

            build_result = mssparkutils.notebook.run(
                "nb_gold_fact_build_dev",
                3600,
                notebook_args(fact_table, session_id=session_id),
            )
            print(f"Build notebook completed for {fact_table}: {build_result}")

            validation_result = mssparkutils.notebook.run(
                "nb_gold_fact_validate_dev",
                1800,
                validation_args(fact_table),
            )
            print(f"Validation notebook completed for {fact_table}: {validation_result}")

            results.append({
                "fact_table": fact_table,
                "build_result": build_result,
                "validation_result": validation_result,
            })

        if as_bool(p_enable_audit, True):
            finish_pipeline_session(session_id, AuditStatus.SUCCESS)

        return {
            "pipeline_name": p_pipeline_name,
            "pipeline_run_id": p_pipeline_run_id,
            "batch_id": p_batch_id,
            "facts": results,
            "status": AuditStatus.SUCCESS.value,
        }

    except Exception:
        if as_bool(p_enable_audit, True) and not is_blank(session_id):
            finish_pipeline_session(session_id, AuditStatus.FAILED)
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

driver_result = run_gold_driver_flow()
print(driver_result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
