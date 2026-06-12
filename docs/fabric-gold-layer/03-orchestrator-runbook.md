# Gold Orchestrator Runbook

## Purpose

`nb_gold_orchestrator_dev` is an orchestration notebook for the Gold ingestion integration work.

It runs the Gold setup/dimension/fact notebooks sequentially so that testing does not depend on manual notebook order or leftover notebook session state. It is intentionally sequential and does not use parallel execution.

## Important Lakehouse prerequisite

Before running this notebook in Fabric, attach the team/dev Lakehouse as the default Lakehouse:

- Lakehouse name: `lh_insurance_dev`
- Expected workspace: the current dev/test workspace used for this branch

Do not commit workspace-specific Lakehouse attachment changes from Fabric unless the team explicitly agrees. Lakehouse bindings can be workspace-specific and may break for other users.

## Execution modes

### 1. Empty or new Lakehouse bootstrap

Use this when the attached Lakehouse is empty and Gold/config/log tables do not exist yet.

```python
p_execution_mode = "BOOTSTRAP_ONLY"
p_run_gold_create_tables = True
```

Expected behavior:

1. Create/setup config/control tables.
2. Create/setup audit/log tables.
3. Create Gold tables.
4. Run Gold static dimension setup.

This mode does not run SCD1, SCD2, or facts because Silver source tables may not exist yet.

### 2. Normal Ingestion flow

Use this when setup tables already exist and the Lakehouse has the required Silver data.

```python
p_execution_mode = "FULL_INGESTION"
p_run_gold_create_tables = False
```

Expected behavior:

1. Run setup prerequisites if enabled.
2. Run static dimensions.
3. Run SCD Type 1 dimensions.
4. Run SCD Type 2 dimensions.
5. Run supported Gold fact ingestion.
6. Run validation.

### 3. Dimensions-only flow

Use this when Silver tables already have data and the goal is to test Gold dimensions before fact ingestion.

```python
p_execution_mode = "DIMENSIONS_ONLY"
p_run_gold_create_tables = False
```

Expected behavior:

1. Run static dimensions.
2. Run SCD Type 1 dimensions.
3. Run SCD Type 2 dimensions.
4. Skip fact driver/validation.

## First bootstrap test evidence

The first run was executed with an empty attached Lakehouse:

```python
p_execution_mode = "BOOTSTRAP_ONLY"
p_run_gold_create_tables = True
```

Result summary:

| Step | Notebook | Status |
|---|---|---|
| 1 | `nb_cfg_etl_control_setup_dev` | SUCCESS |
| 2 | `nb_audit_pipeline_log_dev` | SUCCESS |
| 3 | `nb_gold_create_tables_dev` | SUCCESS |
| 4 | `nb_gold_static_dimension_setup_dev` | SUCCESS |

Observed preflight result:

- SCD1 Silver tables were missing, as expected for an empty Lakehouse.
- SCD2 Silver tables were missing, as expected for an empty Lakehouse.
- `fact_policy` dependencies were missing before Gold setup, as expected for bootstrap.
- The bootstrap plan correctly stopped at static dimension setup.
- The orchestration completed successfully.

## Validation queries

After bootstrap/static setup:

```sql
SELECT * FROM gold.dim_date LIMIT 10;

SELECT * FROM gold.dim_customer WHERE customer_key = -1;
SELECT * FROM gold.dim_agent WHERE agent_key = -1;
SELECT * FROM gold.dim_provider WHERE provider_key = -1;
SELECT * FROM gold.dim_vehicle WHERE vehicle_key = -1;
```

After SCD1:

```sql
SELECT package_code, COUNT(*)
FROM gold.dim_package
GROUP BY package_code
HAVING COUNT(*) > 1;

SELECT policy_id, COUNT(*)
FROM gold.dim_policy
GROUP BY policy_id
HAVING COUNT(*) > 1;
```

After SCD2:

```sql
SELECT customer_id, COUNT(*)
FROM gold.dim_customer
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT agent_id, COUNT(*)
FROM gold.dim_agent
WHERE is_current = true
GROUP BY agent_id
HAVING COUNT(*) > 1;

SELECT provider_id, COUNT(*)
FROM gold.dim_provider
WHERE is_current = true
GROUP BY provider_id
HAVING COUNT(*) > 1;

SELECT vehicle_id, COUNT(*)
FROM gold.dim_vehicle
WHERE is_current = true
GROUP BY vehicle_id
HAVING COUNT(*) > 1;
```

After fact policy run:

```sql
SELECT COUNT(*) FROM gold.fact_policy;

SELECT policy_id, COUNT(*)
FROM gold.fact_policy
GROUP BY policy_id
HAVING COUNT(*) > 1;
```

## Current limitations

- This notebook is a test/orchestration helper, not the final production Fabric pipeline.
- It runs notebooks sequentially to reduce free-capacity pressure.
- It does not solve full Bronze/Silver/Gold orchestration yet.
- It should not be used to repeatedly run destructive DDL against shared Lakehouse data.
- `p_run_gold_create_tables = True` should only be used for bootstrap/testing on a disposable or intentionally reset Lakehouse.

## Commit guidance

Recommended commit scope:

- `fabric/Gold/Notebooks/nb_gold_orchestrator_dev.Notebook/.platform`
- `fabric/Gold/Notebooks/nb_gold_orchestrator_dev.Notebook/notebook-content.py`
- `docs/gold-layer/03-orchestrator-runbook.md`

Avoid committing Fabric-generated Lakehouse attachment metadata or unrelated blank workspace changes.
