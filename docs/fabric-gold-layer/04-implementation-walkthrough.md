# Step-by-Step Gold Layer Ingestion Implementation & Dev Comparison

This document provides a detailed, step-by-step breakdown of the implemented features for [US-33: Gold Layer Ingestion](../../us-33.md) and compares the current feature branch `feature/us-33-implement-ingest-data-gold-layer` against the `dev` branch.

---

## 1. Step-by-Step Implementation Flow

To fulfill the requirements of the Gold Star Schema, the implementation was executed in a conformed dependency order (static setup -> dimensions -> facts -> validations -> orchestration).

### Step 1: Database DDL & Schema Validation Setup
- **Objective**: Ensure that the target Gold tables exist in the Microsoft Fabric Lakehouse catalog and have the expected column data types before running any ingestion.
- **Implementation**:
  - Implemented the schema verification dictionary `EXPECTED_SCHEMAS` mapping all 14 dimensions and 5 facts.
  - Implemented the helper function `validate_gold_schemas()` to compare catalog schemas against expected structures and raise warnings/errors for mismatches.
  - Placed this logic in [nb_gold_static_dimension_setup_dev](../../fabric/Gold/Notebooks/nb_gold_static_dimension_setup_dev.Notebook/notebook-content.py).

### Step 2: Date Dimension Generation & Unknown Member Population (Task 131)
- **Objective**: Populate the static calendar dimension `dim_date` and insert the `-1` Unknown member into all dimension tables to prevent key resolution leaks during fact ingestion.
- **Implementation**:
  - Implemented `generate_dim_date()` to build calendar dates from `2020-01-01` to `2030-12-31` with conformed columns (`date_key`, `full_date`, `day_number`, `day_name`, `week_number`, `month_number`, `month_name`, `quarter_number`, `year_number`, `year_month`, `is_weekend`).
  - Implemented `insert_unknown_members()` using SQL `MERGE INTO` queries to idempotently insert the `-1` Unknown key and `'UNKNOWN'` text placeholders for all dimensions (like `dim_customer`, `dim_agent`, etc.).
  - Placed this logic in [nb_gold_static_dimension_setup_dev](../../fabric/Gold/Notebooks/nb_gold_static_dimension_setup_dev.Notebook/notebook-content.py).

### Step 3: Slowly Changing Dimensions (SCD) Type 1 Loading (Task 132)
- **Objective**: Load reference and status dimensions where changes overwrite the target attributes in-place without preserving historical versions.
- **Implementation**:
  - Built a generic loader function `load_scd1_dimension()` in [nb_gold_dim_scd1_load_dev.py](../../fabric/Gold/Notebooks/nb_gold_dim_scd1_load_dev.py.Notebook/notebook-content.py).
  - The loader:
    1. Identifies new business keys.
    2. Generates surrogate keys dynamically based on the current maximum key in target table + incrementing row number.
    3. Merges data using Delta merge, updating target fields only when a change is detected.
  - Configured and executed ingestion for all 9 Type 1 dimensions:
    - `dim_package` (distinct package codes from `silver.quotation`)
    - `dim_coverage` (distinct coverage types from `silver.quotation_item`)
    - `dim_quotation` (quotations header keys and expiry dates)
    - `dim_policy` (policy business keys)
    - `dim_quotation_status`, `dim_policy_status`, `dim_payment_status` (distinct status codes)
    - `dim_payment_method` (distinct conformed method codes like `BANK_TRANSFER`, `CREDIT_CARD`, `E_WALLET`)
    - `dim_cancellation_reason` (distinct cancellation reasons)

### Step 4: Slowly Changing Dimensions (SCD) Type 2 Loading (Task 133)
- **Objective**: Load historical dimensions preserving change history when tracked attributes change.
- **Implementation**:
  - Developed a generic, hybrid SCD Type 1/2 loader `load_scd2_dimension()` in [nb_gold_dim_scd2_load_dev.py](../../fabric/Gold/Notebooks/nb_gold_dim_scd2_load_dev.py.Notebook/notebook-content.py).
  - The loader uses MD5 hash checks on tracked fields and routes incoming data into three paths:
    - **Insert (Path A)**: Inserts a new active version (`is_current = true`, `effective_to = '9999-12-31 23:59:59'`, and `effective_from = source_date`).
    - **Expire (Path B)**: Expires the old active version by setting `is_current = false` and `effective_to` to the new version's `effective_from`.
    - **Type 1 In-Place Update (Path C)**: Performs in-place updates on active records for non-tracked descriptive columns (e.g. customer phone or email).
  - Executed this for the 4 Type 2 dimensions:
    - `dim_customer` (tracked fields: `city`, `district`)
    - `dim_agent` (tracked fields: `region`, `branch`, `manager_name`)
    - `dim_provider` (tracked fields: `provider_group`, `active_flag`)
    - `dim_vehicle` (tracked fields: `vehicle_value`)

### Step 5: Fact Table Ingestion & Quality Validation (Task 134 - Phase 1)
- **Objective**: Load conformed facts, resolve surrogate keys temporally, populate lineage columns, support soft deletes, and perform reconciliation audits.
- **Implementation**:
  - Integrated `nb_gold_fact_helper_dev` helpers like `lookup_scd2_key()` to match the event date against `effective_from` and `effective_to` of the SCD2 dimensions.
  - Implemented the first fact build for **`fact_policy`** (`nb_gold_fact_build_dev`).
  - Implemented the data quality validations for `fact_policy` (`nb_gold_fact_validate_dev`) verifying row counts, degenerate keys, non-null requirements, premium amount reconciliation, and soft-delete properties.

### Step 6: Sequenced Pipeline Orchestration
- **Objective**: Run the end-to-end ingestion flow safely from a single controller.
- **Implementation**:
  - Created the pipeline orchestrator [nb_gold_orchestrator_dev](../../fabric/Gold/Notebooks/nb_gold_orchestrator_dev.Notebook/notebook-content.py) to execute notebooks sequentially with preflight checks and status auditing.
  - Supported execution modes like `BOOTSTRAP_ONLY`, `DIMENSIONS_ONLY`, and `FULL_INGESTION`.
  - Documented instructions in the runbook [03-orchestrator-runbook.md](./03-orchestrator-runbook.md).

---

## 2. Comparison with the `dev` Branch

Comparing `feature/us-33-implement-ingest-data-gold-layer` to the target `dev` branch highlights the specific changes made in this development cycle.

### Change Log Matrix

| Status | File Path | Component / Layer | Details of Changes |
|---|---|---|---|
| **[NEW]** | [nb_gold_static_dimension_setup_dev](../../fabric/Gold/Notebooks/nb_gold_static_dimension_setup_dev.Notebook/notebook-content.py) | Gold / Setup | Creates date dimensions and idempotently populates conformed `-1` Unknown member records. |
| **[NEW]** | [nb_gold_dim_scd1_load_dev.py](../../fabric/Gold/Notebooks/nb_gold_dim_scd1_load_dev.py.Notebook/notebook-content.py) | Gold / Dimension | Implements the generic `load_scd1_dimension` logic and runs ingestion for all 9 SCD Type 1 tables. |
| **[NEW]** | [nb_gold_dim_scd2_load_dev.py](../../fabric/Gold/Notebooks/nb_gold_dim_scd2_load_dev.py.Notebook/notebook-content.py) | Gold / Dimension | Implements the hybrid `load_scd2_dimension` logic and runs ingestion for the 4 SCD Type 2 tables. |
| **[NEW]** | [nb_gold_orchestrator_dev](../../fabric/Gold/Notebooks/nb_gold_orchestrator_dev.Notebook/notebook-content.py) | Gold / Orchestration | Sequential controller for orchestrating setup, SCD1, SCD2, driver flow and validations. |
| **[NEW]** | [03-orchestrator-runbook.md](./03-orchestrator-runbook.md) | Documentation | Runbook instructions, execution modes, validation SQL queries, and commit guidance. |
| **[MODIFY]**| [nb_gold_driver_flow_dev](../../fabric/Gold/Notebooks/nb_gold_driver_flow_dev.Notebook/notebook-content.py) | Gold / Orchestration | Added `useRootDefaultLakehouse` parameter mapping and resolved minor formatting issues. |
| **[MODIFY]**| `docs/source-to-target-mapping/jsons/...` | Config / Mapping | Updated default source-to-target mapping JSON configs to match the Gold structure. |
| **[MODIFY]**| [nb_cfg_etl_control_setup_dev](../../fabric/Config/Pipeline-Control/nb_cfg_etl_control_setup_dev.Notebook/notebook-content.py) | Config / Control | Adjusted setup configuration parameters to support conformed Gold metadata columns. |

---

## 3. Analysis of Key Differences & Architectural Enhancements

1. **Existence of Dimensions Ingestion**:
   - **On `dev` branch**: Ingestion logic for dimension tables was completely missing. Only DDL definitions and empty tables existed. Fact builders could not resolve surrogate keys as the dimensions were empty.
   - **On current feature branch**: All 13 dimension tables are fully loaded from Silver sources. Dynamic surrogate keys are generated, and static dates (`dim_date`) plus `-1` defaults are inserted.

2. **Temporal Key Resolution Capability**:
   - **On `dev` branch**: The helper notebook `nb_gold_fact_helper_dev` existed, but could not be integrated because the dimension tables were empty, preventing SCD Type 2 history testing.
   - **On current feature branch**: Fact records (currently `fact_policy`) successfully perform left joins against `dim_customer`, `dim_provider`, `dim_agent`, and `dim_vehicle` matching the transaction timestamp with the dimension effective windows (`effective_from` and `effective_to`), ensuring reporting analytics reflect historical truth.

3. **Safe Orchestration**:
   - **On `dev` branch**: Developers had to trigger individual notebooks manually in specific sequences to prevent dependency failures.
   - **On current feature branch**: The introduction of `nb_gold_orchestrator_dev` allows automated sequential execution with preflight guards, skipping steps gracefully if prerequisites are missing.
