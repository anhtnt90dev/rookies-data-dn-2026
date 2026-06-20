# 04 - Audit and Validation Specification

This document defines the operational logic for auditing, state transitions, post-ingestion validation rules, and success/failure handling in the Gold Layer.

---

## 1. Run Modes & State Transitions

The execution behavior of the Gold Layer is driven dynamically by the single-row control table `cfg.next_run_mode` and tracked in the `log` schema.

```mermaid
stateDiagram-v2
    [*] --> NEW : "Default Success State"
    NEW --> SUCCESS : "All Ingestions &<br/>Validation Pass"
    NEW --> FAILED : "Any table load or<br/>validation fails"
    FAILED --> RECOVERY : "Run mode updated<br/>to RECOVERY"
    RECOVERY --> SUCCESS : "Failed batch completes<br/>successfully"
    RECOVERY --> FAILED : "Rerun fails"
    SUCCESS --> NEW : "Reset next<br/>run mode"
```

### 1.1. Ingestion Success Path
When all dimensions and fact tables complete execution in the master orchestrator (`nb_gold_master_load_dev`):
1.  **Flag Audit**: Updates `log.audit_session` status to `SUCCESS` and calculates session duration.
2.  **Reset Run Mode**: Invokes `reset_next_run_mode()` to reset `cfg.next_run_mode` back to:
    - `next_run_mode = 'NEW'`
    - `batch_id = NULL`
    - `session_id = NULL`
3.  **Next Ingestion**: The next pipeline run begins a brand-new batch ID.

### 1.2. Ingestion Failure Path
If any notebook in the parallel pools of the master orchestrator raises an exception:
1.  **Mark Recovery**: The pipeline failure handler (`handle_failed_gold`) triggers and updates `cfg.next_run_mode` to:
    - `next_run_mode = 'RECOVERY'`
    - `batch_id = <current_batch_id>` (retains the active batch)
    - `session_id = <failed_session_id>` (preserves lineage)
2.  **Fail Audit**: Logs `session_status = 'FAILED'` in `log.audit_session`.
3.  **Abort**: The orchestrator raises a Python exception to fail the Fabric pipeline activity `nb_ingestion_gold`, alerting administrators.

---

## 2. Ingestion Source Success Matrix

To evaluate end-to-end data processing completeness, target conformed dimensions and fact table successes are mapped back to their ingestion sources. A source is marked as successful (`gold_status = 'SUCCESS'`) in `log.audit_table_session` **only if all target Gold tables associated with it have succeeded**.

```mermaid
graph TD
    subgraph "Ingestion Source Resolution"
        S1["Source Table Ingestion"]
        T1[("Associated Gold Dim")]
        T2[("Associated Gold Fact")]
        
        S1 -->|cfg.source_dim_fact| T1
        S1 -->|cfg.source_dim_fact| T2
        
        T1_Status{"Dim SUCCESS?"}
        T2_Status{"Fact SUCCESS?"}
        
        T1 --> T1_Status
        T2 --> T2_Status
        
        T1_Status -- Yes --> Both_Passed
        T2_Status -- Yes --> Both_Passed
        
        T1_Status -- No --> Set_Fail["Source Status =<br/>FAILED"]
        T2_Status -- No --> Set_Fail
        
        Both_Passed{"Both Succeeded?"} -- Yes --> Set_Success["Source Status =<br/>SUCCESS"]
        Both_Passed -- No --> Set_Fail
    end
```

The mapping is dynamically evaluated using the mapping metadata table `cfg.source_dim_fact` for the **9 active sources**:

| Source ID | Source Table Name | Associated Dimensions | Associated Fact Tables | Success Rule |
| :---: | :--- | :--- | :--- | :--- |
| **1** | `customers` | `dim_customer` | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Success if `dim_customer` and all 5 facts succeeded. |
| **2** | `agents` | `dim_agent` | `fact_quotation`, `fact_quotation_item`, `fact_policy` | Success if `dim_agent` and all 3 facts succeeded. |
| **3** | `insurance_providers` | `dim_provider` | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Success if `dim_provider` and all 5 facts succeeded. |
| **4** | `vehicle` | `dim_vehicle` | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Success if `dim_vehicle` and all 5 facts succeeded. |
| **5** | `quotation` | `dim_package`, `dim_quotation`, `dim_quotation_status` | `fact_quotation`, `fact_quotation_item`, `fact_policy` | Success if all 3 dimensions and all 3 facts succeeded. |
| **6** | `quotation_item` | `dim_coverage` | `fact_quotation_item` | Success if `dim_coverage` and `fact_quotation_item` succeeded. |
| **7** | `policy` | `dim_policy`, `dim_policy_status` | `fact_policy`, `fact_payment`, `fact_cancellation` | Success if both dimensions and all 3 facts succeeded. |
| **8** | `cancellation` | `dim_cancellation_reason` | `fact_cancellation` | Success if `dim_cancellation_reason` and `fact_cancellation` succeeded. |
| **9** | `payment` | `dim_payment_status`, `dim_payment_method` | `fact_payment` | Success if both dimensions and `fact_payment` succeeded. |

---

## 3. Gold Layer Audit Session Lookup Strategy

Unlike the Bronze and Silver stages, which initialize fresh table sessions in `log.audit_table_session`, the Gold Layer dimensions and facts run in parallel and map back to their respective upstream sources to maintain lineage.

### 3.1. Session ID Resolution (Lineage Connection)
When a Gold ingestion notebook starts, it invokes `start_table_layer`. The helper `nb_gold_audit_helper_dev` performs the following steps:
1. **Dynamic Mapping**: Resolves the conformed table ID to its upstream source table ID using the config mapping `cfg.source_dim_fact` and target schema heuristics.
2. **Session ID Retrieval**: Queries `log.audit_table_session` to locate the active table session ID created during Bronze/Silver ingestion for the current `batch_id` and resolved `source_table_id`.
3. **Lineage Preservation**: Returns the retrieved session ID so that all Gold stage audits are written as detail records in `log.audit_detail` under the original source session.
4. **Fallback Handling**: If no active session is found (e.g., during manual standalone notebook runs), it generates a temporary dummy UUID to bypass logging and prevent failures.

### 3.2. Detailed Audit Logging
Upon completion, the Gold notebook calls `finish_table_layer` to append audit statistics (`inserted_row`, `updated_row`, `source_row_count`, `target_row_count`) under the `"GOLD"` layer for the resolved session.

---

## 4. Post-Ingestion Validation Suite

Executed by the dedicated pipeline activity `nb_validation_gold` (running `nb_gold_validate_reconciliation_dev`) upon successful completion of the master load notebook, this suite enforces data quality and metrics consistency:

1.  **Grain Uniqueness Check**:
    - Verifies that target primary keys are unique.
    - Example: Validates that `quotation_id` in `gold.fact_quotation` has 0 duplicate rows.
2.  **Date Validity Check**:
    - Ensures all date foreign keys map to a valid date key in `gold.dim_date` (or `-1` fallback).
    - Checks that date keys are not null or out of calendar ranges.
3.  **Referential Integrity Check**:
    - Verifies that dimension surrogate keys (e.g. `customer_key`, `agent_key`) map to existing rows in target dimensions.
    - Checks for unmatched keys that incorrectly resolved to `-1` but should have matched.
4.  **Reconciliation Verification**:
    - Compares row counts and measure totals between Silver source tables and Gold target tables.
    - Compares sum of premium amounts, payment amounts, and refund amounts (handling soft deletes).
5.  **Anomaly Logging & Auditing**:
    - If validation checks fail, the notebook calls `finish_table_layer` to record the `"FAILED"` status in `log.audit_detail` under the `"GOLD"` layer for the affected fact table session.
    - It writes the specific record key and validation error details to `log.invalid_record` via `log_invalid_record`.
    - It returns a JSON status map of all checks. Since this notebook runs as a dedicated activity downstream, a validation failure fails the pipeline run for visibility without triggering automatic `RECOVERY` run mode updates (which have already been reset to `NEW` by the completed orchestrator).
