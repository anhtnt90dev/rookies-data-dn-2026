# 10 - Gold Layer Validation and Reconciliation Strategy

This document defines the post-ingestion validation and data quality audit suite executed in the dedicated notebook **`nb_gold_validate_reconciliation_dev`** for target tables in the Gold Layer.

---

## 1. Objectives

*   Enforce strict data quality standards on all conformed dimensions and facts before publishing.
*   Log validation anomalies to `log.invalid_record` for administrative review.
*   Fail the table execution session immediately if critical validation constraints are violated, triggering recovery flow.

---

## 2. Ingestion Validation Logic Flow

```mermaid
graph TD
    Start([Start Validation Suite]) --> GrainCheck[1. Check Grain Uniqueness]
    GrainCheck -->|Pass| FK_Check[2. Verify Foreign Key Integrity]
    GrainCheck -->|Fail| LogError[Log to log.invalid_record & Abort]
    
    FK_Check -->|Pass| DateCheck[3. Verify Date Key Validity]
    FK_Check -->|Fail| LogError
    
    DateCheck -->|Pass| RowReconciliation[4. Reconcile Row Counts Slv vs Gld]
    DateCheck -->|Fail| LogError
    
    RowReconciliation -->|Pass| MetricReconciliation[5. Reconcile Numeric Sums Slv vs Gld]
    RowReconciliation -->|Fail| LogError
    
    MetricReconciliation -->|Pass| SoftDeleteCheck[6. Validate Soft Delete Metadata]
    MetricReconciliation -->|Fail| LogError
    
    SoftDeleteCheck -->|Pass| EndSUCCESS([Validation Passed - SUCCESS])
    SoftDeleteCheck -->|Fail| LogError
    
    LogError --> EndFAILED([Validation Failed - FAILED])
```

---

## 3. The 6 Automated Data Quality Checks

Every fact table undergoes a suite of 6 validation tests:

### 1. Grain Uniqueness Check
*   **Purpose**: Validates that no logical duplicate keys exist at the grain of the fact table.
*   **Logic**:
    ```sql
    SELECT grain_column, COUNT(*)
    FROM gold.fact_table
    GROUP BY grain_column
    HAVING COUNT(*) > 1;
    ```
*   **Failure Rule**: Fails if any rows are returned.

### 2. Foreign Key Integrity Check
*   **Purpose**: Ensures that all resolved surrogate keys in the fact table exist in their mapped conformed dimensions (no orphaned links).
*   **Logic**: Verifies that `target_key_column = -1` OR exists in `target_dimension.surrogate_key`.
*   **Failure Rule**: Fails if any unresolved foreign key is found.

### 3. Date Key Validity Check
*   **Purpose**: Assures all date keys in the fact table map to a valid record in the calendar dimension.
*   **Logic**:
    ```sql
    SELECT f.date_key
    FROM gold.fact_table f
    LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
    WHERE d.date_key IS NULL;
    ```
*   **Failure Rule**: Fails if any date keys do not resolve.

### 4. Row Count Reconciliation
*   **Purpose**: Assures no rows were dropped during ingestion.
*   **Logic**: Compares the total row count of the Gold fact table against the upstream active (non-purged) deduplicated Silver table records for the active batch.
*   **Failure Rule**: Fails if the count difference is non-zero.

### 5. Metric Reconciliation
*   **Purpose**: Confirms that financial metrics remain precise and unaltered between layers.
*   **Logic**:
    ```sql
    SELECT ABS(SUM(s.metric_value) - SUM(g.metric_value)) AS variance
    FROM silver.table s
    FULL OUTER JOIN gold.fact_table g ON s.business_key = g.business_key
    WHERE s.batch_id = current_batch_id;
    ```
*   **Failure Rule**: Fails if `variance > 0.01` (to account for floating-point calculation differences).

### 6. Soft Delete Auditing Check
*   **Purpose**: Verifies that deleted records have their technical delete metadata populated correctly.
*   **Logic**: Confirms that rows with `is_deleted = true` have non-null `deleted_at` timestamps and valid `delete_batch_id` values.
*   **Failure Rule**: Fails if any soft-deleted records lack audit timestamps.

---

## 4. Error Logging Mechanics

When a check fails, the notebook:
1.  Inserts a summary record into `log.invalid_record` describing the failed rule, target table, offending key, and reason.
2.  Calls `finish_table_layer` with `status = 'FAILED'`.
3.  Throws a runtime exception to stop the execution workflow.
