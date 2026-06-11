# Gold Layer Design Rationale (CarPro Insurance Analytics)

This document provides a technical overview of the design decisions, architectural considerations, and execution mechanisms implemented for the **Gold Layer Ingestion & Validation** in Microsoft Fabric. Use this document as a guide to understand and present the architecture to your team and stakeholders.

---

## 1. Architectural Overview & Dimensional Modeling (Star Schema)

The Gold Layer is the final reporting-ready dimensional layer (Star Schema) optimized for Power BI reporting, self-service analytics, and semantic modeling. It is organized as a collection of Fact tables surrounded by Dimension tables:

*   **Fact Tables:** Store numerical measurements (measures) and foreign keys pointing to conformed dimensions.
    *   *5 Implemented Fact tables:* `fact_policy`, `fact_quotation`, `fact_quotation_item`, `fact_payment`, `fact_cancellation`.
*   **Dimension Tables:** Store descriptive attributes about business entities.
    *   *Static Dimension:* `dim_date` (pre-generated calendar from 2020 to 2030).
    *   *SCD Type 1 Dimensions:* `dim_package`, `dim_coverage`, `dim_quotation`, `dim_policy`, and status/method tables (e.g. `dim_policy_status`, `dim_payment_status`).
    *   *SCD Type 2 Dimensions:* `dim_customer`, `dim_agent`, `dim_provider`, `dim_vehicle`.
*   **Surrogate Keys:** All lookups in the Fact tables reference surrogate keys (system-generated `BIGINT` IDs) rather than string-based business keys (e.g. `customer_key` instead of `customer_id`). This optimizes join performance and supports version history tracking (SCD Type 2).
*   **Unknown Members (-1):** To prevent losing fact records during joins when lookups fail or are blank, an Unknown member row with surrogate key = `-1` is idempotently inserted into every dimension table. Any unresolved lookup will point to this key.

---

## 2. Ingestion Strategies for Dimensions (SCD Type 1 vs SCD Type 2)

### A. SCD Type 1 Ingestion
*   **Concept:** Updates descriptive attributes in-place when changes occur at the source, without preserving historical versions.
*   **Mechanism:** 
    *   Utilizes a generic function `load_scd1_dimension()` driven by **Delta Merge**.
    *   Compares hashes of incoming source data against the target. If values mismatch, it updates the existing row (`whenMatchedUpdate`); otherwise, it inserts a new record (`whenNotMatchedInsert`).

### B. SCD Type 2 Ingestion
*   **Concept:** Tracks history by creating a new version of the row with updated attributes when a change occurs in tracked fields (e.g. a customer changes their `city` or `district`).
*   **Mechanism (Hybrid SCD1/SCD2):**
    *   Computes an MD5 hash of business keys and tracked attributes (`tracked_cols`).
    *   **INSERT (SCD2):** Generates a new surrogate key, sets `is_current = true`, `effective_from = source.updated_at`, and `effective_to = '9999-12-31 23:59:59'`.
    *   **EXPIRE (SCD2):** Identifies active target rows that have changed, sets their `is_current = false` and updates their `effective_to` to the new version's `effective_from`.
    *   **UPDATE IN-PLACE (SCD1):** If changes occur in untracked attributes (`type1_cols` like email or phone number), it updates the current active row in-place without generating a new row.

---

## 3. Fact Ingestion & SCD Type 2 Point-in-Time Key Lookups

When loading facts, resolving foreign keys to SCD Type 2 dimensions requires looking up the correct active surrogate key at the moment the transaction occurred.

*   **Point-in-Time Join Logic:** Instead of joining on the business key alone, lookups filter by the event timestamp:
    ```sql
    fact_event_timestamp BETWEEN dim.effective_from AND dim.effective_to
    ```
*   **Example:** If customer A purchases a policy on `2026-01-10` while residing in Da Nang (Version 1 of A), and later moves to TP.HCM on `2026-03-01` (generating Version 2 of A), the fact record for `2026-01-10` resolves to the surrogate key of Version 1 (Da Nang).
*   **Soft Delete Ingestion:** Fact records marked as deleted at the source (i.e. `is_deleted = true` or CDC `operation_type = 'D'`) are not physically deleted from the Gold layer. Instead, they are updated as soft-deleted (`is_deleted = true`, `deleted_at = current_timestamp()`, `delete_batch_id = current_batch_id()`) to preserve complete history for financial audit checks.

---

## 4. Generic Data Quality Validation Suite

After napping each fact table, the validation notebook `nb_gold_fact_validate_dev` automatically runs a suite of 6 generic QA checks:

1.  **Row Count Reconciliation:** Compares target fact table count against deduped, non-empty source records to ensure no rows were dropped during ingestion.
2.  **Grain Uniqueness:** Ensures no duplicate business grains exist (e.g. no duplicate `policy_id` values in `fact_policy`).
3.  **Date Key Validity:** Verifies all date key columns (e.g. `20260611`) resolve and exist within `dim_date.date_key`.
4.  **Foreign Key Integrity:** Asserts all resolved surrogate keys exist in their respective dimensions or equal `-1`.
5.  **Metric Reconciliation:** Sums financial metrics (e.g. `premium_amount`, `payment_amount`) between Silver and Gold layers and ensures they reconcile within a `0.01` precision threshold.
6.  **Soft Delete Auditing:** Verifies that rows marked as soft-deleted correctly populate technical metadata columns (`deleted_at`, `delete_batch_id`).

---

## 5. Orchestration Flow & Preflight Guards (`nb_gold_orchestrator_dev`)

The orchestration notebook manages the safe execution sequence of the Gold Layer ingestion:

```text
[Start]
   │
   ▼
[Preflight Guards Check] (Checks schemas for Silver and Gold availability)
   │
   ▼
[nb_cfg_etl_control_setup_dev] (Loads configuration metadata)
   │
   ▼
[nb_audit_pipeline_log_dev] (Initializes audit monitoring session)
   │
   ▼
[nb_gold_static_dimension_setup_dev] (Creates calendar & insert -1 Unknown rows)
   │
   ▼
[nb_gold_dim_scd1_load_dev.py] (Ingests SCD Type 1 dimensions)
   │
   ▼
[nb_gold_dim_scd2_load_dev.py] (Ingests SCD Type 2 dimensions)
   │
   ▼
[nb_gold_driver_flow_dev] ──► (Runs ingestion & validation sequentially for all 5 Facts)
   │
   ▼
[End & Compile JSON Summary]
```

### Rationale Behind `REQUIRED_GOLD_FOR_FACT_POLICY` in Preflight Checks
In the preflight section of the orchestrator, you will find this array of tables:
```python
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
```
**Why do we check this specific list?**
1.  **Proxy DDL Check:** All 19 Gold Layer tables are created together in a single batch DDL script via `nb_gold_create_tables_dev`. 
2.  **High Dependency Coverage:** The `fact_policy` table is the most complex fact in the schema, referencing nearly all major dimensions. 
3.  **Efficiency:** Rather than writing slow check queries for all 19 tables, verifying the existence of `fact_policy` and its dependencies serves as a reliable proxy. If these tables are present, it guarantees that the Gold DDL script ran successfully and the schema structure is complete.
