# 09 - Fact Cancellation Ingestion Strategy (`nb_gold_load_fact_cancellation_dev`)

This document defines the ingestion specifications, conformed key lookups, and metadata tracking rules for the `gold.fact_cancellation` table. Context for columns and rules is aligned with the project specification in [silver-to-gold-mapping.md](../source-to-target-mapping/silver-to-gold-mapping.md) and [fact_cancellation.json](../source-to-target-mapping/jsons/silver-to-gold/fact_cancellation.json).

---

## 1. Objectives

*   Ingest policy cancellations from `silver.cancellation` into `gold.fact_cancellation`.
*   Link cancellations to conformed dimensions (`dim_policy`, `dim_cancellation_reason`).
*   Resolve SCD Type 2 dimension keys (`dim_customer`, `dim_provider`, `dim_vehicle`) from parent policy context.
*   Track cancellation metrics (refunded premium amounts).

---

## 2. Ingestion Logic Flow

```mermaid
graph TD
    Start([Start Ingestion]) --> ReadCancellation[Read silver.cancellation]
    ReadCancellation --> JoinPolicy[Join silver.policy on policy_id]
    JoinPolicy --> JoinSCD1[Join SCD1 Dimensions: dim_policy, dim_cancellation_reason]
    JoinSCD1 --> JoinSCD2[Join SCD2 Dimensions: dim_customer, dim_provider, dim_vehicle]
    JoinSCD2 --> PIT_Join{PIT Join condition met?}
    
    PIT_Join -->|Yes| ResolveKeys[Assign Target Surrogate Keys]
    PIT_Join -->|No / Null| AssignDefault[Assign -1 Surrogate Key]
    
    ResolveKeys & AssignDefault --> FormatDates[Resolve Date Key: cancellation_date_key]
    FormatDates --> MergeFact{Delta Merge on cancellation_id}
    
    MergeFact -->|Key Match & Diff Detected| UpdateFact[Update Record In-Place]
    MergeFact -->|Key Match & No Diff| SkipWrite[Bypass Write / Do Nothing]
    MergeFact -->|No Key Match| InsertFact[Insert New Record]
    
    UpdateFact & SkipWrite & InsertFact --> End([Fact Completed])
```

---

## 3. Dimension Key Lookups Schema

Surrogate keys for `gold.fact_cancellation` are resolved using the following specifications:

| Target Key Column | Source Field | Target Dimension Table | Join Type & Lookup Logic |
| :--- | :--- | :--- | :--- |
| `policy_key` | `policy_id` | `dim_policy` (SCD1) | Join `silver.cancellation.policy_id` = `dim_policy.policy_id`. Null fallback = `-1`. |
| `cancellation_date_key` | `cancellation_at` | `dim_date` | `CAST(DATE_FORMAT(cancellation_at, 'yyyyMMdd') AS INT)`. Null fallback = `-1`. |
| `customer_key` | `customer_id` (Parent) | `dim_customer` (SCD2) | PIT Join: Join `silver.policy` to get `customer_id`, where `silver.cancellation.cancellation_at` BETWEEN `dim_customer.effective_from` AND `dim_customer.effective_to`. Null fallback = `-1`. |
| `provider_key` | `provider_code` (Parent) | `dim_provider` (SCD2) | PIT Join: Join `silver.policy` to get `provider_code`, where `silver.cancellation.cancellation_at` BETWEEN `dim_provider.effective_from` AND `dim_provider.effective_to`. Null fallback = `-1`. |
| `cancellation_reason_key` | `cancellation_reason` | `dim_cancellation_reason` (SCD1) | Join `silver.cancellation.cancellation_reason` = `dim_cancellation_reason.cancellation_reason`. Null fallback = `-1`. |
| `vehicle_key` | `customer_id` (Parent) | `dim_vehicle` (SCD2) | PIT Join: Join `silver.policy` to get `customer_id` and find active `vehicle_id` from `silver.vehicle` where `silver.cancellation.cancellation_at` BETWEEN `dim_vehicle.effective_from` AND `dim_vehicle.effective_to`. Null fallback = `-1`. |

### Measures and Degenerate Dimensions:
*   `refund_amount`: `COALESCE(refund_amount, 0)`
*   `cancellation_id` (STRING), `policy_id` (STRING): Mapped directly as degenerate dimensions.

---

## 4. Soft Delete and Ingestion Rules

*   **Soft Deletes**: If a cancellation is soft-deleted in `silver.cancellation` (`is_deleted = true`), the target row is updated:
    *   `is_deleted = true`
    *   `deleted_at = current_timestamp()`
    *   `delete_batch_id = <current_batch_id>`
    *   `refund_amount = 0.00`

---

## 5. Idempotency & Rerun Behavior (No-Change Scenario)

*   **No Changes**: If cancellation records in the incoming Silver batch are identical to existing target records, the MERGE condition evaluates to `false`. **No update operations are performed, and no data is written to disk**, saving processing time and I/O.
*   **Batch Reruns**: In recovery runs, the MERGE statement matches on `cancellation_id` and only updates records with actual column changes, preventing duplicated cancellation rows.


