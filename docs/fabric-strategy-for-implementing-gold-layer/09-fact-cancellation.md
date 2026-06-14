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

## 4. Ingestion Strategy & PySpark Implementation Details

To build the fact table conformed rows, the notebook performs the following:

1. **Read Incoming Batch**: Filters `silver.cancellation` by the current `batch_id`.
2. **Resolve Parent Context**: Left joins with `silver.policy` on `policy_id` to retrieve parent fields: `customer_id`, `provider_code`.
3. **Resolve Vehicle ID**: Left joins with `silver.vehicle` (deduplicated by `customer_id`) on `customer_id` (from parent context) to obtain `vehicle_id` (using the 1-to-1 assumption).
4. **Resolve Dimension Keys**: Left joins conformed dimensions:
   - For **SCD1** dimensions (`dim_policy`, `dim_cancellation_reason`), joins are on business keys.
   - For **SCD2** dimensions (`dim_customer`, `dim_provider`, `dim_vehicle`), point-in-time joins check that the cancellation transaction date `cancellation_at` falls between `effective_from` and `effective_to`.
   - Surrogate keys fallback to `-1` on null.
5. **Convert Date Keys**: Formats dates (`cancellation_at`) into `YYYYMMDD` integer keys (`cancellation_date_key`), falling back to `-1` on null.

### Implementation PySpark Merge Pattern
The conformed cancellation records are merged on a matching condition on `cancellation_id`:

```python
delta_table = DeltaTable.forName(spark, "gold.fact_cancellation")

delta_table.alias("target").merge(
    final_df.alias("source"),
    "target.cancellation_id = source.cancellation_id"
).whenMatchedUpdate(
    set={
        "policy_id": "source.policy_id",
        "policy_key": "source.policy_key",
        "cancellation_reason_key": "source.cancellation_reason_key",
        "cancellation_date_key": "source.cancellation_date_key",
        "customer_key": "source.customer_key",
        "provider_key": "source.provider_key",
        "vehicle_key": "source.vehicle_key",
        "refund_amount": "source.refund_amount",
        "updated_at": "current_timestamp()",
        "_batch_id": "source._batch_id",
        "_source_system": "source._source_system",
        "pipeline_run_id": "source.pipeline_run_id",
        "is_deleted": "source.is_deleted",
        "deleted_at": "source.deleted_at",
        "delete_batch_id": "source.delete_batch_id"
    }
).whenNotMatchedInsertAll().execute()
```

---

## 5. Idempotency & Rerun Behavior (No-Change Scenario)

*   **Idempotent Updates**: In recovery runs or batch reruns, the MERGE statement matches on `cancellation_id`. Existing rows are updated with conformed dimension surrogate keys matching the active batch state. No duplicate records are inserted, ensuring that the fact grain remains strictly unique.
*   **Active Audits**: All metrics and counts are validated post-ingestion by `nb_gold_validate_reconciliation_dev` to ensure data completeness and consistency before finishing the run.


