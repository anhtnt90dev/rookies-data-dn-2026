# 06 - Fact Quotation Item Ingestion Strategy (`nb_gold_load_fact_quotation_item_dev`)

This document defines the ingestion design and key lookup rules for `gold.fact_quotation_item`, which tracks line-item detail values for insurance quotations. Context for columns and rules is aligned with the project specification in [silver-to-gold-mapping.md](../source-to-target-mapping/silver-to-gold-mapping.md) and [fact_quotation_item.json](../source-to-target-mapping/jsons/silver-to-gold/fact_quotation_item.json).

---

## 1. Objectives

*   Ingest quotation line items from `silver.quotation_item` into `gold.fact_quotation_item`.
*   Establish lookup lineage for coverages (`dim_coverage`) and parent quotation attributes.
*   Resolve SCD Type 2 lookups by inheriting transaction context from the parent quotation record.

---

## 2. Ingestion Logic Flow

To resolve dimension keys for quotation items, the source data must first be enriched with the parent quotation's business keys and creation timestamp before joining dimension tables.

```mermaid
graph TD
    Start([Start Ingestion]) --> ReadItems[Read silver.quotation_item]
    ReadItems --> JoinParent[Join silver.quotation on quotation_id]
    JoinParent --> JoinCoverage[Join dim_coverage on coverage_type]
    JoinCoverage --> JoinSCD2[Join SCD2 Dimensions: dim_customer, dim_agent, dim_provider, dim_vehicle]
    JoinSCD2 --> PIT_Join{PIT Join condition met?}
    
    PIT_Join -->|Yes| ResolveKeys[Assign Target Surrogate Keys]
    PIT_Join -->|No / Null| AssignDefault[Assign -1 Surrogate Key]
    
    ResolveKeys & AssignDefault --> FormatDates[Convert Date Columns to YYYYMMDD Date Keys]
    FormatDates --> MergeFact{Delta Merge on quotation_id & coverage_type}
    
    MergeFact -->|Key Match & Diff Detected| UpdateFact[Update Record In-Place]
    MergeFact -->|Key Match & No Diff| SkipWrite[Bypass Write / Do Nothing]
    MergeFact -->|No Key Match| InsertFact[Insert New Record]
    
    UpdateFact & SkipWrite & InsertFact --> End([Fact Completed])
```

---

## 3. Dimension Key Lookups Schema

The conformed keys for `gold.fact_quotation_item` are resolved as follows:

| Target Key Column | Source Field | Target Dimension Table | Join Type & Lookup Logic |
| :--- | :--- | :--- | :--- |
| `quotation_key` | `quotation_id` | `dim_quotation` (SCD1) | Join `silver.quotation_item.quotation_id` = `dim_quotation.quotation_id`. Null fallback = `-1`. |
| `quotation_date_key` | `quotation_at` (Parent) | `dim_date` | `CAST(DATE_FORMAT(quotation.quotation_at, 'yyyyMMdd') AS INT)`. Null fallback = `-1`. |
| `customer_key` | `customer_id` (Parent) | `dim_customer` (SCD2) | PIT Join: Join `silver.quotation` to get `customer_id`, where `silver.quotation.quotation_at` BETWEEN `dim_customer.effective_from` AND `dim_customer.effective_to`. Null fallback = `-1`. |
| `agent_key` | `agent_id` (Parent) | `dim_agent` (SCD2) | PIT Join: Join `silver.quotation` to get `agent_id`, where `silver.quotation.quotation_at` BETWEEN `dim_agent.effective_from` AND `dim_agent.effective_to`. Null fallback = `-1`. |
| `provider_key` | `provider_code` (Parent) | `dim_provider` (SCD2) | PIT Join: Join `silver.quotation` to get `provider_code`, where `silver.quotation.quotation_at` BETWEEN `dim_provider.effective_from` AND `dim_provider.effective_to`. Null fallback = `-1`. |
| `package_key` | `package_code` (Parent) | `dim_package` (SCD1) | Join `silver.quotation.package_code` = `dim_package.package_code`. Null fallback = `-1`. |
| `coverage_key` | `coverage_type` | `dim_coverage` (SCD1) | Join `silver.quotation_item.coverage_type` = `dim_coverage.coverage_type`. Null fallback = `-1`. |
| `quotation_status_key` | `quotation_status` (Parent) | `dim_quotation_status` (SCD1) | Join `silver.quotation.quotation_status` = `dim_quotation_status.quotation_status_code`. Null fallback = `-1`. |
| `vehicle_key` | `customer_id` (Parent) | `dim_vehicle` (SCD2) | Join `silver.quotation.customer_id` = `silver.vehicle.customer_id` (1-to-1 assumption), then lookup `dim_vehicle` by `vehicle_id` where `silver.quotation.quotation_at` BETWEEN `dim_vehicle.effective_from` AND `dim_vehicle.effective_to`. Null fallback = `-1`. |

### Measures and Degenerate Dimensions:
*   `coverage_amount`: `COALESCE(coverage_amount, 0)`
*   `deductible_amount`: `COALESCE(deductible_amount, 0)`
*   `quotation_item_id` (STRING), `quotation_id` (STRING): Mapped directly as degenerate dimensions.

---

## 4. Ingestion Strategy & PySpark Implementation Details

To build the fact table conformed rows, the notebook performs the following:

1. **Read Incoming Batch**: Filters `silver.quotation_item` by the current `batch_id`.
2. **Resolve Parent Context**: Inner joins with `silver.quotation` on `quotation_id` to retrieve parent fields: `customer_id`, `agent_id`, `provider_code`, `quotation_at`, `quotation_status`, `package_code`.
3. **Resolve Vehicle ID**: Left joins with `silver.vehicle` (deduplicated by `customer_id`) on `customer_id` (from parent context) to obtain `vehicle_id` (using the 1-to-1 assumption).
4. **Resolve Dimension Keys**: Left joins with conformed dimensions:
   - For **SCD1** dimensions (`dim_quotation`, `dim_package`, `dim_coverage`, `dim_quotation_status`), joins are on business keys.
   - For **SCD2** dimensions (`dim_customer`, `dim_agent`, `dim_provider`, `dim_vehicle`), point-in-time joins check that the parent `quotation_at` falls between `effective_from` and `effective_to`.
   - Surrogate keys fallback to `-1` on null.
5. **Convert Date Keys**: Formats the parent transaction date (`quotation_at`) to a `YYYYMMDD` integer key (`quotation_date_key`), falling back to `-1`.

### Implementation PySpark Merge Pattern
The conformed line-item records are merged on a composite match condition (`quotation_id` and `coverage_key`):

```python
delta_table = DeltaTable.forName(spark, "gold.fact_quotation_item")

delta_table.alias("target").merge(
    final_df.alias("source"),
    "target.quotation_id = source.quotation_id AND target.coverage_key = source.coverage_key"
).whenMatchedUpdate(
    set={
        "quotation_item_id": "source.quotation_item_id",
        "quotation_key": "source.quotation_key",
        "quotation_date_key": "source.quotation_date_key",
        "customer_key": "source.customer_key",
        "agent_key": "source.agent_key",
        "provider_key": "source.provider_key",
        "package_key": "source.package_key",
        "quotation_status_key": "source.quotation_status_key",
        "vehicle_key": "source.vehicle_key",
        "coverage_amount": "source.coverage_amount",
        "deductible_amount": "source.deductible_amount",
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

*   **Idempotent Updates**: In recovery runs or batch reruns, the MERGE statement matches on `(quotation_id, coverage_key)`. Existing rows are updated with conformed dimension surrogate keys matching the active batch state. No duplicate records are inserted, ensuring that the fact grain remains strictly unique.
*   **Active Audits**: All metrics and counts are validated post-ingestion by `nb_gold_validate_reconciliation_dev` to ensure data completeness and consistency before finishing the run.


