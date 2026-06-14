# 05 - Fact Quotation Ingestion Strategy (`nb_gold_load_fact_quotation_dev`)

This document defines the ingestion design and dimension key lookup specifications for the `gold.fact_quotation` table. Context for columns and rules is aligned with the project specification in [silver-to-gold-mapping.md](../source-to-target-mapping/silver-to-gold-mapping.md) and [fact_quotation.json](../source-to-target-mapping/jsons/silver-to-gold/fact_quotation.json).

---

## 1. Objectives

*   Ingest quotation transaction records from `silver.quotation` into `gold.fact_quotation`.
*   Resolve conformed dimension keys using direct joins (SCD Type 1) and Point-in-Time joins (SCD Type 2).
*   Enforce default key fallbacks (`-1`) for unresolved or null relationships.
*   Support soft deletes to preserve financial transaction history.

---

## 2. Ingestion Logic Flow

```mermaid
graph TD
    Start([Start Ingestion]) --> ReadSilver[Read silver.quotation by batch_id]
    ReadSilver --> JoinSCD1[Join SCD1 Dimensions: dim_quotation, dim_package, dim_quotation_status]
    JoinSCD1 --> JoinSCD2[Join SCD2 Dimensions: dim_customer, dim_agent, dim_provider, dim_vehicle]
    JoinSCD2 --> PIT_Join{PIT Join condition met?}
    
    PIT_Join -->|Yes| ResolveKeys[Assign Target Surrogate Keys]
    PIT_Join -->|No / Null| AssignDefault[Assign -1 Surrogate Key]
    
    ResolveKeys & AssignDefault --> FormatDates[Convert Date Columns to YYYYMMDD Date Keys]
    FormatDates --> MergeFact{Delta Merge on quotation_id}
    
    MergeFact -->|Key Match & Diff Detected| UpdateFact[Update Record In-Place]
    MergeFact -->|Key Match & No Diff| SkipWrite[Bypass Write / Do Nothing]
    MergeFact -->|No Key Match| InsertFact[Insert New Record]
    
    UpdateFact & SkipWrite & InsertFact --> End([Fact Completed])
```

---

## 3. Dimension Key Lookups Schema

The target columns for `gold.fact_quotation` are resolved from the source fields using the following lookup joins:

| Target Key Column | Source Field | Target Dimension Table | Join Type & Lookup Logic |
| :--- | :--- | :--- | :--- |
| `quotation_key` | `quotation_id` | `dim_quotation` (SCD1) | Join `silver.quotation.quotation_id` = `dim_quotation.quotation_id`. Null fallback = `-1`. |
| `quotation_date_key` | `quotation_at` | `dim_date` | `CAST(DATE_FORMAT(quotation_at, 'yyyyMMdd') AS INT)`. Null fallback = `-1`. |
| `quotation_expiry_date_key` | `quotation_expiry_at` | `dim_date` | `CAST(DATE_FORMAT(quotation_expiry_at, 'yyyyMMdd') AS INT)`. Null fallback = `-1`. |
| `customer_key` | `customer_id` | `dim_customer` (SCD2) | PIT Join: Match `customer_id` AND `silver.quotation.quotation_at` BETWEEN `dim_customer.effective_from` AND `dim_customer.effective_to`. Null fallback = `-1`. |
| `agent_key` | `agent_id` | `dim_agent` (SCD2) | PIT Join: Match `agent_id` AND `silver.quotation.quotation_at` BETWEEN `dim_agent.effective_from` AND `dim_agent.effective_to`. Null fallback = `-1`. |
| `provider_key` | `provider_code` | `dim_provider` (SCD2) | PIT Join: Match `provider_code` AND `silver.quotation.quotation_at` BETWEEN `dim_provider.effective_from` AND `dim_provider.effective_to`. Null fallback = `-1`. |
| `package_key` | `package_code` | `dim_package` (SCD1) | Join `silver.quotation.package_code` = `dim_package.package_code`. Null fallback = `-1`. |
| `quotation_status_key` | `quotation_status` | `dim_quotation_status` (SCD1) | Join `silver.quotation.quotation_status` = `dim_quotation_status.quotation_status_code`. Null fallback = `-1`. |
| `vehicle_key` | `customer_id` | `dim_vehicle` (SCD2) | Join `silver.quotation.customer_id` = `silver.vehicle.customer_id` (1-to-1 assumption), then lookup `dim_vehicle` by `vehicle_id` where `silver.quotation.quotation_at` BETWEEN `dim_vehicle.effective_from` AND `dim_vehicle.effective_to`. Null fallback = `-1`. |

### Measures and Degenerate Dimensions:
*   `premium_amount`: `COALESCE(premium_amount, 0)`
*   `quotation_id` (STRING), `customer_id` (STRING), `agent_id` (STRING), `provider_code` (STRING): Mapped directly as degenerate dimensions.
*   `converted_flag` (BOOLEAN): Set to `true` if `quotation_id` exists in `silver.policy`, else `false`.

---

## 4. Ingestion Query & Soft Delete Rules

During fact ingestion, if a source quotation record is flagged as deleted (`is_deleted = true`), it is not purged from the Gold Layer. Instead, metadata columns in the target table are populated to flag the soft delete:
*   `is_deleted = true`
*   `deleted_at = current_timestamp()`
*   `delete_batch_id = <current_batch_id>`
*   Measure columns (such as `premium_amount`) are set to `0.00` to prevent skewing active summary calculations in reporting layers.

### Ingestion SQL Pattern
```sql
MERGE INTO gold.fact_quotation AS target
USING (
    SELECT 
        q.quotation_id,
        q.quotation_at,
        q.quotation_expiry_at,
        q.customer_id,
        q.agent_id,
        q.provider_code,
        q.package_code,
        q.quotation_status,
        q.premium_amount,
        q._source_system,
        q._batch_id,
        CASE WHEN p.quotation_id IS NOT NULL THEN true ELSE false END AS converted_flag
    FROM silver.quotation q
    LEFT JOIN silver.policy p ON q.quotation_id = p.quotation_id
) AS source
ON target.quotation_id = source.quotation_id
WHEN MATCHED AND (
    target.quotation_date_key <> COALESCE(CAST(DATE_FORMAT(source.quotation_at, 'yyyyMMdd') AS INT), -1) OR
    target.quotation_expiry_date_key <> COALESCE(CAST(DATE_FORMAT(source.quotation_expiry_at, 'yyyyMMdd') AS INT), -1) OR
    target.customer_key <> COALESCE((SELECT customer_key FROM gold.dim_customer WHERE customer_id = source.customer_id AND source.quotation_at BETWEEN effective_from AND effective_to), -1) OR
    target.agent_key <> COALESCE((SELECT agent_key FROM gold.dim_agent WHERE agent_id = source.agent_id AND source.quotation_at BETWEEN effective_from AND effective_to), -1) OR
    target.provider_key <> COALESCE((SELECT provider_key FROM gold.dim_provider WHERE provider_code = source.provider_code AND source.quotation_at BETWEEN effective_from AND effective_to), -1) OR
    target.package_key <> COALESCE((SELECT package_key FROM gold.dim_package WHERE package_code = source.package_code), -1) OR
    target.quotation_status_key <> COALESCE((SELECT quotation_status_key FROM gold.dim_quotation_status WHERE quotation_status_code = source.quotation_status), -1) OR
    target.vehicle_key <> COALESCE((
        SELECT v.vehicle_key 
        FROM silver.vehicle sv
        INNER JOIN gold.dim_vehicle v ON sv.vehicle_id = v.vehicle_id
        WHERE sv.customer_id = source.customer_id 
          AND source.quotation_at BETWEEN v.effective_from AND v.effective_to
        LIMIT 1
    ), -1) OR
    target.premium_amount <> COALESCE(source.premium_amount, 0) OR
    target.converted_flag <> source.converted_flag
) THEN
    UPDATE SET 
        target.quotation_key = COALESCE((SELECT quotation_key FROM gold.dim_quotation WHERE quotation_id = source.quotation_id), -1),
        target.quotation_date_key = COALESCE(CAST(DATE_FORMAT(source.quotation_at, 'yyyyMMdd') AS INT), -1),
        target.quotation_expiry_date_key = COALESCE(CAST(DATE_FORMAT(source.quotation_expiry_at, 'yyyyMMdd') AS INT), -1),
        target.customer_key = COALESCE((SELECT customer_key FROM gold.dim_customer WHERE customer_id = source.customer_id AND source.quotation_at BETWEEN effective_from AND effective_to), -1),
        target.agent_key = COALESCE((SELECT agent_key FROM gold.dim_agent WHERE agent_id = source.agent_id AND source.quotation_at BETWEEN effective_from AND effective_to), -1),
        target.provider_key = COALESCE((SELECT provider_key FROM gold.dim_provider WHERE provider_code = source.provider_code AND source.quotation_at BETWEEN effective_from AND effective_to), -1),
        target.package_key = COALESCE((SELECT package_key FROM gold.dim_package WHERE package_code = source.package_code), -1),
        target.quotation_status_key = COALESCE((SELECT quotation_status_key FROM gold.dim_quotation_status WHERE quotation_status_code = source.quotation_status), -1),
        target.vehicle_key = COALESCE((
            SELECT v.vehicle_key 
            FROM silver.vehicle sv
            INNER JOIN gold.dim_vehicle v ON sv.vehicle_id = v.vehicle_id
            WHERE sv.customer_id = source.customer_id 
              AND source.quotation_at BETWEEN v.effective_from AND v.effective_to
            LIMIT 1
        ), -1),
        target.premium_amount = COALESCE(source.premium_amount, 0),
        target.converted_flag = source.converted_flag,
        target.updated_at = current_timestamp()
---

## 5. Idempotency & Rerun Behavior (No-Change Scenario)

*   **No Changes**: If the quotation records in the incoming Silver batch are identical to the target (meaning conformed keys, measures, and flags are unchanged), the Delta MERGE condition evaluates to `false`. **No update operations are performed, and no data is written to disk**, saving execution time and resources.
*   **Batch Reruns**: If a batch is re-run (e.g. during a recovery execution), the MERGE statement matches on `quotation_id` and only updates records where values have actually changed. No duplicate entries are created.

