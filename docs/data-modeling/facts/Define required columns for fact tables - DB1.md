# Gold Layer Fact Tables — Field Design Document

### Insurance Analytics — Dimensional Model (Star Schema)

## 1. Purpose

This document defines the proposed Gold Layer Fact Table structures for the following two fact tables derived from the CRM source system (`insurance_crm_db`):

* `fact_quotation`
* `fact_quotation_item`

These definitions serve as the authoritative reference for downstream BI development, data validation, and pipeline engineering.

---

## 2. General Fact Table Design Standards

| Standard                  | Description                                                                                                                                         |
| :------------------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Grain**                 | Each fact table is defined at the most atomic level of its business event.                                                                          |
| **Surrogate Keys**        | All foreign keys reference dimension surrogate keys (`BIGINT`), not source natural keys.                                                            |
| **Date Keys**             | All date fields are replaced by integer date keys in `YYYYMMDD` format referencing `dim_date`.                                                      |
| **Measures**              | Only fully additive or semi-additive numeric values are stored as measures. All categorical context lives in dimensions.                            |
| **Degenerate Dimensions** | Identifiers that carry no descriptive attributes but are critical for traceability (`quotation_id`) are stored directly in the fact table.          |
| **No NULLs on Measures**  | Measure columns default to `0` rather than `NULL`; enforced at the Silver → Gold transform layer.                                                   |
| **Lineage & Soft Delete** | Comprehensive audit and execution tracking fields are present to isolate processing batches and exclude logically deleted records from BI measures. |

---

## 3. Common Technical Columns

All fact tables share the following standard Gold lineage, batch control, and audit columns.

| Column            | Type      | Description                                                            |
| :---------------- | :-------- | :--------------------------------------------------------------------- |
| `source_system`   | STRING    | Source system name (e.g., `insurance_crm_db`).                         |
| `batch_id`        | STRING    | Identifier of the data platform processing batch that loaded this row. |
| `pipeline_run_id` | STRING    | Specific execution ID of the pipeline run for lineage tracking.        |
| `is_deleted`      | TINYINT   | Indicator for soft deletes (`0` = Active, `1` = Deleted in source).    |
| `deleted_at`      | TIMESTAMP | Timestamp when the row was flagged as deleted.                         |
| `delete_batch_id` | STRING    | Processing batch identifier that executed the soft delete.             |
| `created_at`      | TIMESTAMP | Timestamp when the fact row was first loaded into Gold.                |
| `updated_at`      | TIMESTAMP | Timestamp when the fact row was last updated in Gold.                  |

---

# 4. Fact Table Structures

## 4.1 fact_quotation

| Property      | Value                                                                 |
| :------------ | :-------------------------------------------------------------------- |
| **Grain**     | One row per quotation issued to a customer                            |
| **Source**    | `insurance_crm_db.quotation` (Silver Layer) joined with `policy_info` |
| **Fact Type** | Transaction Fact                                                      |

| Column                      | Type          | FK / Role                   | Description                                                                                                 |
| :-------------------------- | :------------ | :-------------------------- | :---------------------------------------------------------------------------------------------------------- |
| `quotation_key`             | BIGINT        | FK → `dim_quotation`        | Shared dimension surrogate key path to support proper semantic modeling and clean structural relationships. |
| `quotation_id`              | VARCHAR(20)   | Degenerate Dimension        | Natural key retained for traceability and drill-through.                                                    |
| `customer_key`              | BIGINT        | FK → `dim_customer`         | Reference to the customer who requested the quotation.                                                      |
| `vehicle_key`               | BIGINT        | FK → `dim_vehicle`          | Reference to the insured vehicle associated with the quotation.                                             |
| `customer_id`               | BIGINT       | FK → `dim_customer`           | Reference to the customer who requested the quotation. |
| `agent_id`                  | BIGINT       | FK → `dim_agent`              | Reference to the agent responsible for the quotation. |
| `provider_code`             | VARCHAR(50)  | FK → `dim_insurance_provider` | Reference to the insurance provider associated with the quotation. |
| `agent_key`                 | BIGINT        | FK → `dim_agent`            | Reference to the agent who handled the quotation.                                                           |
| `provider_key`              | BIGINT        | FK → `dim_provider`         | Reference to the insurance provider.                                                                        |
| `package_key`               | BIGINT        | FK → `dim_package`          | Reference to the insurance package offered.                                                                 |
| `quotation_status_key`      | BIGINT        | FK → `dim_quotation_status` | Reference to the current quotation status.                                                                  |
| `quotation_date_key`        | INT           | FK → `dim_date`             | Date key representing quotation creation date.                                                              |
| `quotation_expiry_date_key` | INT           | FK → `dim_date`             | Date key representing quotation expiry date.                                                                |
| `converted_flag`            | TINYINT       | Derived Metric              | Indicates whether the quotation converted to a policy (`1` = Converted, `0` = Not Converted).               |
| `premium_amount`            | DECIMAL(18,2) | Measure                     | Gross quoted premium amount.                                                                                |

### Design Notes

* **dim_quotation Integration:** Both fact tables reference `dim_quotation` through `quotation_key`, avoiding direct fact-to-fact joins.
* **Conversion Business Rule:** `converted_flag = 1` when the quotation exists in `policy_info`.
* **Role-Playing Dates:** Separate date keys support independent analysis of quotation creation and expiry dates.

---

## 4.2 fact_quotation_item

| Property      | Value                                             |
| :------------ | :------------------------------------------------ |
| **Grain**     | One row per coverage line item within a quotation |
| **Source**    | `insurance_crm_db.quotation_item` (Silver Layer)  |
| **Fact Type** | Transaction Fact                                  |

| Column                 | Type          | FK / Role                   | Description                                                     |
| :--------------------- | :------------ | :-------------------------- | :-------------------------------------------------------------- |
| `quotation_item_id`    | VARCHAR(20)   | Degenerate Dimension        | Natural key for quotation line-item traceability.               |
| `quotation_key`        | BIGINT        | FK → `dim_quotation`        | Links to parent quotation dimension.                            |
| `quotation_id`         | VARCHAR(20)   | Degenerate Dimension        | Parent quotation identifier retained for traceability.          |
| `customer_key`         | BIGINT        | FK → `dim_customer`         | Customer associated with the quotation.                         |
| `quotation_date_key`   | INT           | FK → `dim_date`             | Denormalized parent quotation date.                             |
| `agent_key`            | BIGINT        | FK → `dim_agent`            | Denormalized parent quotation agent.                            |
| `provider_key`         | BIGINT        | FK → `dim_provider`         | Denormalized parent quotation provider.                         |
| `vehicle_key`          | BIGINT        | FK → `dim_vehicle`          | Reference to the insured vehicle associated with the quotation. |
| `package_key`          | BIGINT        | FK → `dim_package`          | Denormalized parent quotation package.                          |
| `quotation_status_key` | BIGINT        | FK → `dim_quotation_status` | Denormalized parent quotation status.                           |
| `coverage_key`         | BIGINT        | FK → `dim_coverage`         | Standardized coverage type reference.                           |
| `coverage_amount`      | DECIMAL(18,2) | Measure                     | Maximum insured amount for this coverage line.                  |
| `deductible_amount`    | DECIMAL(18,2) | Measure                     | Customer deductible amount. Defaults to `0`.                    |

### Design Notes

* Parent quotation attributes are intentionally denormalized to simplify coverage-level analytics.
* `deductible_amount` is enforced to default to `0` to preserve additive calculations.

---

# 5. Dimension Reference Summary

| Dimension Table        | fact_quotation | fact_quotation_item |
| :--------------------- | :------------: | :-----------------: |
| `dim_quotation`        |        ✓       |          ✓          |
| `dim_date`             |       2×       |          ✓          |
| `dim_customer`         |        ✓       |          ✓          |
| `dim_vehicle`          |        ✓       |          ✓          |
| `dim_agent`            |        ✓       |          ✓          |
| `dim_provider`         |        ✓       |          ✓          |
| `dim_package`          |        ✓       |          ✓          |
| `dim_quotation_status` |        ✓       |          ✓          |
| `dim_coverage`         |        —       |          ✓          |

---

# 6. Measures Summary

| Fact Table            | Column              | Type          | Additive? | Notes                             |
| :-------------------- | :------------------ | :------------ | :-------- | :-------------------------------- |
| `fact_quotation`      | `premium_amount`    | DECIMAL(18,2) | Yes       | Gross quoted premium value.       |
| `fact_quotation`      | `converted_flag`    | TINYINT       | Yes       | Sum = Total Converted Quotations. |
| `fact_quotation_item` | `coverage_amount`   | DECIMAL(18,2) | Yes       | Total insured exposure.           |
| `fact_quotation_item` | `deductible_amount` | DECIMAL(18,2) | Yes       | Total deductible exposure.        |

---

# 7. Source-to-Gold Column Mapping

## 7.1 fact_quotation ← insurance_crm_db.quotation + policy_info

| Source Column           | Gold Column                 | Transform                                                                            |
| :---------------------- | :-------------------------- | :----------------------------------------------------------------------------------- |
| `quotation_id`          | `quotation_key`             | Lookup → `dim_quotation`                                                             |
| `quotation_id`          | `quotation_id`              | Direct mapping                                                                       |
| `customer_id`           | `customer_key`              | Lookup → `dim_customer`                                                              |
| `customer_id`           | `vehicle_key`               | `quotation.customer_id` → `vehicle.customer_id` → Lookup → `dim_vehicle.vehicle_key` |
| `agent_id`              | `agent_key`                 | Lookup → `dim_agent`                                                                 |
| `provider_code`         | `provider_key`              | Lookup → `dim_provider`                                                              |
| `package_code`          | `package_key`               | Lookup → `dim_package`                                                               |
| `quotation_status`      | `quotation_status_key`      | Lookup → `dim_quotation_status`                                                      |
| `quotation_date`        | `quotation_date_key`        | `CAST(DATE_FORMAT(...))`                                                             |
| `quotation_expiry_date` | `quotation_expiry_date_key` | `CAST(DATE_FORMAT(...))`                                                             |
| *System Cross-Check*    | `converted_flag`            | Policy existence check against `policy_info`                                         |
| `premium_amount`        | `premium_amount`            | Direct mapping                                                                       |
| *Pipeline Metadata*     | *Technical Columns*         | Generated during pipeline execution                                                  |

### Vehicle Key Resolution

```text
quotation.customer_id
    -> vehicle.customer_id
    -> dim_vehicle.vehicle_key
```

---

## 7.2 fact_quotation_item ← insurance_crm_db.quotation_item + parent quotation lookup

| Source Column        | Gold Column            | Transform                                                                   |
| :------------------- | :--------------------- | :-------------------------------------------------------------------------- |
| `quotation_item_id`  | `quotation_item_id`    | Direct mapping                                                              |
| `quotation_id`       | `quotation_key`        | Lookup → `dim_quotation`                                                    |
| `quotation_id`       | `quotation_id`         | Direct mapping                                                              |
| (Via quotation join) | `customer_key`         | `quotation.customer_id` → `dim_customer`                                    |
| (Via quotation join) | `vehicle_key`          | `quotation.customer_id` → `vehicle.customer_id` → `dim_vehicle.vehicle_key` |
| (Via quotation join) | `quotation_date_key`   | Parent quotation date lookup                                                |
| (Via quotation join) | `agent_key`            | Parent agent lookup                                                         |
| (Via quotation join) | `provider_key`         | Parent provider lookup                                                      |
| (Via quotation join) | `package_key`          | Parent package lookup                                                       |
| (Via quotation join) | `quotation_status_key` | Parent status lookup                                                        |
| `coverage_type`      | `coverage_key`         | Standardize free-text value → Lookup → `dim_coverage`                       |
| `coverage_amount`    | `coverage_amount`      | Direct mapping                                                              |
| `deductible_amount`  | `deductible_amount`    | `COALESCE(deductible_amount, 0.00)`                                         |
| *Pipeline Metadata*  | *Technical Columns*    | Generated during pipeline execution                                         |

### Vehicle Key Resolution

```text
quotation_item
    -> quotation
    -> quotation.customer_id
    -> vehicle.customer_id
    -> dim_vehicle.vehicle_key
```

---

# 8. Open Items and Recommendations

| #  | Table                                   | Item                                            | Recommendation                                                                                                                                                                                                                                                    |
| :- | :-------------------------------------- | :---------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1  | `fact_quotation_item`                   | Limited coverage profiles in early seed records | Pre-populate `dim_coverage` with standard industry coverage categories before production release.                                                                                                                                                                 |
| 2  | `fact_quotation_item`                   | Free-text source values (`NVARCHAR(100)`)       | Implement regex-based standardization rules in Silver before loading into Gold.                                                                                                                                                                                   |
| 3  | `fact_quotation`, `fact_quotation_item` | Vehicle context implementation                  | Vehicle context is already included through `vehicle_key` in both fact tables using the current 1-to-1 customer-to-vehicle assumption. |

---

# 9. Revision History

| Version | Date       | Author           | Notes                                                                                                                                                                                           |
| :------ | :--------- | :--------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1.0     | 2026-05-31 | Data Engineering | Initial draft — fact_quotation and fact_quotation_item defined.                                                                                                                                 |
| 1.1     | 2026-06-02 | Data Engineering | Added dim_quotation relationship path, converted_flag logic, denormalized quotation context, lineage standards, vehicle_key mapping documentation, and updated vehicle context recommendations. |
