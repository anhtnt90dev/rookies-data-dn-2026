# Gold Layer Fact Tables — Field Design Document
### Insurance Analytics — Dimensional Model (Star Schema)

## 1. Purpose
This document defines the proposed Gold Layer Fact Table structures for the following two fact tables derived from the CRM source system (`insurance_crm_db`):
* `fact_quotation`
* `fact_quotation_item`

These definitions serve as the authoritative reference for downstream BI development, data validation, and pipeline engineering.

## 2. General Fact Table Design Standards
| Standard | Description |
| :--- | :--- |
| **Grain** | Each fact table is defined at the most atomic level of its business event. |
| **Surrogate Keys** | All foreign keys reference dimension surrogate keys (`BIGINT`), not source natural keys. |
| **Date Keys** | All date fields are replaced by integer date keys in `YYYYMMDD` format referencing `dim_date`. |
| **Measures** | Only fully additive or semi-additive numeric values are stored as measures. All categorical context lives in dimensions. |
| **Degenerate Dimensions**| Identifiers that carry no descriptive attributes but are critical for traceability (`quotation_id`) are stored directly in the fact table. |
| **No NULLs on measures** | Measure columns default to 0 rather than NULL; enforced at the Silver → Gold transform layer. |
| **Lineage & Soft Delete** | Comprehensive audit and execution tracking fields are present to isolate processing batches and exclude logically deleted records from BI measures. |

## 3. Common Technical Columns
All fact tables share the following standard Gold lineage, batch control, and audit columns:

| Column | Type | Description |
| :--- | :--- | :--- |
| `source_system` | STRING | Source system name (e.g., `insurance_crm_db`). |
| `batch_id` | STRING | Identifier of the data platform processing batch that loaded this row. |
| `pipeline_run_id` | STRING | Specific execution ID of the pipeline run for lineage tracking. |
| `is_deleted` | TINYINT | Indicator for soft deletes (`0` = Active, `1` = Deleted in source). Enforced to exclude rows from active KPIs. |
| `deleted_at` | TIMESTAMP | Timestamp when the row was flagged as deleted. |
| `delete_batch_id` | STRING | Processing batch identifier that executed the soft delete. |
| `created_at` | TIMESTAMP | Timestamp when the fact row was first loaded into Gold. |
| `updated_at` | TIMESTAMP | Timestamp when the fact row was last updated in Gold. |

---

## 4. Fact Table Structures

### 4.1 fact_quotation
| Property | Value |
| :--- | :--- |
| **Grain** | One row per quotation issued to a customer |
| **Source** | `insurance_crm_db.quotation` (Silver Layer) joined with `policy_info` |
| **Fact Type** | Transaction fact |

| Column | Type | FK / Role | Description |
| :--- | :--- | :--- | :--- |
| `quotation_key` | BIGINT | FK → `dim_quotation` | Shared dimension surrogate key path to support proper semantic modeling and clean structural relationships. |
| `quotation_id` | VARCHAR(20) | Degenerate Dimension | Natural key from source retained directly in the fact table for end-to-end traceability and drill-through. |
| `customer_key` | BIGINT | FK → `dim_customer` | Reference to the customer who requested the quotation. |
| `vehicle_key` | BIGINT | FK → `dim_vehicle` | Reference to the insured vehicle for which the quotation was requested. |
| `agent_key` | BIGINT | FK → `dim_agent` | Reference to the agent who handled and generated the quotation. |
| `provider_key` | BIGINT | FK → `dim_provider` | Reference to the insurance provider associated with the quotation. |
| `package_key` | BIGINT | FK → `dim_package` | Reference to the insurance package offered (BASIC / STANDARD / PREMIUM / VIP). |
| `quotation_status_key` | BIGINT | FK → `dim_quotation_status` | Reference to the current quotation status (QUOTED / ACCEPTED / REJECTED / EXPIRED / CONVERTED). |
| `quotation_date_key` | INT | FK → `dim_date` | Date key for when the quotation was created (`YYYYMMDD`). |
| `quotation_expiry_date_key`| INT | FK → `dim_date` | Date key for the quotation expiry date (`YYYYMMDD`). |
| `converted_flag` | TINYINT | Derived Column / Metric | Flag indicating if the quote successfully converted into a policy (`1` = Converted, `0` = Not Converted). Determined by existence check against `policy_info`. |
| `premium_amount` | DECIMAL(18,2) | Measure (Additive) | Quoted gross premium amount. Fully additive — can be summed across all dimensions. |

#### Design Notes:
* **dim_quotation Integration:** Rather than executing direct cross-fact joins via `quotation_id`, both fact tables now hook into `dim_quotation` via `quotation_key`. This guarantees compliance with standard multi-dimensional semantic modeling in Power BI and preserves structured relationship paths.
* **Conversion Business Rule:** To drive reliable conversion calculations on Dashboard 01, `converted_flag` uses a derived look-ahead logic: `converted_flag = 1` if `quotation_id` exists in the `policy_info` table. This decouples the core metric from brittle CRM status strings, providing a stable formula for DAX measures:  
  Conversion Rate = SUM(converted_flag) / COUNT(quotation_key)
* **Two separate date keys** (`quotation_date_key`, `quotation_expiry_date_key`) allow independent role-playing time-based slicing using a single shared `dim_date`.

---

### 4.2 fact_quotation_item
| Property | Value |
| :--- | :--- |
| **Grain** | One row per coverage line item within a quotation |
| **Source** | `insurance_crm_db.quotation_item` (Silver Layer) |
| **Fact Type** | Transaction fact |

| Column | Type | FK / Role | Description |
| :--- | :--- | :--- | :--- |
| `quotation_item_id` | VARCHAR(20) | Degenerate Dimension | Natural key from source. Granular line-item identifier stored for traceability. |
| `quotation_key` | BIGINT | FK → `dim_quotation` | Surrogate key linking back to `dim_quotation` and establishing the semantic path to parent metadata. |
| `quotation_id` | VARCHAR(20) | Degenerate Dimension | Parent quotation natural identifier retained for direct source traceability. |
| `customer_key` | BIGINT | FK → `dim_customer` | Reference to the customer. Denormalized onto this table for direct performance slicing. |
| `quotation_date_key` | INT | FK → `dim_date` | Denormalized from parent quote to support native time-based coverage slicing. |
| `agent_key` | BIGINT | FK → `dim_agent` | Denormalized from parent quote to enable agent-specific coverage analysis. |
| `provider_key` | BIGINT | FK → `dim_provider` | Denormalized from parent quote to analyze coverage distributions by insurance provider. |
| `package_key` | BIGINT | FK → `dim_package` | Denormalized from parent quote to analyze coverages across core product packages. |
| `quotation_status_key` | BIGINT | FK → `dim_quotation_status` | Denormalized from parent quote to filter active or converted coverages. |
| `coverage_key` | BIGINT | FK → `dim_coverage_type` | Reference to the standardized coverage type taxonomy (e.g., Physical Damage, Third Party Liability, PA). |
| `coverage_amount` | DECIMAL(18,2) | Measure (Additive) | Maximum insured value for this coverage line. Fully additive. |
| `deductible_amount` | DECIMAL(18,2) | Measure (Additive) | Customer-borne deductible for this coverage line. Defaults to 0; fully additive. |

#### Design Notes:
* **Context Denormalization:** Per PO feedback, comprehensive parent quotation context keys (`quotation_date_key`, `agent_key`, `provider_key`, `package_key`, `quotation_status_key`) have been explicitly denormalized into `fact_quotation_item`. This mitigates performance bottlenecks by eliminating the need for complex, heavy fact-to-fact or multi-hop joins when running localized coverage-level analytics.
* **Preserving Additivity:** `deductible_amount` defaults strictly to `0` instead of `NULL` to prevent mathematical discrepancies during aggregations in the Fabric Spark execution layer and DAX calculations.

---

## 5. Dimension Reference Summary
| Dimension Table | fact_quotation | fact_quotation_item |
| :--- | :---: | :---: |
| `dim_quotation` | ✓ (`quotation_key`) | ✓ (`quotation_key`) |
| `dim_date` | 2× (`quotation_date`, `expiry_date`) | ✓ (`quotation_date_key`) |
| `dim_customer` | ✓ | ✓ |
| `dim_agent` | ✓ | ✓ (Denormalized) |
| `dim_provider` | ✓ | ✓ (Denormalized) |
| `dim_package` | ✓ | ✓ (Denormalized) |
| `dim_quotation_status` | ✓ | ✓ (Denormalized) |
| `dim_coverage_type` | — | ✓ |

---

## 6. Measures Summary
All measures are fully additive. Analysts must ensure filters exclude soft-deleted rows using `is_deleted = 0` prior to running summaries.

| Fact Table | Column | Type | Additive? | Notes / Calculations |
| :--- | :--- | :--- | :--- | :--- |
| `fact_quotation` | `premium_amount` | DECIMAL(18,2) | Fully additive | Gross quoted premium value. |
| `fact_quotation` | `converted_flag` | TINYINT | Fully additive | Summation represents Total Converted Quotations. |
| `fact_quotation_item`| `coverage_amount` | DECIMAL(18,2) | Fully additive | Summation equals total portfolio risk exposure. |
| `fact_quotation_item`| `deductible_amount`| DECIMAL(18,2) | Fully additive | Summation equals total customer risk retention. |

---

## 7. Source-to-Gold Column Mapping

### 7.1 fact_quotation ← insurance_crm_db.quotation + policy_info
| Source Column | Gold Column | Transform |
| :--- | :--- | :--- |
| `quotation_id` | `quotation_key` | Lookup → `dim_quotation` (get surrogate `BIGINT`) |
| `quotation_id` | `quotation_id` | Direct mapping (degenerate dimension) |
| `customer_id` | `customer_key` | Lookup → `dim_customer` |
| `agent_id` | `agent_key` | Lookup → `dim_agent` |
| `provider_code` | `provider_key` | Lookup → `dim_provider` |
| `package_code` | `package_key` | Lookup → `dim_package` |
| `quotation_status` | `quotation_status_key`| Lookup → `dim_quotation_status` |
| `quotation_date` | `quotation_date_key` | `CAST(DATE_FORMAT(quotation_date, 'yyyyMMdd') AS INT)` |
| `quotation_expiry_date`| `quotation_expiry_date_key`| `CAST(DATE_FORMAT(quotation_expiry_date, 'yyyyMMdd') AS INT)` |
| *System Cross-Check* | `converted_flag` | `CASE WHEN EXISTS(SELECT 1 FROM policy_info p WHERE p.quotation_id = q.quotation_id) THEN 1 ELSE 0 END` |
| `premium_amount` | `premium_amount` | Direct mapping |
| *Pipeline Metadata* | *Technical Columns* | Generated via Spark/Data Factory run context (`batch_id`, `run_id`, etc.) |

### 7.2 fact_quotation_item ← insurance_crm_db.quotation_item + parent quotation lookup
| Source Column | Gold Column | Transform |
| :--- | :--- | :--- |
| `quotation_item_id` | `quotation_item_id` | Direct mapping (degenerate dimension) |
| `quotation_id` | `quotation_key` | Lookup → `dim_quotation` (get surrogate `BIGINT`) |
| `quotation_id` | `quotation_id` | Direct mapping (degenerate dimension) |
| (Via `quotation` join) | `customer_key` | Join `quotation.customer_id` → Lookup → `dim_customer` |
| (Via `quotation` join) | `quotation_date_key` | Extract parent `quotation_date` → Format as `INT` |
| (Via `quotation` join) | `agent_key` | Extract parent `agent_id` → Lookup → `dim_agent` |
| (Via `quotation` join) | `provider_key` | Extract parent `provider_code` → Lookup → `dim_provider` |
| (Via `quotation` join) | `package_key` | Extract parent `package_code` → Lookup → `dim_package` |
| (Via `quotation` join) | `quotation_status_key`| Extract parent `quotation_status` → Lookup → `dim_quotation_status` |
| `coverage_type` | `coverage_type_key` | Standardize free-text string → Lookup → `dim_coverage_type` |
| `coverage_amount` | `coverage_amount` | Direct mapping |
| `deductible_amount` | `deductible_amount` | `COALESCE(deductible_amount, 0.00)` |
| *Pipeline Metadata* | *Technical Columns* | Generated via Spark/Data Factory run context (`batch_id`, `run_id`, etc.) |

---

## 8. Open Items and Recommendations
| # | Table | Item | Recommendation |
| :--- | :--- | :--- | :--- |
| 1 | `fact_quotation_item` | Limited coverage profiles in early seed records | Pre-populate `dim_coverage_type` master data with standard industry groupings (Third Party, Passenger Alternative, Comprehensive, Fire) before production release. |
| 2 | `fact_quotation_item` | Free-text source values (`NVARCHAR(100)`) | Build robust regex cleanup rules inside the Silver processing notebooks to guarantee standardized routing into dimension keys. |
| 3 | `fact_quotation` | Absence of vehicle-level attributes | Keep as a future enhancement phase. If telemetry or asset segment pricing analytics are needed, a `vehicle_key` from source can be denormalized directly. |

---

## 9. Revision History
| Version | Date | Author | Notes |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-05-31 | Data Engineering | Initial draft — fact_quotation and fact_quotation_item defined. |
| 1.1 | 2026-06-02 | Data Engineering | Revised per PO feedback: Created dim_quotation relationship path, embedded stable converted_flag rules, denormalized comprehensive quotation context onto items, and appended full Gold lineage/soft-delete standards. |
