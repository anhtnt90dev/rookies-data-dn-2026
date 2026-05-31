# Gold Layer Fact Tables — Field Design Document
**Insurance Analytics — Dimensional Model (Star Schema)**

---

## 1. Purpose

This document defines the proposed Gold Layer Fact Table structures for the following two fact tables derived from the CRM source system (`insurance_crm_db`):

- `fact_quotation`
- `fact_quotation_item`

These definitions serve as the authoritative reference for downstream BI development, data validation, and pipeline engineering.

---

## 2. General Fact Table Design Standards

| Standard | Description |
|---|---|
| **Grain** | Each fact table is defined at the most atomic level of its business event. |
| **Surrogate Keys** | All foreign keys reference dimension surrogate keys (`BIGINT`), not source natural keys. |
| **Date Keys** | All date fields are replaced by integer date keys in `YYYYMMDD` format referencing `dim_date`. |
| **Measures** | Only fully additive or semi-additive numeric values are stored as measures. All categorical context lives in dimensions. |
| **Degenerate Dimensions** | Identifiers that carry no descriptive attributes are stored directly in the fact table without a dedicated dimension table. |
| **No NULLs on measures** | Measure columns default to `0` rather than `NULL`; enforced at the Silver → Gold transform layer. |

---

## 3. Common Technical Columns

All fact tables share the following audit and metadata columns:

| Column | Type | Description |
|---|---|---|
| `source_system` | STRING | Source system name (e.g., `insurance_crm_db`). |
| `created_at` | TIMESTAMP | Timestamp when the fact row was first loaded into Gold. |
| `updated_at` | TIMESTAMP | Timestamp when the fact row was last updated in Gold. |

---

## 4. Fact Table Structures

### 4.1 fact_quotation

| Property | Value |
|---|---|
| **Grain** | One row per quotation issued to a customer |
| **Source** | `insurance_crm_db.quotation` (Silver Layer) |
| **Fact Type** | Transaction fact |

| Column | Type | FK / Role | Description |
|---|---|---|---|
| `quotation_id` | VARCHAR(20) | Degenerate Dimension | Natural key from source. Retained directly in the fact table — unique identifier with no descriptive attributes requiring a separate dimension. Used for drill-through to `fact_quotation_item`. |
| `customer_key` | BIGINT | FK → `dim_customer` | Reference to the customer who requested the quotation. |
| `agent_key` | BIGINT | FK → `dim_agent` | Reference to the agent who handled and generated the quotation. |
| `provider_key` | BIGINT | FK → `dim_provider` | Reference to the insurance provider associated with the quotation. |
| `package_key` | BIGINT | FK → `dim_package` | Reference to the insurance package offered (BASIC / STANDARD / PREMIUM / VIP). |
| `quotation_status_key` | BIGINT | FK → `dim_quotation_status` | Reference to the quotation status (QUOTED / ACCEPTED / REJECTED / EXPIRED / CONVERTED). Enables conversion funnel analysis. |
| `quotation_date_key` | INT | FK → `dim_date` | Date key for when the quotation was created (YYYYMMDD). |
| `quotation_expiry_date_key` | INT | FK → `dim_date` | Date key for the quotation expiry date (YYYYMMDD). |
| `premium_amount` | DECIMAL(18,2) | Measure (Additive) | Quoted gross premium amount. Fully additive — can be summed across all dimensions. |
| `source_system` | STRING | Audit | Source system identifier. |
| `created_at` | TIMESTAMP | Audit | Gold load timestamp. |
| `updated_at` | TIMESTAMP | Audit | Gold last update timestamp. |

**Design Notes:**

- `quotation_id` is retained as a degenerate dimension to support drill-through to `fact_quotation_item` without requiring a dedicated `dim_quotation` table.
- Two separate date keys (`quotation_date_key`, `quotation_expiry_date_key`) allow independent time-based slicing — e.g., quotations created in a period vs. quotations expiring in a period — using the single shared `dim_date`.
- `quotation_status_key` is essential for conversion funnel analysis: QUOTED → ACCEPTED → CONVERTED tracks the full pipeline from initial quote to issued policy. This is the primary KPI of Dashboard 01.
- `agent_key` and `provider_key` are both present to allow direct agent performance and provider performance slicing without joining other fact tables.
- `premium_amount` is the sole additive measure. All categorical analysis (region, package type, provider group) is delegated to the referenced dimension tables.

---

### 4.2 fact_quotation_item

| Property | Value |
|---|---|
| **Grain** | One row per coverage line item within a quotation |
| **Source** | `insurance_crm_db.quotation_item` (Silver Layer) |
| **Fact Type** | Transaction fact |

| Column | Type | FK / Role | Description |
|---|---|---|---|
| `quotation_item_id` | VARCHAR(20) | Degenerate Dimension | Natural key from source. Granular line-item identifier with no descriptive attributes — stored directly without a dedicated dimension. |
| `quotation_id` | VARCHAR(20) | Degenerate Dimension | Parent quotation reference. Links back to `fact_quotation` via the shared degenerate dimension key, enabling parent-child analysis across the two fact tables. |
| `customer_key` | BIGINT | FK → `dim_customer` | Reference to the customer. Denormalised from the parent quotation to allow direct customer-level coverage exposure analysis without joining `fact_quotation`. |
| `coverage_type_key` | BIGINT | FK → `dim_coverage_type` | Reference to the standardised coverage type (e.g., Physical Damage, Third Party Liability, PA). Replaces the free-text `coverage_type` column from source. |
| `coverage_amount` | DECIMAL(18,2) | Measure (Additive) | Maximum insured value for this coverage line. Fully additive — can be summed to derive total portfolio exposure by customer, provider, or period. |
| `deductible_amount` | DECIMAL(18,2) | Measure (Additive) | Customer-borne deductible for this coverage line. Defaults to `0` if not applicable. Fully additive. |
| `source_system` | STRING | Audit | Source system identifier. |
| `created_at` | TIMESTAMP | Audit | Gold load timestamp. |
| `updated_at` | TIMESTAMP | Audit | Gold last update timestamp. |

**Design Notes:**

- `quotation_id` as a degenerate dimension enables a direct cross-fact join between `fact_quotation` and `fact_quotation_item` without an intermediate bridge table.
- `customer_key` is intentionally denormalised onto this table. While it can be derived by joining through `fact_quotation`, direct presence avoids a multi-hop join when analysts need to aggregate total coverage exposure by customer.
- `coverage_type_key` replaces the raw `coverage_type` free-text field from source (`NVARCHAR(100)`). The Silver layer is responsible for standardising and mapping values to `dim_coverage_type` before loading to Gold.
- Both `coverage_amount` and `deductible_amount` are fully additive measures. Summing `coverage_amount` gives total insured value across a portfolio segment; summing `deductible_amount` gives total customer risk retention.
- `deductible_amount` defaults to `0` rather than `NULL` to preserve additivity — NULL values would cause incorrect SUM aggregations in both DAX (Power BI semantic model) and Spark SQL (Fabric notebook).

---

## 5. Dimension Reference Summary

| Dimension Table | `fact_quotation` | `fact_quotation_item` |
|---|:---:|:---:|
| `dim_date` | 2× (quotation_date, expiry_date) | — |
| `dim_customer` | ✓ | ✓ |
| `dim_agent` | ✓ | — |
| `dim_provider` | ✓ | — |
| `dim_package` | ✓ | — |
| `dim_quotation_status` | ✓ | — |
| `dim_coverage_type` | — | ✓ |

---

## 6. Measures Summary

The following measures are defined across the two fact tables. All measures are fully additive unless stated otherwise.

| Fact Table | Column | Type | Additive? | Notes |
|---|---|---|---|---|
| `fact_quotation` | `premium_amount` | DECIMAL(18,2) | Fully additive | Quoted premium before policy issuance. |
| `fact_quotation_item` | `coverage_amount` | DECIMAL(18,2) | Fully additive | Maximum insured value per coverage line. |
| `fact_quotation_item` | `deductible_amount` | DECIMAL(18,2) | Fully additive | Customer deductible per line. Defaults to `0`. |

---

## 7. Source-to-Gold Column Mapping

### 7.1 fact_quotation ← insurance_crm_db.quotation

| Source Column | Gold Column | Transform |
|---|---|---|
| `quotation_id` | `quotation_id` | Direct (degenerate dimension) |
| `customer_id` | `customer_key` | Lookup → `dim_customer` |
| `agent_id` | `agent_key` | Lookup → `dim_agent` |
| `provider_code` | `provider_key` | Lookup → `dim_provider` |
| `package_code` | `package_key` | Lookup → `dim_package` |
| `quotation_status` | `quotation_status_key` | Lookup → `dim_quotation_status` |
| `quotation_date` | `quotation_date_key` | FORMAT(date, 'YYYYMMDD') → INT |
| `quotation_expiry_date` | `quotation_expiry_date_key` | FORMAT(date, 'YYYYMMDD') → INT |
| `premium_amount` | `premium_amount` | Direct |

### 7.2 fact_quotation_item ← insurance_crm_db.quotation_item

| Source Column | Gold Column | Transform |
|---|---|---|
| `quotation_item_id` | `quotation_item_id` | Direct (degenerate dimension) |
| `quotation_id` | `quotation_id` | Direct (degenerate dimension) |
| *(via quotation join)* | `customer_key` | Join `quotation.customer_id` → Lookup → `dim_customer` |
| `coverage_type` | `coverage_type_key` | Standardise text → Lookup → `dim_coverage_type` |
| `coverage_amount` | `coverage_amount` | Direct |
| `deductible_amount` | `deductible_amount` | COALESCE(value, 0) |

---

## 8. Open Items and Recommendations

| # | Table | Item | Recommendation |
|---|---|---|---|
| 1 | `fact_quotation_item` | Source data only contains `Physical Damage` coverage type in seed data | Pre-populate `dim_coverage_type` with full coverage taxonomy (Third Party, PA, Comprehensive, Others) before go-live. |
| 2 | `fact_quotation_item` | `coverage_type` is free-text `NVARCHAR(100)` in source | Implement Silver-layer cleansing rule to standardise and map to `dim_coverage_type` codes before Gold load. |
| 3 | `fact_quotation` | No vehicle-level dimension on the quotation fact | Consider adding `vehicle_key` FK in a future revision if vehicle-segmented premium analysis is required (vehicle table exists in source). |

---

## 9. Revision History

| Version | Date | Author | Notes |
|---|---|---|---|
| 1.0 | 2026-05-31 | Data Engineering Team | Initial draft — `fact_quotation` and `fact_quotation_item` defined. |
