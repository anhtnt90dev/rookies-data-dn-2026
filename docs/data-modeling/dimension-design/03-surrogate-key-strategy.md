# Task 15: Define Surrogate Key Strategy

## 1. Purpose

This document defines the surrogate key strategy for the Insurance Analytics Gold Layer dimensional model.

The strategy ensures that fact tables use stable dimension surrogate keys instead of source system natural keys. This allows the model to support historical tracking, consistent joins, unknown members, and future source integration.

## 2. Key Principles

| Principle | Description |
|---|---|
| Facts join to dimensions using surrogate keys. | Fact tables should store dimension foreign keys such as `customer_key`, `provider_key`, and `policy_status_key`, not only source IDs. |
| Natural keys are preserved in dimensions. | Source identifiers such as `customer_id`, `agent_id`, `provider_code`, `quotation_id`, and `policy_id` remain available for traceability. |
| Surrogate keys are system-generated. | Surrogate keys are generated in the Gold Layer and are independent from source business keys. |
| SCD Type 2 dimensions generate a new surrogate key per version. | A customer or agent can have multiple historical rows with the same business key but different surrogate keys. |
| Facts must resolve to the correct dimension version. | For Type 2 dimensions, fact event date is used to find the correct active version. |
| Unknown members are mandatory. | Unknown/default rows prevent fact load failures when a dimension member cannot be resolved. |

## 3. Surrogate Key Naming Standards

| Dimension | Surrogate Key | Business Key |
|---|---|---|
| `Dim_Date` | `date_key` | `full_date` |
| `Dim_Customer` | `customer_key` | `customer_id` |
| `Dim_Vehicle` | `vehicle_key` | `vehicle_id` |
| `Dim_Agent` | `agent_key` | `agent_id` |
| `Dim_Provider` | `provider_key` | `provider_code` |
| `Dim_Region` | `region_key` | `region_code` |
| `Dim_Package` | `package_key` | `package_code` |
| `Dim_Coverage` | `coverage_key` | `coverage_type` |
| `Dim_Quotation` | `quotation_key` | `quotation_id` |
| `Dim_Policy` | `policy_key` | `policy_id` |
| `Dim_Quote_Status` | `quote_status_key` | `quote_status_code` |
| `Dim_Policy_Status` | `policy_status_key` | `policy_status_code` |
| `Dim_Payment_Status` | `payment_status_key` | `payment_status_code` |
| `Dim_Payment_Method` | `payment_method_key` | `payment_method_code` |
| `Dim_Cancellation_Reason` | `cancellation_reason_key` | `cancellation_reason_code` |

## 4. Recommended Key Data Types

| Key Type | Recommended Type | Reason |
|---|---|---|
| Most surrogate keys | BIGINT | Safe for growth and SCD Type 2 versioning. |
| `date_key` | INT | Standard `YYYYMMDD` date key. |
| Natural/business keys | STRING | Source IDs contain prefixes such as `CUS`, `AG`, `POL`, `QUO`, `PAY`, `CAN`. |
| Degenerate identifiers in facts | STRING | Keep transaction references such as `transaction_reference` for traceability if needed. |

## 5. Surrogate Key Generation Approach

### 5.1 Preferred Approach

Use generated numeric surrogate keys in Gold dimension tables.

Recommended implementation options:

| Platform Pattern | Option |
|---|---|
| Fabric Lakehouse / Delta | Use identity column if supported, or generate keys with a controlled dimension load process. |
| Spark / DataFrame pipeline | Generate new keys only for new dimension records, using current max key + row number for inserted rows. |
| SQL Warehouse | Use identity/sequence if the dimension is loaded through SQL tables. |

### 5.2 Hash Key Alternative

A deterministic hash key can be used only if the team wants easier reprocessing. However, for SCD Type 2 dimensions, the hash must include business key plus version/effective timestamp, not only the business key.

Recommended default for this project: **numeric BIGINT surrogate keys**.

## 6. Unknown and Default Member Strategy

Each dimension except `Dim_Date` should include an unknown row.

| Key Value | Meaning | Usage |
|---:|---|---|
| `-1` | Unknown | Business key is missing, invalid, or not found in the dimension. |
| `-2` | Not Applicable | Optional. Used when the dimension does not apply to the fact row. |
| `-3` | Error / Invalid | Optional. Used when source value exists but fails validation. |

Recommended minimum for Sprint 1:

```text
-1 = Unknown
```

Example unknown customer row:

| Column | Value |
|---|---|
| `customer_key` | `-1` |
| `customer_id` | `UNKNOWN` |
| `full_name` | `Unknown Customer` |
| `is_current` | `true` |
| `effective_from` | `1900-01-01` |
| `effective_to` | `9999-12-31` |

## 7. Fact Key Resolution Rules

## 7.1 `Fact_Quotation`

| Dimension Key | Resolution Rule |
|---|---|
| `quotation_key` | Lookup `Dim_Quotation` by `quotation_id`. |
| `customer_key` | Lookup `Dim_Customer` by `customer_id` using `quotation_date`. |
| `vehicle_key` | Lookup `Dim_Vehicle` by customer relationship using `quotation_date`. |
| `agent_key` | Lookup `Dim_Agent` by `agent_id` using `quotation_date`. |
| `provider_key` | Lookup `Dim_Provider` by `provider_code` using `quotation_date`. |
| `region_key` | Resolve from agent/customer standardized region mapping. |
| `package_key` | Lookup `Dim_Package` by `package_code`. |
| `quote_status_key` | Lookup `Dim_Quote_Status` by `quotation_status`. |
| `quotation_date_key` | Lookup `Dim_Date` by `quotation_date`. |
| `quotation_expiry_date_key` | Lookup `Dim_Date` by `quotation_expiry_date`. |

## 7.2 `Fact_Quotation_Item`

| Dimension Key | Resolution Rule |
|---|---|
| `quotation_key` | Lookup `Dim_Quotation` by `quotation_id`. |
| `coverage_key` | Lookup `Dim_Coverage` by `coverage_type`. |
| Customer/vehicle/agent/provider/package/status/date keys | Inherit from quotation header context by joining `quotation_item` to `quotation`. |

## 7.3 `Fact_Policy`

| Dimension Key | Resolution Rule |
|---|---|
| `policy_key` | Lookup `Dim_Policy` by `policy_id`. |
| `quotation_key` | Lookup `Dim_Quotation` by `quotation_id`. |
| `customer_key` | Lookup `Dim_Customer` by `customer_id` using `issued_date` or `policy_start_date`. |
| `vehicle_key` | Resolve from customer using `issued_date` or `policy_start_date`. |
| `provider_key` | Lookup `Dim_Provider` by `provider_code` using `issued_date`. |
| `agent_key` | Inherit from related quotation if available. Otherwise use `-1`. |
| `region_key` | Resolve from inherited agent/customer region. |
| `package_key` | Inherit from related quotation if available. Otherwise use `-1`. |
| `policy_status_key` | Lookup `Dim_Policy_Status` by `policy_status`. |
| Date keys | Lookup `Dim_Date` by issued, start, end, and cancellation dates. |

## 7.4 `Fact_Payment`

| Dimension Key | Resolution Rule |
|---|---|
| `policy_key` | Lookup `Dim_Policy` by `policy_id`. |
| `payment_status_key` | Lookup `Dim_Payment_Status` by `payment_status`. |
| `payment_method_key` | Lookup `Dim_Payment_Method` by `payment_method`. |
| `payment_date_key` | Lookup `Dim_Date` by `payment_date`. |
| Customer/vehicle/agent/provider/region/package/policy status keys | Inherit from related policy and quotation context. |

## 7.5 `Fact_Cancellation`

| Dimension Key | Resolution Rule |
|---|---|
| `policy_key` | Lookup `Dim_Policy` by `policy_id`. |
| `cancellation_reason_key` | Lookup `Dim_Cancellation_Reason` by `cancellation_reason`. |
| `cancellation_date_key` | Lookup `Dim_Date` by `cancellation_date`. |
| Customer/vehicle/agent/provider/region/package/policy status keys | Inherit from related policy and quotation context. |

## 8. SCD Type 2 Key Resolution

For Type 2 dimensions, a fact must join to the dimension row that was valid at the time of the business event.

General condition:

```sql
fact.business_event_timestamp >= dim.effective_from
AND fact.business_event_timestamp < dim.effective_to
AND fact.business_key = dim.business_key
```

Recommended event date by fact:

| Fact Table | Event Date for SCD Lookup |
|---|---|
| `Fact_Quotation` | `quotation_date` |
| `Fact_Quotation_Item` | related `quotation_date` |
| `Fact_Policy` | `issued_date` or `policy_start_date`, depending on KPI context |
| `Fact_Payment` | `payment_date` |
| `Fact_Cancellation` | `cancellation_date` |

## 9. Natural Key Retention

Facts may keep selected natural keys for audit and traceability, but these should not be used as primary semantic relationships.

| Fact Table | Natural Keys to Retain for Traceability |
|---|---|
| `Fact_Quotation` | `quotation_id`, `customer_id`, `agent_id`, `provider_code` |
| `Fact_Quotation_Item` | `quotation_item_id`, `quotation_id` |
| `Fact_Policy` | `policy_id`, `policy_number`, `quotation_id`, `customer_id`, `provider_code` |
| `Fact_Payment` | `payment_id`, `policy_id`, `transaction_reference` |
| `Fact_Cancellation` | `cancellation_id`, `policy_id` |

## 10. Key Quality Checks

| Check | Rule |
|---|---|
| Dimension surrogate key uniqueness | Each surrogate key must be unique within its dimension. |
| Current row uniqueness for Type 2 | Each business key should have only one `is_current = true` row. |
| Fact foreign key validity | All fact foreign keys should match a dimension key or use `-1`. |
| Unknown key monitoring | Count of `-1` keys should be tracked as data quality metric. |
| Date key validity | All date keys should exist in `Dim_Date`, except allowed unknown/default date handling. |

## 11. Output

This document is the output for **Task 15: Define surrogate key strategy**.
