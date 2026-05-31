# Task 15: Define Surrogate Key Strategy

## 1. Purpose

This document defines the surrogate key strategy for the Insurance Analytics Gold Layer dimensional model.

The strategy ensures that fact tables use stable dimension surrogate keys instead of only source system natural keys. This supports historical tracking, consistent joins, unknown members, and future source integration.

## 2. Key Principles

| Principle | Description |
|---|---|
| Facts join to dimensions using surrogate keys. | Fact tables should store dimension foreign keys such as `customer_key`, `provider_key`, and `policy_status_key`. |
| Natural keys are preserved in dimensions. | Source identifiers such as `customer_id`, `agent_id`, `provider_code`, `quotation_id`, and `policy_id` remain available for traceability. |
| Surrogate keys are system-generated. | Surrogate keys are generated in the Gold Layer and are independent from source business keys. |
| SCD Type 2 dimensions generate a new surrogate key per version. | A customer, agent, or provider can have multiple historical rows with the same business key but different surrogate keys. |
| Facts must resolve to the correct dimension version. | For Type 2 dimensions, fact event date is used to find the correct valid dimension version. |
| Unknown members are mandatory. | Unknown/default rows prevent fact load failures when a dimension member cannot be resolved. |

## 3. Surrogate Key Naming Standards

| Dimension | Surrogate Key | Business Key |
|---|---|---|
| `dim_date` | `date_key` | `full_date` |
| `dim_customer` | `customer_key` | `customer_id` |
| `dim_agent` | `agent_key` | `agent_id` |
| `dim_provider` | `provider_key` | `provider_code` |
| `dim_package` | `package_key` | `package_code` |
| `dim_coverage` | `coverage_key` | `coverage_type` |
| `dim_quotation` | `quotation_key` | `quotation_id` |
| `dim_policy` | `policy_key` | `policy_id` |
| `dim_quotation_status` | `quotation_status_key` | `quotation_status_code` |
| `dim_policy_status` | `policy_status_key` | `policy_status_code` |
| `dim_payment_status` | `payment_status_key` | `payment_status_code` |
| `dim_payment_method` | `payment_method_key` | `payment_method_code` |
| `dim_cancellation_reason` | `cancellation_reason_key` | `cancellation_reason_code` |

## 4. Recommended Key Data Types

| Key Type | Recommended Type | Reason |
|---|---|---|
| Most surrogate keys | BIGINT | Safe for growth and SCD Type 2 versioning. |
| `date_key` | INT | Standard `YYYYMMDD` date key. |
| Natural/business keys | STRING | Source IDs contain prefixes such as `CUS`, `AG`, `POL`, `QUO`, `PAY`, and `CAN`. |
| Degenerate identifiers in facts | STRING | Keep transaction references such as `payment_id`, `cancellation_id`, and `transaction_reference` for traceability. |

## 5. Surrogate Key Generation Approach

### 5.1 Preferred Approach

Use generated numeric surrogate keys in Gold dimension tables.

| Platform Pattern | Option |
|---|---|
| Fabric Lakehouse / Delta | Use identity column if supported, or generate keys with a controlled dimension load process. |
| Spark / DataFrame pipeline | Generate new keys only for new dimension records, using current max key plus row number for inserted rows. |
| SQL Warehouse | Use identity/sequence if the dimension is loaded through SQL tables. |

Recommended default for this project: **numeric BIGINT surrogate keys**.

### 5.2 Hash Key Alternative

A deterministic hash key can be used only if the team wants easier reprocessing. For SCD Type 2 dimensions, the hash must include the business key plus the version/effective timestamp, not only the business key.

## 6. Unknown and Default Member Strategy

Each dimension except `dim_date` should include an unknown row.

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

## 7.1 `fact_quotation`

| Dimension Key | Resolution Rule |
|---|---|
| `quotation_key` | Lookup `dim_quotation` by `quotation_id`. |
| `customer_key` | Lookup `dim_customer` by `customer_id` using `quotation_date`. |
| `agent_key` | Lookup `dim_agent` by `agent_id` using `quotation_date`. |
| `provider_key` | Lookup `dim_provider` by `provider_code` using `quotation_date`. |
| `package_key` | Lookup `dim_package` by `package_code`. |
| `quotation_status_key` | Lookup `dim_quotation_status` by `quotation_status`. |
| `quotation_date_key` | Lookup `dim_date` by `quotation_date`. |
| `quotation_expiry_date_key` | Lookup `dim_date` by `quotation_expiry_date`. |

## 7.2 `fact_quotation_item`

| Dimension Key | Resolution Rule |
|---|---|
| `quotation_key` | Lookup `dim_quotation` by `quotation_id`. |
| `coverage_key` | Lookup `dim_coverage` by `coverage_type`. |
| `quotation_date_key` | Inherit from quotation header by joining `quotation_item.quotation_id` to `quotation.quotation_id`. |
| Customer/agent/provider/package/status keys | Inherit from quotation header context. |

## 7.3 `fact_policy`

| Dimension Key | Resolution Rule |
|---|---|
| `policy_key` | Lookup `dim_policy` by `policy_id`. |
| `quotation_key` | Lookup `dim_quotation` by `quotation_id`. |
| `customer_key` | Lookup `dim_customer` by `customer_id` using `issued_date` or `policy_start_date`. |
| `provider_key` | Lookup `dim_provider` by `provider_code` using `issued_date` or `policy_start_date`. |
| `agent_key` | Inherit from related quotation if `policy_info.quotation_id` exists; otherwise use `-1`. |
| `package_key` | Inherit from related quotation if `policy_info.quotation_id` exists; otherwise use `-1`. |
| `policy_status_key` | Lookup `dim_policy_status` by `policy_status`. |
| Date keys | Lookup `dim_date` by issued, start, and end dates. |

## 7.4 `fact_payment`

| Dimension Key | Resolution Rule |
|---|---|
| `policy_key` | Lookup `dim_policy` by `policy_id`. |
| `payment_status_key` | Lookup `dim_payment_status` by `payment_status`. |
| `payment_method_key` | Lookup `dim_payment_method` by `payment_method`. |
| `payment_date_key` | Lookup `dim_date` by `payment_date`. |
| `customer_key` | Inherit from related policy using `payment.policy_id -> policy_info.policy_id`; resolve by `payment_date` where possible. |
| `provider_key` | Inherit from related policy using `payment.policy_id -> policy_info.policy_id`; resolve by `payment_date` where possible. |

## 7.5 `fact_cancellation`

| Dimension Key | Resolution Rule |
|---|---|
| `policy_key` | Lookup `dim_policy` by `policy_id`. |
| `cancellation_reason_key` | Lookup `dim_cancellation_reason` by `cancellation_reason`. |
| `cancellation_date_key` | Lookup `dim_date` by `cancellation_date`. |
| `customer_key` | Inherit from related policy using `cancellation.policy_id -> policy_info.policy_id`; resolve by `cancellation_date` where possible. |
| `provider_key` | Inherit from related policy using `cancellation.policy_id -> policy_info.policy_id`; resolve by `cancellation_date` where possible. |

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
| `fact_quotation` | `quotation_date` |
| `fact_quotation_item` | related `quotation_date` |
| `fact_policy` | `issued_date` or `policy_start_date`, depending on KPI context |
| `fact_payment` | `payment_date` |
| `fact_cancellation` | `cancellation_date` |

## 9. Natural Key Retention

Facts may keep selected natural keys for audit and traceability, but these should not be used as primary semantic relationships.

| Fact Table | Natural Keys to Retain for Traceability |
|---|---|
| `fact_quotation` | `quotation_id`, `customer_id`, `agent_id`, `provider_code` |
| `fact_quotation_item` | `quotation_item_id`, `quotation_id` |
| `fact_policy` | `policy_id`, `policy_number`, `quotation_id`, `customer_id`, `provider_code` |
| `fact_payment` | `payment_id`, `policy_id`, `transaction_reference` |
| `fact_cancellation` | `cancellation_id`, `policy_id` |

## 10. Out-of-Scope Key Handling

| Item | Handling |
|---|---|
| `vehicle_key` | Not generated or stored in current facts because `quotation` and `policy_info` do not have `vehicle_id`. |
| `region_key` | Not generated or stored in current facts because no confirmed `dim_region` mapping is in scope. Use `city`, `district`, or `agent.region` attributes for reporting until a mapping is confirmed. |

## 11. Key Quality Checks

| Check | Rule |
|---|---|
| Dimension surrogate key uniqueness | Each surrogate key must be unique within its dimension. |
| Current row uniqueness for Type 2 | Each business key should have only one `is_current = true` row. |
| Fact foreign key validity | All fact foreign keys should match a dimension key or use `-1`. |
| Unknown key monitoring | Count of `-1` keys should be tracked as a data quality metric. |
| Date key validity | All date keys should exist in `dim_date`, except allowed unknown/default date handling. |

## 12. Output

This document is the output for **Task 15: Define surrogate key strategy**.
