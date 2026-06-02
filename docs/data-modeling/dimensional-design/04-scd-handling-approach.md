# Task 85: Define SCD Handling Approach

## 1. Purpose

This document defines the Slowly Changing Dimension handling approach for the Insurance Analytics Gold Layer.

The objective is to define which dimensions require historical tracking, which dimensions can be overwritten, and how dimension changes should be processed during incremental loads.

This version is aligned with the current Gold star schema scope. `dim_vehicle` and `dim_region` are excluded from the current implementation scope.

## 2. SCD Type Summary

| Dimension | SCD Type | Reason |
|---|---:|---|
| `dim_date` | No SCD | Calendar is generated and does not change historically. |
| `dim_customer` | Type 2 | Customer profile and geography may change. Historical reporting should preserve customer attributes at the time of quotation, policy, payment, or cancellation. |
| `dim_agent` | Type 2 | Agent region, branch, or manager may change. Historical agent performance should be reported using the assignment valid at the event time. |
| `dim_provider` | Type 2 | Provider group or active flag may change. Historical provider reporting should preserve old provider attributes. |
| `dim_vehicle` | Type 2 | Vehicle specification and value can change historically. Under the assumption that a customer owns exactly one vehicle, it is tracked as a Type 2 dimension. |
| `Dim_Package` | Type 1 | Small reference dimension derived from distinct `quotation.package_code` values. Current source only supports package code; additional package attributes require confirmed mapping or derivation rules. |
| `dim_coverage` | Type 1 | Coverage type is a reference value. Changes are expected to be corrections or enrichments. |
| `dim_quotation` | Type 1 | Identifier dimension for grouping quotation header, quotation item, and related policy facts. Historical status is handled separately by facts/status dimension. |
| `dim_policy` | Type 1 | Transaction identifier dimension for grouping policy, payment, and cancellation facts. Policy status is handled separately by `dim_policy_status`. |
| `dim_quotation_status` | Type 1 | Status reference table. Business definition changes should overwrite or be managed as metadata. |
| `dim_policy_status` | Type 1 | Status reference table. Business definition changes should overwrite or be managed as metadata. |
| `dim_payment_status` | Type 1 | Status reference table. Business definition changes should overwrite or be managed as metadata. |
| `dim_payment_method` | Type 1 | Payment method reference. Corrections/enrichments should overwrite. |
| `dim_cancellation_reason` | Type 1 | Cancellation reason reference. Corrections/enrichments should overwrite. |

## 3. Source Change Detection Context

| Source Area | Change Tracking Field / Pattern | Usage |
|---|---|---|
| CRM SQL sources | `created_date`, `updated_date` | Used to identify new and updated CRM records for customer, agent, provider, quotation, and quotation item related dimensions. |
| Policy JSON / policy DB source | `operation_type`, `last_updated`, `batch_date`, `source_system` where available | Used to process insert/update/delete events for policy-related dimensions and facts. |
| Payment JSON / payment DB source | `operation_type`, `last_updated`, `batch_date`, `source_system` where available | Used to process insert/update events for payment-related reference dimensions and facts. |
| Cancellation JSON / policy DB source | `operation_type`, `last_updated`, `batch_date`, `source_system` where available | Used to process insert/update events for cancellation-related reference dimensions and facts. |

## 4. Type 1 Handling Approach

Type 1 dimensions overwrite existing rows when source attributes change. They do not keep historical versions.

Applicable dimensions:

- `dim_package`
- `dim_coverage`
- `dim_quotation`
- `dim_policy`
- `dim_quotation_status`
- `dim_policy_status`
- `dim_payment_status`
- `dim_payment_method`
- `dim_cancellation_reason`

### Type 1 Processing Logic

| Step | Logic |
|---|---|
| 1 | Standardize and deduplicate incoming source values by business key. |
| 2 | Lookup existing dimension record by business key. |
| 3 | If business key does not exist, insert a new dimension row. |
| 4 | If business key exists and tracked attributes changed, update the existing row in place. |
| 5 | Update `updated_at` for changed records. |

### Type 1 Example

If payment method `E-wallet` is standardized to `E-Wallet`, update `dim_payment_method` in place. Historical facts remain connected to the same `payment_method_key`.

## 5. Type 2 Handling Approach

Type 2 dimensions preserve historical versions when selected descriptive attributes change.

Applicable dimensions:

- `dim_customer`
- `dim_agent`
- `dim_provider`
- `dim_vehicle`

### Type 2 Required Columns

| Column | Description |
|---|---|
| Surrogate key | Unique key for each dimension version. |
| Business key | Stable source identifier, for example `customer_id`, `agent_id`, or `provider_code`. |
| Type 2 tracked attributes | Attributes that require history preservation. |
| `effective_from` | Timestamp when the version becomes valid. |
| `effective_to` | Timestamp when the version stops being valid. |
| `is_current` | Current active version flag. |
| `is_deleted` | Indicates deleted/inactive source records if supported. |
| `source_system` | Origin system. |
| `created_at` | Gold insert timestamp. |
| `updated_at` | Gold update timestamp. |

### Type 2 Processing Logic

| Step | Logic |
|---|---|
| 1 | Load incremental source records based on `created_date`, `updated_date`, or operation metadata. |
| 2 | Standardize values and remove exact duplicates. |
| 3 | Lookup current dimension row by business key where `is_current = true`. |
| 4 | If the business key does not exist, insert a new current row. |
| 5 | If the business key exists and Type 2 tracked attributes did not change, do nothing or update audit metadata only. |
| 6 | If Type 2 tracked attributes changed, expire the current row by setting `effective_to` to the change timestamp and `is_current = false`. |
| 7 | Insert a new row with a new surrogate key, new attribute values, `effective_from` as change timestamp, `effective_to = 9999-12-31`, and `is_current = true`. |

## 6. Type 2 Attribute Classification

## 6.1 `dim_customer`

| Attribute | Handling | Reason |
|---|---|---|
| `full_name` | Type 1 or Type 2 | Usually correction; can be Type 1 unless business requires name history. |
| `gender` | Type 1 | Usually correction/static attribute. |
| `dob` | Type 1 | Usually correction/static attribute. |
| `phone_number` | Type 1 | Contact detail, usually overwritten. |
| `email` | Type 1 | Contact detail, usually overwritten. |
| `city` | Type 2 | Location affects customer/geography analysis. |
| `district` | Type 2 | Location affects customer/geography analysis. |

Recommended Sprint 1 simplification: track all customer descriptive changes as Type 2 except obvious technical corrections if identified.

## 6.2 `dim_agent`

| Attribute | Handling | Reason |
|---|---|---|
| `agent_name` | Type 1 | Usually correction. |
| `region` | Type 2 | Agent regional assignment affects performance analysis. |
| `branch` | Type 2 | Branch movement affects historical reporting. |
| `manager_name` | Type 2 | Manager assignment affects team performance analysis. |

## 6.3 `dim_provider`

| Attribute | Handling | Reason |
|---|---|---|
| `provider_name` | Type 1 | Usually correction or display change. |
| `provider_group` | Type 2 | Provider grouping affects historical provider performance. |
| `active_flag` | Type 2 | Active/inactive status should be preserved historically. |

## 6.4 `dim_vehicle`

| Attribute | Handling | Reason |
|---|---|---|
| `plate_number` | Type 1 | Usually correction or license plate transfer; can be Type 1 unless history is required. |
| `vehicle_brand` | Type 1 | Core manufacturer, does not change. |
| `vehicle_model` | Type 1 | Core model, does not change. |
| `manufacture_year` | Type 1 | Core specifications, does not change. |
| `vehicle_value` | Type 2 | Vehicle value changes and depreciates over time, affecting historical quotation or premium evaluation. |

## 7. Effective Date Rules

| Scenario | `effective_from` Rule |
|---|---|
| CRM insert | Use `created_date` if available, otherwise Gold load timestamp. |
| CRM update | Use `updated_date` if available, otherwise Gold load timestamp. |
| JSON insert/update event | Use `last_updated` if available, otherwise `batch_date` or Gold load timestamp. |
| Initial full load | Use source created date if available; otherwise use a default such as `1900-01-01` or the load timestamp depending on team standard. |

Recommended for this project:

```text
effective_from = COALESCE(source.updated_date, source.created_date, source.last_updated, gold_load_timestamp)
effective_to   = '9999-12-31'
is_current     = true
```

For expiring an old row:

```text
old.effective_to = new.effective_from
old.is_current   = false
```

## 8. Delete Handling

| Source Delete Pattern | Handling |
|---|---|
| Explicit delete event with `operation_type = 'D'` | Expire current Type 2 dimension row and set `is_deleted = true`. |
| No delete event available | Keep the current dimension row. Do not infer delete from absence in incremental load. |
| Reference dimension delete | Usually do not physically delete. Mark inactive or keep unchanged unless PO confirms. |

For source systems with delete events, deletion should be treated as a historical state rather than physical deletion from dimensions.

## 9. Late-Arriving Dimension Handling

If a fact arrives before the related dimension member exists:

| Step | Handling |
|---|---|
| 1 | Assign the fact foreign key to `-1` Unknown. |
| 2 | Log the unresolved business key as a data quality issue. |
| 3 | Once the dimension member arrives, optionally reprocess affected fact rows to resolve the correct key. |

## 10. Late-Arriving Fact Handling

If a fact arrives late and the related Type 2 dimension has multiple versions:

| Step | Handling |
|---|---|
| 1 | Use the fact business event date, not load date. |
| 2 | Resolve the dimension row where the fact event date falls between `effective_from` and `effective_to`. |
| 3 | If no valid version exists, use `-1` Unknown and log the issue. |

## 11. Dimension Change Detection Hash

For Type 2 dimensions, calculate a hash of tracked attributes.

Example:

```text
customer_scd_hash = hash(customer_id, city, district)
agent_scd_hash    = hash(agent_id, region, branch, manager_name)
provider_scd_hash = hash(provider_code, provider_group, active_flag)
vehicle_scd_hash  = hash(vehicle_id, vehicle_value)
```

Usage:

| Check | Action |
|---|---|
| Incoming business key not found | Insert new dimension row. |
| Incoming hash equals current hash | No Type 2 version change. |
| Incoming hash differs from current hash | Expire old row and insert new version. |

Each dimension should define its own tracked attribute list instead of using every source column blindly.

## 12. Out-of-Scope SCD Items

| Item | Handling |
|---|---|
| `dim_region` | Excluded from current scope as standalone dimension. Region/geography attributes are handled inside `dim_customer` and `dim_agent`. |

## 13. SCD Testing Checklist

| Test | Expected Result |
|---|---|
| New customer inserted | New `customer_key` generated with `is_current = true`. |
| Existing customer city changed | Old customer row expired; new `customer_key` inserted. |
| Existing agent branch changed | Old agent row expired; new `agent_key` inserted. |
| Provider active flag changed | Old provider row expired; new `provider_key` inserted. |
| Existing vehicle value changed | Old vehicle row expired; new `vehicle_key` inserted with updated value. |
| Type 1 payment method display name changed | Existing `payment_method_key` remains the same and attributes are overwritten. |
| Type 1 quotation status display name changed | Existing `quotation_status_key` remains the same and attributes are overwritten. |
| Fact lookup for old event date | Fact resolves to the historical dimension version valid on that event date. |
| Missing dimension member | Fact uses `-1` Unknown key and issue is logged. |

## 14. Output

This document is the output for **Task 85: Define SCD handling approach**.
