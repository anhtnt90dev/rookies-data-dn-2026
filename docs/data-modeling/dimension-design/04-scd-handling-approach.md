# Task 85: Define SCD Handling Approach

## 1. Purpose

This document defines the Slowly Changing Dimension handling approach for the Insurance Analytics Gold Layer.

The objective is to define which dimensions require historical tracking, which dimensions can be overwritten, and how dimension changes should be processed during incremental loads.

## 2. SCD Type Summary

| Dimension | SCD Type | Reason |
|---|---:|---|
| `Dim_Date` | No SCD | Calendar is generated and does not change historically. |
| `Dim_Customer` | Type 2 | Customer profile and geography may change. Historical reporting should preserve the customer attributes at the time of quotation, policy, payment, or cancellation. |
| `Dim_Vehicle` | Type 2 | Vehicle value and vehicle attributes may change. Historical reporting should preserve the vehicle context at event time. |
| `Dim_Agent` | Type 2 | Agent region, branch, or manager may change. Historical agent performance should be reported using the agent assignment valid at the event time. |
| `Dim_Provider` | Type 2 | Provider group or active flag may change. Historical provider reporting should preserve old provider attributes. |
| `Dim_Region` | Type 1 | Region is a standardized reference/mapping dimension. Corrections should overwrite previous values. |
| `Dim_Package` | Type 1 | Package code is currently a simple reference value. Changes are expected to be corrections or enrichments. |
| `Dim_Coverage` | Type 1 | Coverage type is a reference value. Changes are expected to be corrections or enrichments. |
| `Dim_Quotation` | Type 1 | Identifier dimension for grouping quotation header and item facts. Historical status is handled separately by facts/status dimension. |
| `Dim_Policy` | Type 1 | Identifier dimension for grouping policy, payment, and cancellation facts. Policy lifecycle is handled by `Fact_Policy` and `Dim_Policy_Status`. |
| `Dim_Quote_Status` | Type 1 | Status reference table. Business definition changes should overwrite or be managed as metadata. |
| `Dim_Policy_Status` | Type 1 | Status reference table. Business definition changes should overwrite or be managed as metadata. |
| `Dim_Payment_Status` | Type 1 | Status reference table. Business definition changes should overwrite or be managed as metadata. |
| `Dim_Payment_Method` | Type 1 | Payment method reference. Corrections/enrichments should overwrite. |
| `Dim_Cancellation_Reason` | Type 1 | Cancellation reason reference. Corrections/enrichments should overwrite. |

## 3. Source Change Detection Context

| Source Area | Change Tracking Field / Pattern | Usage |
|---|---|---|
| CRM SQL sources | `created_date`, `updated_date` | Used to identify new and updated CRM records for customer, agent, provider, vehicle, quotation, and quotation item related dimensions. |
| Policy JSON source | `operation_type`, `last_updated`, `batch_date`, `source_system` | Used to process insert/update/delete events for policy-related source data. |
| Payment JSON source | `operation_type`, `last_updated`, `batch_date`, `source_system` | Used to process insert/update events for payment-related reference dimensions and facts. |
| Cancellation JSON source | `operation_type`, `last_updated`, `batch_date`, `source_system` | Used to process insert/update events for cancellation-related reference dimensions and facts. |

## 4. Type 1 Handling Approach

Type 1 dimensions overwrite existing rows when source attributes change. They do not keep historical versions.

Applicable dimensions:

- `Dim_Region`
- `Dim_Package`
- `Dim_Coverage`
- `Dim_Quotation`
- `Dim_Policy`
- `Dim_Quote_Status`
- `Dim_Policy_Status`
- `Dim_Payment_Status`
- `Dim_Payment_Method`
- `Dim_Cancellation_Reason`

### Type 1 Processing Logic

| Step | Logic |
|---|---|
| 1 | Standardize and deduplicate incoming source values by business key. |
| 2 | Lookup existing dimension record by business key. |
| 3 | If business key does not exist, insert a new dimension row. |
| 4 | If business key exists and tracked attributes changed, update the existing row in place. |
| 5 | Update `updated_at` for changed records. |

### Type 1 Example

If payment method `E-wallet` is standardized to `E-Wallet`, update `Dim_Payment_Method` in place. Historical facts remain connected to the same `payment_method_key`.

## 5. Type 2 Handling Approach

Type 2 dimensions preserve historical versions when selected descriptive attributes change.

Applicable dimensions:

- `Dim_Customer`
- `Dim_Vehicle`
- `Dim_Agent`
- `Dim_Provider`

### Type 2 Required Columns

| Column | Description |
|---|---|
| Surrogate key | Unique key for each dimension version. |
| Business key | Stable source identifier, for example `customer_id` or `agent_id`. |
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

## 6.1 `Dim_Customer`

| Attribute | Handling | Reason |
|---|---|---|
| `full_name` | Type 1 or Type 2 | Usually correction; can be Type 1 unless business requires name history. |
| `gender` | Type 1 | Usually correction/static attribute. |
| `dob` | Type 1 | Usually correction/static attribute. |
| `phone_number` | Type 1 | Contact detail, usually overwritten. |
| `email` | Type 1 | Contact detail, usually overwritten. |
| `city` | Type 2 | Location affects regional/customer analysis. |
| `district` | Type 2 | Location affects regional/customer analysis. |

Recommended Sprint 1 simplification: track all customer descriptive changes as Type 2 except obvious technical corrections if identified.

## 6.2 `Dim_Vehicle`

| Attribute | Handling | Reason |
|---|---|---|
| `plate_number` | Type 1 or Type 2 | Usually stable, but can be preserved if changed. |
| `vehicle_brand` | Type 1 | Usually static/correction. |
| `vehicle_model` | Type 1 | Usually static/correction. |
| `manufacture_year` | Type 1 | Usually static/correction. |
| `vehicle_value` | Type 2 | Vehicle value may affect premium/risk analysis historically. |
| `customer_id` | Type 2 | Ownership/customer relationship change should be historically tracked if supported. |

## 6.3 `Dim_Agent`

| Attribute | Handling | Reason |
|---|---|---|
| `agent_name` | Type 1 | Usually correction. |
| `region` | Type 2 | Agent regional assignment affects performance analysis. |
| `branch` | Type 2 | Branch movement affects historical reporting. |
| `manager_name` | Type 2 | Manager assignment affects team performance analysis. |

## 6.4 `Dim_Provider`

| Attribute | Handling | Reason |
|---|---|---|
| `provider_name` | Type 1 | Usually correction or display change. |
| `provider_group` | Type 2 | Provider grouping affects historical provider performance. |
| `active_flag` | Type 2 | Active/inactive status should be preserved historically. |

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

For Type 2 dimensions, it is useful to calculate a hash of tracked attributes.

Example:

```text
scd_hash = hash(customer_id, city, district, vehicle_value, branch, manager_name, provider_group, active_flag)
```

Usage:

| Check | Action |
|---|---|
| Incoming business key not found | Insert new dimension row. |
| Incoming hash equals current hash | No Type 2 version change. |
| Incoming hash differs from current hash | Expire old row and insert new version. |

Each dimension should define its own tracked attribute list instead of using every source column blindly.

## 12. SCD Testing Checklist

| Test | Expected Result |
|---|---|
| New customer inserted | New `customer_key` generated with `is_current = true`. |
| Existing customer city changed | Old customer row expired; new `customer_key` inserted. |
| Existing agent branch changed | Old agent row expired; new `agent_key` inserted. |
| Provider active flag changed | Old provider row expired; new `provider_key` inserted. |
| Type 1 payment method display name changed | Existing `payment_method_key` remains the same and attributes are overwritten. |
| Fact lookup for old event date | Fact resolves to the historical dimension version valid on that event date. |
| Missing dimension member | Fact uses `-1` Unknown key and issue is logged. |

## 13. Output

This document is the output for **Task 85: Define SCD handling approach**.
