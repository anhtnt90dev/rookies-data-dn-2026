# Task 14: Define Dimension Table Structures

## 1. Purpose

This document defines the proposed Gold Layer dimension table structures for the Insurance Analytics dimensional model.

The design is based on the five agreed fact tables:

- `fact_quotation`
- `fact_quotation_item`
- `fact_policy`
- `fact_payment`
- `fact_cancellation`

This version is aligned with the updated source data, the fact grain document version 1.4, and the current Star Schema ERD review.

## 2. General Dimension Design Standards

| Rule | Standard |
|---|---|
| Naming convention | Dimension and fact table names use `lower_snake_case`, for example `dim_customer` and `fact_policy`. |
| Surrogate key | Every dimension has a surrogate primary key ending with `_key`. |
| Business key | Every source-based dimension keeps the original source business key. |
| Unknown member | Every dimension except `dim_date` should have an unknown/default row with key `-1`. |
| Audit columns | Dimensions should include source and load metadata where applicable. |
| SCD columns | Type 2 dimensions include effective dating and current flag columns. |
| Soft delete tracking | Source-based Type 2 dimensions may include `is_deleted` if delete detection is supported. |

## 3. Current Scope Decisions

| Item | Decision | Reason |
|---|---|---|
| `dim_vehicle` | Excluded from current Gold star schema. | The source has `vehicle.customer_id`, but `quotation` and `policy_info` do not contain `vehicle_id`. Vehicle analysis should wait for PO/client confirmation. |
| `dim_region` | Excluded as a standalone dimension. | Region/geography is available as attributes in `customers` and `agents`; no confirmed reporting-region mapping table exists. |
| `dim_policy` | Included. | Needed to connect `fact_policy`, `fact_payment`, and `fact_cancellation` without direct fact-to-fact relationships. |
| `dim_quotation_status` naming | Use `dim_quotation_status` instead of `dim_quote_status`. | This matches the source field name `quotation_status` and the Star Schema naming style. |

## 4. Common Technical Columns

### 4.1 Type 1 / Reference Dimension Common Columns

| Column | Description |
|---|---|
| `<dimension>_key` | Surrogate primary key. |
| `<business_key>` | Natural/business key from source or generated reference code. |
| Descriptive attributes | Business attributes used in filtering, grouping, and reporting. |
| `source_system` | Source system name, where applicable. |
| `created_at` | Date/time when the dimension row was created in Gold. |
| `updated_at` | Date/time when the dimension row was last updated in Gold. |

### 4.2 Type 2 Dimension Common Columns

| Column | Description |
|---|---|
| `<dimension>_key` | Surrogate primary key. New key is generated for each historical version. |
| `<business_key>` | Stable natural key from the source system. |
| Descriptive attributes | Attributes tracked either as Type 1 or Type 2 depending on business meaning. |
| `effective_from` | Start timestamp for the dimension version. |
| `effective_to` | End timestamp for the dimension version. Use `9999-12-31` for current row. |
| `is_current` | Indicates the current active version for a business key. |
| `is_deleted` | Indicates whether the source record has been deleted or no longer active, if supported. |
| `source_system` | Source system name. |
| `created_at` | Date/time when the dimension row was created in Gold. |
| `updated_at` | Date/time when the dimension row was last updated in Gold. |

## 5. Dimension Structures

## 5.1 `dim_date`

**Grain:** One row per calendar date  
**SCD Type:** No SCD  
**Source:** Generated calendar dimension

| Column | Type Suggestion | Description |
|---|---|---|
| `date_key` | INT | Surrogate key in `YYYYMMDD` format. |
| `full_date` | DATE | Calendar date. |
| `day_number` | INT | Day of month. |
| `day_name` | STRING | Day name. |
| `week_number` | INT | Week number. |
| `month_number` | INT | Month number. |
| `month_name` | STRING | Month name. |
| `quarter_number` | INT | Quarter number. |
| `year_number` | INT | Year. |
| `year_month` | STRING | Year-month label. |
| `is_weekend` | BOOLEAN | Weekend flag. |

## 5.2 `dim_customer`

**Grain:** One row per customer version  
**SCD Type:** Type 2  
**Source:** `customers`

| Column | Type Suggestion | Description |
|---|---|---|
| `customer_key` | BIGINT | Surrogate key. |
| `customer_id` | STRING | Source customer business key. |
| `full_name` | STRING | Customer full name. |
| `gender` | STRING | Customer gender. |
| `dob` | DATE | Date of birth. |
| `phone_number` | STRING | Customer phone number. |
| `email` | STRING | Customer email. |
| `city` | STRING | Customer city. |
| `district` | STRING | Customer district. |
| `effective_from` | TIMESTAMP | Version start timestamp. |
| `effective_to` | TIMESTAMP | Version end timestamp. |
| `is_current` | BOOLEAN | Current version flag. |
| `is_deleted` | BOOLEAN | Source deletion flag if available. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.3 `dim_agent`

**Grain:** One row per agent version  
**SCD Type:** Type 2  
**Source:** `agents`

| Column | Type Suggestion | Description |
|---|---|---|
| `agent_key` | BIGINT | Surrogate key. |
| `agent_id` | STRING | Source agent business key. |
| `agent_name` | STRING | Agent name. |
| `region` | STRING | Agent business region from source. |
| `branch` | STRING | Agent branch. |
| `manager_name` | STRING | Manager name. |
| `effective_from` | TIMESTAMP | Version start timestamp. |
| `effective_to` | TIMESTAMP | Version end timestamp. |
| `is_current` | BOOLEAN | Current version flag. |
| `is_deleted` | BOOLEAN | Source deletion flag if available. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.4 `dim_provider`

**Grain:** One row per provider version  
**SCD Type:** Type 2  
**Source:** `insurance_providers`, `policy_info.provider_code`

| Column | Type Suggestion | Description |
|---|---|---|
| `provider_key` | BIGINT | Surrogate key. |
| `provider_code` | STRING | Source provider business key. |
| `provider_name` | STRING | Provider name. |
| `provider_group` | STRING | Provider group, for example Domestic or International. |
| `active_flag` | INT | Source active flag. |
| `effective_from` | TIMESTAMP | Version start timestamp. |
| `effective_to` | TIMESTAMP | Version end timestamp. |
| `is_current` | BOOLEAN | Current version flag. |
| `is_deleted` | BOOLEAN | Source deletion flag if available. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.5 `dim_package`

**Grain:** One row per insurance package code  
**SCD Type:** Type 1  
**Source:** `quotation.package_code`

| Column | Type Suggestion | Description |
|---|---|---|
| `package_key` | BIGINT | Surrogate key. |
| `package_code` | STRING | Package business key from `quotation.package_code`, for example `BASIC`, `STANDARD`, `PREMIUM`, `VIP`. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |
## 5.6 `dim_coverage`

**Grain:** One row per coverage type  
**SCD Type:** Type 1  
**Source:** `quotation_item.coverage_type`

| Column | Type Suggestion | Description |
|---|---|---|
| `coverage_key` | BIGINT | Surrogate key. |
| `coverage_type` | STRING | Coverage type business value. |
| `coverage_group` | STRING | Optional coverage grouping. |
| `coverage_description` | STRING | Optional coverage description. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.7 `dim_quotation`

**Grain:** One row per quotation  
**SCD Type:** Type 1  
**Source:** `quotation`

| Column | Type Suggestion | Description |
|---|---|---|
| `quotation_key` | BIGINT | Surrogate key. |
| `quotation_id` | STRING | Source quotation business key. |
| `quotation_number` | STRING | Optional display identifier; can equal `quotation_id` if no separate number exists. |
| `quotation_expiry_date` | DATE/TIMESTAMP | Quotation expiry date. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.8 `dim_policy`

**Grain:** One row per policy  
**SCD Type:** Type 1  
**Source:** `policy_info`

| Column | Type Suggestion | Description |
|---|---|---|
| `policy_key` | BIGINT | Surrogate key. |
| `policy_id` | STRING | Source policy business key. |
| `policy_number` | STRING | Policy display number. |
| `quotation_id` | STRING | Related quotation ID for traceability. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.9 `dim_quotation_status`

**Grain:** One row per quotation status  
**SCD Type:** Type 1  
**Source:** `quotation.quotation_status`

| Column | Type Suggestion | Description |
|---|---|---|
| `quotation_status_key` | BIGINT | Surrogate key. |
| `quotation_status_code` | STRING | Status code, for example `QUOTED`, `ACCEPTED`, `REJECTED`, `EXPIRED`, `CONVERTED`. |
| `quotation_status_name` | STRING | Display name. |
| `is_open` | BOOLEAN | Whether the quotation is still open. |
| `is_accepted` | BOOLEAN | Whether the customer accepted the quotation. |
| `is_converted` | BOOLEAN | Whether the quotation converted to policy. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.10 `dim_policy_status`

**Grain:** One row per policy status  
**SCD Type:** Type 1  
**Source:** `policy_info.policy_status`

| Column | Type Suggestion | Description |
|---|---|---|
| `policy_status_key` | BIGINT | Surrogate key. |
| `policy_status_code` | STRING | Status code, for example `ISSUED`, `ACTIVE`, `EXPIRED`, `CANCELLED`. |
| `policy_status_name` | STRING | Display name. |
| `status_group` | STRING | Optional group such as Active, Closed, Cancelled. |
| `is_active_policy` | BOOLEAN | Whether status represents an active policy. |
| `is_terminal_status` | BOOLEAN | Whether status represents final lifecycle state. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.11 `dim_payment_status`

**Grain:** One row per payment status  
**SCD Type:** Type 1  
**Source:** `payment.payment_status`

| Column | Type Suggestion | Description |
|---|---|---|
| `payment_status_key` | BIGINT | Surrogate key. |
| `payment_status_code` | STRING | Status code, for example `PENDING`, `PAID`, `FAILED`, `REFUNDED`. |
| `payment_status_name` | STRING | Display name. |
| `status_group` | STRING | Optional group such as Successful, Failed, Pending, Refunded. |
| `is_successful_payment` | BOOLEAN | Whether payment is successful. |
| `is_refund_status` | BOOLEAN | Whether payment is refunded. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.12 `dim_payment_method`

**Grain:** One row per payment method  
**SCD Type:** Type 1  
**Source:** `payment.payment_method`

| Column | Type Suggestion | Description |
|---|---|---|
| `payment_method_key` | BIGINT | Surrogate key. |
| `payment_method_code` | STRING | Standardized method code. |
| `payment_method_name` | STRING | Payment method display name, for example Bank Transfer, Credit Card, E-wallet. |
| `payment_method_group` | STRING | Optional grouping. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5.13 `dim_cancellation_reason`

**Grain:** One row per distinct cancellation reason  
**SCD Type:** Type 1  
**Source:** `cancellation.cancellation_reason`

| Column | Type Suggestion | Description |
|---|---|---|
| `cancellation_reason_key` | BIGINT | Surrogate key. |
| `cancellation_reason_code` | STRING | Standardized cancellation reason code. |
| `cancellation_reason` | STRING | Cancellation reason value from source. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 6. Fact Foreign Key Expectations

| Fact Table | Expected Dimension Foreign Keys |
|---|---|
| `fact_quotation` | `quotation_key`, `quotation_date_key`, `quotation_expiry_date_key`, `customer_key`, `agent_key`, `provider_key`, `package_key`, `quotation_status_key` |
| `fact_quotation_item` | `quotation_key`, `quotation_date_key`, `customer_key`, `agent_key`, `provider_key`, `package_key`, `coverage_key`, `quotation_status_key` |
| `fact_policy` | `policy_key`, `quotation_key`, `issued_date_key`, `policy_start_date_key`, `policy_end_date_key`, `customer_key`, `agent_key`, `provider_key`, `package_key`, `policy_status_key` |
| `fact_payment` | `policy_key`, `payment_date_key`, `customer_key`, `provider_key`, `payment_status_key`, `payment_method_key` |
| `fact_cancellation` | `policy_key`, `cancellation_date_key`, `customer_key`, `provider_key`, `cancellation_reason_key` |

## 7. Review Points

| Topic | Review Required |
|---|---|
| `dim_policy` in ERD | Confirm that the ERD includes `dim_policy`, because it is needed to connect policy, payment, and cancellation facts without direct fact-to-fact relationships. |
| `dim_vehicle` | Confirm whether future vehicle analysis is required and whether quotation/policy should include `vehicle_id`. |
| Downstream inherited keys | Confirm whether payment and cancellation facts should physically store inherited `customer_key` and `provider_key`, or only rely on `policy_key`. |

## 8. Output

This document is the output for **Task 14: Define dimension table structures**.
