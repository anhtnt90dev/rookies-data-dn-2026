# Task 14: Define Dimension Table Structures

## 1. Purpose

This document defines the proposed Gold Layer dimension table structures for the Insurance Analytics dimensional model.

The design is based on the five agreed fact tables:

- `Fact_Quotation`
- `Fact_Quotation_Item`
- `Fact_Policy`
- `Fact_Payment`
- `Fact_Cancellation`

The goal is to define dimension structures that are reusable, consistent, and compatible with the Bus Matrix, surrogate key strategy, SCD approach, and Star Schema ERD.

## 2. General Dimension Design Standards

| Rule | Standard |
|---|---|
| Naming convention | Dimension tables use `Dim_<Entity>` format. Physical table names may use lowercase snake case such as `dim_customer`. |
| Surrogate key | Every dimension has a surrogate primary key ending with `_key`. |
| Business key | Every source-based dimension keeps the original source business key. |
| Unknown member | Every dimension except `Dim_Date` should have an unknown/default row with key `-1`. |
| Audit columns | Dimensions should include source and load metadata where applicable. |
| SCD columns | Type 2 dimensions include effective dating and current flag columns. |
| Soft delete tracking | Source-based Type 2 dimensions may include `is_deleted` if delete detection is supported. |

## 3. Common Technical Columns

### 3.1 Type 1 / Reference Dimension Common Columns

| Column | Description |
|---|---|
| `<dimension>_key` | Surrogate primary key. |
| `<business_key>` | Natural/business key from source or generated reference code. |
| descriptive attributes | Business attributes used in filtering, grouping, and reporting. |
| `source_system` | Source system name, where applicable. |
| `created_at` | Date/time when the dimension row was created in Gold. |
| `updated_at` | Date/time when the dimension row was last updated in Gold. |

### 3.2 Type 2 Dimension Common Columns

| Column | Description |
|---|---|
| `<dimension>_key` | Surrogate primary key. New key is generated for each historical version. |
| `<business_key>` | Stable natural key from the source system. |
| descriptive attributes | Attributes tracked either as Type 1 or Type 2 depending on business meaning. |
| `effective_from` | Start timestamp for the dimension version. |
| `effective_to` | End timestamp for the dimension version. Use `9999-12-31` for current record. |
| `is_current` | Indicates the current active version for a business key. |
| `is_deleted` | Indicates whether the source record has been deleted or no longer active, if supported. |
| `source_system` | Source system name. |
| `created_at` | Date/time when the dimension row was created in Gold. |
| `updated_at` | Date/time when the dimension row was last updated in Gold. |

## 4. Dimension Structures

## 4.1 `Dim_Date`

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

## 4.2 `Dim_Customer`

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
| `region_key` | BIGINT | Optional resolved region key. |
| `effective_from` | TIMESTAMP | Version start timestamp. |
| `effective_to` | TIMESTAMP | Version end timestamp. |
| `is_current` | BOOLEAN | Current version flag. |
| `is_deleted` | BOOLEAN | Source deletion flag if available. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.3 `Dim_Vehicle`

**Grain:** One row per vehicle version  
**SCD Type:** Type 2  
**Source:** `vehicle`  
**Assumption:** One customer has exactly one vehicle in project scope.

| Column | Type Suggestion | Description |
|---|---|---|
| `vehicle_key` | BIGINT | Surrogate key. |
| `vehicle_id` | STRING | Source vehicle business key. |
| `customer_id` | STRING | Source customer ID. |
| `plate_number` | STRING | Vehicle plate number. |
| `vehicle_brand` | STRING | Vehicle brand. |
| `vehicle_model` | STRING | Vehicle model. |
| `manufacture_year` | INT | Manufacture year. |
| `vehicle_value` | DECIMAL(18,2) | Vehicle value. |
| `effective_from` | TIMESTAMP | Version start timestamp. |
| `effective_to` | TIMESTAMP | Version end timestamp. |
| `is_current` | BOOLEAN | Current version flag. |
| `is_deleted` | BOOLEAN | Source deletion flag if available. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.4 `Dim_Agent`

**Grain:** One row per agent version  
**SCD Type:** Type 2  
**Source:** `agents`

| Column | Type Suggestion | Description |
|---|---|---|
| `agent_key` | BIGINT | Surrogate key. |
| `agent_id` | STRING | Source agent business key. |
| `agent_name` | STRING | Agent name. |
| `region` | STRING | Agent business region. |
| `branch` | STRING | Agent branch. |
| `manager_name` | STRING | Manager name. |
| `region_key` | BIGINT | Optional resolved region key. |
| `effective_from` | TIMESTAMP | Version start timestamp. |
| `effective_to` | TIMESTAMP | Version end timestamp. |
| `is_current` | BOOLEAN | Current version flag. |
| `is_deleted` | BOOLEAN | Source deletion flag if available. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.5 `Dim_Provider`

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

## 4.6 `Dim_Region`

**Grain:** One row per normalized reporting region  
**SCD Type:** Type 1  
**Source:** Derived from agent region and/or customer geography

| Column | Type Suggestion | Description |
|---|---|---|
| `region_key` | BIGINT | Surrogate key. |
| `region_code` | STRING | Standardized region code. |
| `region_name` | STRING | Standardized region name. |
| `city` | STRING | City if region is defined at city level. |
| `district` | STRING | District if required. |
| `region_group` | STRING | Optional group such as North, Central, South. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.7 `Dim_Package`

**Grain:** One row per insurance package code  
**SCD Type:** Type 1  
**Source:** `quotation.package_code`

| Column | Type Suggestion | Description |
|---|---|---|
| `package_key` | BIGINT | Surrogate key. |
| `package_code` | STRING | Package business key, for example `BASIC`, `STANDARD`, `PREMIUM`, `VIP`. |
| `package_name` | STRING | Display name. |
| `package_tier` | INT | Optional ordering tier. |
| `package_group` | STRING | Optional package group. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.8 `Dim_Coverage`

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

## 4.9 `Dim_Quotation`

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

## 4.10 `Dim_Policy`

**Grain:** One row per policy  
**SCD Type:** Type 1  
**Source:** `policy_info`

| Column | Type Suggestion | Description |
|---|---|---|
| `policy_key` | BIGINT | Surrogate key. |
| `policy_id` | STRING | Source policy business key. |
| `policy_number` | STRING | Policy number. |
| `quotation_id` | STRING | Related quotation ID. |
| `source_system` | STRING | Source system name. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.11 `Dim_Quote_Status`

**Grain:** One row per quotation status  
**SCD Type:** Type 1  
**Source:** `quotation.quotation_status`

| Column | Type Suggestion | Description |
|---|---|---|
| `quote_status_key` | BIGINT | Surrogate key. |
| `quote_status_code` | STRING | Status code, for example `QUOTED`, `ACCEPTED`, `REJECTED`, `EXPIRED`, `CONVERTED`. |
| `quote_status_name` | STRING | Display name. |
| `status_group` | STRING | Optional group such as Open, Won, Lost, Expired. |
| `is_open` | BOOLEAN | Whether the quotation is still open. |
| `is_accepted` | BOOLEAN | Whether the customer accepted the quotation. |
| `is_converted` | BOOLEAN | Whether the quotation converted to policy. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 4.12 `Dim_Policy_Status`

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

## 4.13 `Dim_Payment_Status`

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

## 4.14 `Dim_Payment_Method`

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

## 4.15 `Dim_Cancellation_Reason`

**Grain:** One row per cancellation reason  
**SCD Type:** Type 1  
**Source:** `cancellation.cancellation_reason`

| Column | Type Suggestion | Description |
|---|---|---|
| `cancellation_reason_key` | BIGINT | Surrogate key. |
| `cancellation_reason_code` | STRING | Standardized cancellation reason code. |
| `cancellation_reason_name` | STRING | Cancellation reason display name. |
| `reason_group` | STRING | Optional reason grouping. |
| `is_customer_initiated` | BOOLEAN | Optional flag for customer-driven cancellation. |
| `created_at` | TIMESTAMP | Gold row creation time. |
| `updated_at` | TIMESTAMP | Gold row update time. |

## 5. Fact Foreign Key Expectations

| Fact Table | Expected Dimension Foreign Keys |
|---|---|
| `Fact_Quotation` | `quotation_key`, `quotation_date_key`, `quotation_expiry_date_key`, `customer_key`, `vehicle_key`, `agent_key`, `provider_key`, `region_key`, `package_key`, `quote_status_key` |
| `Fact_Quotation_Item` | `quotation_key`, `quotation_date_key`, `customer_key`, `vehicle_key`, `agent_key`, `provider_key`, `region_key`, `package_key`, `coverage_key`, `quote_status_key` |
| `Fact_Policy` | `policy_key`, `quotation_key`, `issued_date_key`, `policy_start_date_key`, `policy_end_date_key`, `cancelled_date_key`, `customer_key`, `vehicle_key`, `agent_key`, `provider_key`, `region_key`, `package_key`, `policy_status_key` |
| `Fact_Payment` | `policy_key`, `payment_date_key`, `customer_key`, `vehicle_key`, `agent_key`, `provider_key`, `region_key`, `package_key`, `policy_status_key`, `payment_status_key`, `payment_method_key` |
| `Fact_Cancellation` | `policy_key`, `cancellation_date_key`, `customer_key`, `vehicle_key`, `agent_key`, `provider_key`, `region_key`, `package_key`, `policy_status_key`, `cancellation_reason_key` |

## 6. Review Points

| Topic | Review Required |
|---|---|
| `Dim_Region` | Confirm whether the official reporting region should follow agent region, customer city, or a standardized mapping table. |
| `Dim_Vehicle` | Confirm the project assumption that one customer has exactly one vehicle. |
| `Dim_Package` | Confirm whether package code is enough for Sprint 1 or whether a richer product dimension is required. |
| `Dim_Quotation` and `Dim_Policy` | Confirm that these identifier dimensions are acceptable for avoiding direct fact-to-fact relationships. |
| Downstream inherited keys | Confirm that policy/payment/cancellation facts may inherit agent, region, vehicle, and package context from quotation/policy joins. |

## 7. Output

This document is the output for **Task 14: Define dimension table structures**.
