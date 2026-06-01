# Silver to Gold Layer Column Mapping (Updated)
**Insurance Analytics - Dimensional Model (Star Schema)**

---

## 1. Purpose

This document defines the column-level mapping between the Silver layer (cleansed, typed, conformed tables) and the Gold layer (dimensional model). It aligns with the current Bronze-to-Silver mapping and naming conventions.

---

## 2. Naming Alignment and Assumptions

- Silver tables use Lakehouse prefix naming: `silver_<entity>` (if using schemas, replace with `silver.<entity>`).
- Timestamp columns end with `_at`; date-only columns end with `_date`.
- JSON-derived dates are already cast to `DATE` in Silver (e.g., `payment_date`, `cancellation_date`).
- Provider active flag is standardized as `is_active` (BOOLEAN) in Silver.
- Silver audit columns `_batch_id`, `_loaded_at`, `_source_system`, `_source_name` exist but only `_source_system` is carried into Gold.

---

## 3. General Transformation Standards

| Standard | Description |
| --- | --- |
| Type casting | Silver stores values in native types (STRING, INT, DECIMAL, DATE, TIMESTAMP). Gold casts only where noted. |
| Surrogate key resolution | Fact table FK columns are resolved by lookup against the corresponding dimension using business keys. |
| SCD Type 2 resolution | For Type 2 dimensions, facts resolve the correct version by matching event dates against `effective_from` and `effective_to`. |
| Unknown member default | If a lookup fails, the FK defaults to `-1` (Unknown). |
| Date key format | Date columns convert to integer `YYYYMMDD` for `dim_date` FK resolution. |
| Null handling on measures | `COALESCE(value, 0)` for all numeric measures. |
| Audit columns | `source_system`, `created_at`, `updated_at` are set in the Gold load step. |

---

## 4. Silver to Gold: Dimension Tables

### 4.1 `dim_date`

> **Source:** Generated calendar (no Silver source table)

| Gold Column | Type | Generation Rule |
| --- | --- | --- |
| `date_key` | INT | `FORMAT(calendar_date, 'YYYYMMDD')` |
| `full_date` | DATE | Calendar date |
| `day_number` | INT | `DAY(full_date)` |
| `day_name` | STRING | `DAYNAME(full_date)` |
| `week_number` | INT | `WEEKOFYEAR(full_date)` |
| `month_number` | INT | `MONTH(full_date)` |
| `month_name` | STRING | `MONTHNAME(full_date)` |
| `quarter_number` | INT | `QUARTER(full_date)` |
| `year_number` | INT | `YEAR(full_date)` |
| `year_month` | STRING | `FORMAT(full_date, 'YYYY-MM')` |
| `is_weekend` | BOOLEAN | `DAYOFWEEK(full_date) IN (1, 7)` |

---

### 4.2 `dim_customer`

> **Silver Source Table:** `silver_customer`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `customer_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Business key retained. |
| `full_name` | STRING | `full_name` | STRING | Direct mapping. |
| `gender` | STRING | `gender` | STRING | Direct mapping. |
| `dob` | DATE | `dob` | DATE | Direct mapping. |
| `phone_number` | STRING | `phone_number` | STRING | Direct mapping. |
| `email` | STRING | `email` | STRING | Direct mapping. |
| `city` | STRING | `city` | STRING | Direct mapping. |
| `district` | STRING | `district` | STRING | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.3 `dim_agent`

> **Silver Source Table:** `silver_agent`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `agent_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `agent_id` | STRING | `agent_id` | STRING | Direct mapping. |
| `agent_name` | STRING | `agent_name` | STRING | Direct mapping. |
| `region` | STRING | `region` | STRING | Direct mapping. |
| `branch` | STRING | `branch` | STRING | Direct mapping. |
| `manager_name` | STRING | `manager_name` | STRING | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.4 `dim_provider`

> **Silver Source Table:** `silver_provider`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `provider_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Business key retained. |
| `provider_name` | STRING | `provider_name` | STRING | Direct mapping. |
| `provider_group` | STRING | `provider_group` | STRING | Direct mapping. |
| `is_active` | BOOLEAN | `is_active` | BOOLEAN | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.5 `dim_package`

> **Silver Source Table:** `silver_quotation` (distinct `package_code`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `package_key` | BIGINT | System-generated surrogate key. |
| `package_code` | STRING | `package_code` | STRING | `DISTINCT package_code` from `silver_quotation`. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.6 `dim_coverage`

> **Silver Source Table:** `silver_quotation_item` (distinct `coverage_type`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `coverage_key` | BIGINT | System-generated surrogate key. |
| `coverage_type` | STRING | `coverage_type` | STRING | `DISTINCT coverage_type` from `silver_quotation_item`. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.7 `dim_quotation`

> **Silver Source Table:** `silver_quotation`
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `quotation_key` | BIGINT | System-generated surrogate key. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Business key. |
| `quotation_id` | STRING | `quotation_number` | STRING | Direct mapping. Equals `quotation_id` if no display number exists. |
| `quotation_expiry_at` | TIMESTAMP | `quotation_expiry_date` | DATE | Cast `TIMESTAMP` to `DATE`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.8 `dim_policy`

> **Silver Source Table:** `silver_policy`
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `policy_key` | BIGINT | System-generated surrogate key. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Business key. |
| `policy_number` | STRING | `policy_number` | STRING | Direct mapping. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. |
| `policy_start_date` | DATE | `policy_start_date` | DATE | Direct mapping. |
| `policy_end_date` | DATE | `policy_end_date` | DATE | Direct mapping. |
| `premium_amount` | DECIMAL(18,2) | `premium_amount` | DECIMAL(18,2) | Direct mapping. |
| `issued_at` | TIMESTAMP | `issued_at` | TIMESTAMP | Direct mapping. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.9 `dim_quotation_status`

> **Silver Source Table:** `silver_quotation` (distinct `quotation_status`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `quotation_status_key` | BIGINT | System-generated surrogate key. |
| `quotation_status` | STRING | `quotation_status_code` | STRING | Direct mapping. |
| *(derived)* | - | `quotation_status_name` | STRING | `INITCAP(quotation_status_code)` |
| *(derived)* | - | `is_open` | BOOLEAN | `quotation_status_code IN ('QUOTED', 'ACCEPTED')` |
| *(derived)* | - | `is_accepted` | BOOLEAN | `quotation_status_code IN ('ACCEPTED', 'CONVERTED')` |
| *(derived)* | - | `is_converted` | BOOLEAN | `quotation_status_code = 'CONVERTED'` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.10 `dim_policy_status`

> **Silver Source Table:** `silver_policy` (distinct `policy_status`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `policy_status_key` | BIGINT | System-generated surrogate key. |
| `policy_status` | STRING | `policy_status_code` | STRING | Direct mapping. |
| *(derived)* | - | `policy_status_name` | STRING | `INITCAP(policy_status_code)` |
| *(derived)* | - | `status_group` | STRING | `CASE WHEN code IN ('ISSUED','ACTIVE') THEN 'Active' WHEN code = 'EXPIRED' THEN 'Closed' WHEN code = 'CANCELLED' THEN 'Cancelled' END` |
| *(derived)* | - | `is_active_policy` | BOOLEAN | `policy_status_code = 'ACTIVE'` |
| *(derived)* | - | `is_terminal_status` | BOOLEAN | `policy_status_code IN ('EXPIRED', 'CANCELLED')` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.11 `dim_payment_status`

> **Silver Source Table:** `silver_payment` (distinct `payment_status`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `payment_status_key` | BIGINT | System-generated surrogate key. |
| `payment_status` | STRING | `payment_status_code` | STRING | Direct mapping. |
| *(derived)* | - | `payment_status_name` | STRING | `INITCAP(payment_status_code)` |
| *(derived)* | - | `status_group` | STRING | `CASE WHEN code = 'PENDING' THEN 'Pending' WHEN code = 'PAID' THEN 'Successful' WHEN code = 'FAILED' THEN 'Failed' WHEN code = 'REFUNDED' THEN 'Refunded' END` |
| *(derived)* | - | `is_successful_payment` | BOOLEAN | `payment_status_code = 'PAID'` |
| *(derived)* | - | `is_refund_status` | BOOLEAN | `payment_status_code = 'REFUNDED'` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.12 `dim_payment_method`

> **Silver Source Table:** `silver_payment` (distinct `payment_method`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `payment_method_key` | BIGINT | System-generated surrogate key. |
| `payment_method` | STRING | `payment_method_code` | STRING | Standardize raw values: `Bank Transfer -> BANK_TRANSFER`, `Credit Card -> CREDIT_CARD`, `E-wallet -> E_WALLET`. |
| *(derived)* | - | `payment_method_name` | STRING | `CASE WHEN code = 'BANK_TRANSFER' THEN 'Bank Transfer' WHEN code = 'CREDIT_CARD' THEN 'Credit Card' WHEN code = 'E_WALLET' THEN 'E-wallet' END` |
| *(derived)* | - | `payment_method_group` | STRING | `CASE WHEN code = 'BANK_TRANSFER' THEN 'Offline/Direct' WHEN code = 'CREDIT_CARD' THEN 'Card' WHEN code = 'E_WALLET' THEN 'Digital' END` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.13 `dim_cancellation_reason`

> **Silver Source Table:** `silver_cancellation` (distinct `cancellation_reason`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `cancellation_reason_key` | BIGINT | System-generated surrogate key. |
| `cancellation_reason` | STRING | `cancellation_reason` | STRING | Direct mapping. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.14 `dim_vehicle`

> **Silver Source Table:** `silver_vehicle`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `vehicle_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `vehicle_id` | STRING | `vehicle_id` | STRING | Direct mapping. Business key. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. |
| `plate_number` | STRING | `plate_number` | STRING | Direct mapping. |
| `vehicle_brand` | STRING | `vehicle_brand` | STRING | Direct mapping. |
| `vehicle_model` | STRING | `vehicle_model` | STRING | Direct mapping. |
| `manufacture_year` | INT | `manufacture_year` | INT | Direct mapping. |
| `vehicle_value` | DECIMAL(18,2) | `vehicle_value` | DECIMAL(18,2) | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

## 5. Silver to Gold: Fact Tables

### 5.1 `fact_quotation`

> **Silver Source Table:** `silver_quotation`
> **Grain:** One row per quotation

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| `customer_id` | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `agent_id` | STRING | `agent_key` | BIGINT | Lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `provider_code` | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `package_code` | STRING | `package_key` | BIGINT | Lookup `dim_package` by `package_code`. |
| `quotation_status` | STRING | `quotation_status_key` | BIGINT | Lookup `dim_quotation_status` by `quotation_status_code`. |
| `quotation_at` | TIMESTAMP | `quotation_date_key` | INT | `CAST(FORMAT(quotation_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `quotation_expiry_at` | TIMESTAMP | `quotation_expiry_date_key` | INT | `CAST(FORMAT(quotation_expiry_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Degenerate dimension. |
| `agent_id` | STRING | `agent_id` | STRING | Direct mapping. Degenerate dimension. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Degenerate dimension. |
| `premium_amount` | DECIMAL(18,2) | `premium_amount` | DECIMAL(18,2) | `COALESCE(premium_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `silver_quotation.customer_id -> silver_vehicle.customer_id` (1-to-1 assumption), then lookup `dim_vehicle` by `vehicle_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. Default `-1` if not found.

---

### 5.2 `fact_quotation_item`

> **Silver Source Tables:** `silver_quotation_item` (primary), `silver_quotation` (header context)
> **Grain:** One row per coverage line item within a quotation

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `quotation_item_id` | STRING | `quotation_item_id` | STRING | Direct mapping. Degenerate dimension. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| *(via join to quotation)* | STRING | `quotation_key` | BIGINT | Join `quotation_item.quotation_id -> quotation.quotation_id`, then lookup `dim_quotation`. |
| *(via join to quotation)* | TIMESTAMP | `quotation_date_key` | INT | From `quotation.quotation_at` -> date key. |
| *(via join to quotation)* | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `agent_key` | BIGINT | Lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `package_key` | BIGINT | Lookup `dim_package` by `package_code`. |
| *(via join to quotation)* | STRING | `quotation_status_key` | BIGINT | Lookup `dim_quotation_status` by `quotation_status_code`. |
| `coverage_type` | STRING | `coverage_key` | BIGINT | Lookup `dim_coverage` by `coverage_type`. |
| `coverage_amount` | DECIMAL(18,2) | `coverage_amount` | DECIMAL(18,2) | `COALESCE(coverage_amount, 0)`. |
| `deductible_amount` | DECIMAL(18,2) | `deductible_amount` | DECIMAL(18,2) | `COALESCE(deductible_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Inherit `customer_id` from joined quotation, join to `silver_vehicle.customer_id`, then lookup `dim_vehicle` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`.

---

### 5.3 `fact_policy`

> **Silver Source Tables:** `silver_policy` (primary), `silver_quotation` (agent and package context)
> **Grain:** One row per issued policy

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_number` | STRING | `policy_number` | STRING | Direct mapping. Degenerate dimension. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Degenerate dimension. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `quotation_id` | STRING | `quotation_key` | BIGINT | Lookup `dim_quotation` by `quotation_id`. |
| `customer_id` | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`. |
| `provider_code` | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `agent_key` | BIGINT | Join `policy.quotation_id -> quotation.quotation_id`, then lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `package_key` | BIGINT | Join `policy.quotation_id -> quotation.quotation_id`, then lookup `dim_package` by `package_code`. |
| `policy_status` | STRING | `policy_status_key` | BIGINT | Lookup `dim_policy_status` by `policy_status_code`. |
| `issued_at` | TIMESTAMP | `issued_date_key` | INT | `CAST(FORMAT(issued_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `policy_start_date` | DATE | `policy_start_date_key` | INT | `CAST(FORMAT(policy_start_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `policy_end_date` | DATE | `policy_end_date_key` | INT | `CAST(FORMAT(policy_end_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `premium_amount` | DECIMAL(18,2) | `issued_premium_amount` | DECIMAL(18,2) | `COALESCE(premium_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `policy.customer_id -> silver_vehicle.customer_id`, then lookup `dim_vehicle` by `vehicle_id` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`.

---

### 5.4 `fact_payment`

> **Silver Source Tables:** `silver_payment` (primary), `silver_policy` (context)
> **Grain:** One row per payment transaction

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `payment_id` | STRING | `payment_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `transaction_reference` | STRING | `transaction_reference` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `payment_status` | STRING | `payment_status_key` | BIGINT | Lookup `dim_payment_status` by `payment_status_code`. |
| `payment_method` | STRING | `payment_method_key` | BIGINT | Standardize -> lookup `dim_payment_method` by `payment_method_code`. |
| `payment_date` | DATE | `payment_date_key` | INT | `CAST(FORMAT(payment_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| *(via join to policy)* | STRING | `customer_key` | BIGINT | Join `payment.policy_id -> policy.policy_id`, lookup `dim_customer` WHERE `payment_date` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to policy)* | STRING | `provider_key` | BIGINT | Join `payment.policy_id -> policy.policy_id`, lookup `dim_provider` WHERE `payment_date` BETWEEN `effective_from` AND `effective_to`. |
| `payment_amount` | DECIMAL(18,2) | `payment_amount` | DECIMAL(18,2) | `COALESCE(payment_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `payment.policy_id -> policy.policy_id -> silver_vehicle.customer_id`, then lookup `dim_vehicle` WHERE `payment_date` BETWEEN `effective_from` AND `effective_to`.

---

### 5.5 `fact_cancellation`

> **Silver Source Tables:** `silver_cancellation` (primary), `silver_policy` (context)
> **Grain:** One row per policy cancellation event

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `cancellation_id` | STRING | `cancellation_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `cancellation_reason` | STRING | `cancellation_reason_key` | BIGINT | Lookup `dim_cancellation_reason` by `cancellation_reason`. |
| `cancellation_date` | DATE | `cancellation_date_key` | INT | `CAST(FORMAT(cancellation_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| *(via join to policy)* | STRING | `customer_key` | BIGINT | Join `cancellation.policy_id -> policy.policy_id`, lookup `dim_customer` WHERE `cancellation_date` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to policy)* | STRING | `provider_key` | BIGINT | Join `cancellation.policy_id -> policy.policy_id`, lookup `dim_provider` WHERE `cancellation_date` BETWEEN `effective_from` AND `effective_to`. |
| `refund_amount` | DECIMAL(18,2) | `refund_amount` | DECIMAL(18,2) | `COALESCE(refund_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `cancellation.policy_id -> policy.policy_id -> silver_vehicle.customer_id`, then lookup `dim_vehicle` WHERE `cancellation_date` BETWEEN `effective_from` AND `effective_to`.

---

## 6. Silver Table Dependency Summary

| Gold Table | Primary Silver Source | Secondary Silver Sources |
| --- | --- | --- |
| `dim_date` | *(generated)* | - |
| `dim_customer` | `silver_customer` | - |
| `dim_agent` | `silver_agent` | - |
| `dim_provider` | `silver_provider` | - |
| `dim_package` | `silver_quotation` | - |
| `dim_coverage` | `silver_quotation_item` | - |
| `dim_quotation` | `silver_quotation` | - |
| `dim_policy` | `silver_policy` | - |
| `dim_quotation_status` | `silver_quotation` | - |
| `dim_policy_status` | `silver_policy` | - |
| `dim_payment_status` | `silver_payment` | - |
| `dim_payment_method` | `silver_payment` | - |
| `dim_cancellation_reason` | `silver_cancellation` | - |
| `dim_vehicle` | `silver_vehicle` | - |
| `fact_quotation` | `silver_quotation` | `silver_vehicle` |
| `fact_quotation_item` | `silver_quotation_item` | `silver_quotation`, `silver_vehicle` |
| `fact_policy` | `silver_policy` | `silver_quotation`, `silver_vehicle` |
| `fact_payment` | `silver_payment` | `silver_policy`, `silver_vehicle` |
| `fact_cancellation` | `silver_cancellation` | `silver_policy`, `silver_vehicle` |

---

## 7. Unknown Member Reference

All dimension lookups that fail to resolve must default to the unknown member row.

| Dimension | Unknown Key | Unknown Business Key | Notes |
| --- | --- | --- | --- |
| `dim_customer` | `-1` | `UNKNOWN` | Applied when `customer_id` is missing or not found. |
| `dim_agent` | `-1` | `UNKNOWN` | Applied when `agent_id` is missing. |
| `dim_provider` | `-1` | `UNKNOWN` | Applied when `provider_code` is missing or not found. |
| `dim_package` | `-1` | `UNKNOWN` | Applied when `package_code` is missing or not found. |
| `dim_coverage` | `-1` | `UNKNOWN` | Applied when `coverage_type` is missing or unmatched. |
| `dim_quotation` | `-1` | `UNKNOWN` | Applied when `quotation_id` is null or not found. |
| `dim_policy` | `-1` | `UNKNOWN` | Applied when `policy_id` is not found. |
| `dim_quotation_status` | `-1` | `UNKNOWN` | Applied when status code is not in reference set. |
| `dim_policy_status` | `-1` | `UNKNOWN` | Applied when status code is not in reference set. |
| `dim_payment_status` | `-1` | `UNKNOWN` | Applied when status code is not in reference set. |
| `dim_payment_method` | `-1` | `UNKNOWN` | Applied when method cannot be standardized or matched. |
| `dim_cancellation_reason` | `-1` | `UNKNOWN` | Applied when reason is null or unmatched. |
| `dim_vehicle` | `-1` | `UNKNOWN` | Applied when no vehicle can be resolved from customer context. |

---

## 8. Concerns and Decisions to Confirm

- Confirm Silver naming style to use everywhere: `silver_<entity>` (Lakehouse) or `silver.<entity>` (Warehouse).
- Confirm whether JSON fields `last_updated_at` should be normalized to `updated_at` for Silver policy, payment, and cancellation.
- Confirm the 1-to-1 assumption between customer and vehicle; if 1-to-many, add a bridge and update vehicle key resolution rules.
- Confirm whether `payment_date` and `cancellation_date` should be kept as `DATE` or promoted to `TIMESTAMP`.
- Confirm which date should drive SCD2 resolution for `dim_customer` and `dim_provider` in facts (event date vs issued date).

---

## 9. Revision History

| Version | Date | Author | Notes |
| --- | --- | --- | --- |
| 1.1 | 2026-06-01 | Data Engineering Team | Updated Silver to Gold mapping to match naming conventions and the Bronze-to-Silver spec. |
# Silver to Gold Layer Column Mapping (Updated)
**Insurance Analytics - Dimensional Model (Star Schema)**

---

## 1. Purpose

This document defines the column-level mapping between the Silver layer (cleansed, typed, conformed tables) and the Gold layer (dimensional model). It aligns with the current Bronze-to-Silver mapping and naming conventions.

---

## 2. Naming Alignment and Assumptions

- Silver tables use Lakehouse prefix naming: `silver_<entity>` (if using schemas, replace with `silver.<entity>`).
- Timestamp columns end with `_at`; date-only columns end with `_date`.
- JSON-derived dates are already cast to `DATE` in Silver (e.g., `payment_date`, `cancellation_date`).
- Provider active flag is standardized as `is_active` (BOOLEAN) in Silver.
- Silver audit columns `_batch_id`, `_loaded_at`, `_source_system`, `_source_name` exist but only `_source_system` is carried into Gold.

---

## 3. General Transformation Standards

| Standard | Description |
| --- | --- |
| Type casting | Silver stores values in native types (STRING, INT, DECIMAL, DATE, TIMESTAMP). Gold casts only where noted. |
| Surrogate key resolution | Fact table FK columns are resolved by lookup against the corresponding dimension using business keys. |
| SCD Type 2 resolution | For Type 2 dimensions, facts resolve the correct version by matching event dates against `effective_from` and `effective_to`. |
| Unknown member default | If a lookup fails, the FK defaults to `-1` (Unknown). |
| Date key format | Date columns convert to integer `YYYYMMDD` for `dim_date` FK resolution. |
| Null handling on measures | `COALESCE(value, 0)` for all numeric measures. |
| Audit columns | `source_system`, `created_at`, `updated_at` are set in Gold load step. |

---

## 4. Silver to Gold: Dimension Tables

### 4.1 `dim_date`

> **Source:** Generated calendar (no Silver source table)

| Gold Column | Type | Generation Rule |
| --- | --- | --- |
| `date_key` | INT | `FORMAT(calendar_date, 'YYYYMMDD')` |
| `full_date` | DATE | Calendar date |
| `day_number` | INT | `DAY(full_date)` |
| `day_name` | STRING | `DAYNAME(full_date)` |
| `week_number` | INT | `WEEKOFYEAR(full_date)` |
| `month_number` | INT | `MONTH(full_date)` |
| `month_name` | STRING | `MONTHNAME(full_date)` |
| `quarter_number` | INT | `QUARTER(full_date)` |
| `year_number` | INT | `YEAR(full_date)` |
| `year_month` | STRING | `FORMAT(full_date, 'YYYY-MM')` |
| `is_weekend` | BOOLEAN | `DAYOFWEEK(full_date) IN (1, 7)` |

---

### 4.2 `dim_customer`

> **Silver Source Table:** `silver_customer`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `customer_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Business key retained. |
| `full_name` | STRING | `full_name` | STRING | Direct mapping. |
| `gender` | STRING | `gender` | STRING | Direct mapping. |
| `dob` | DATE | `dob` | DATE | Direct mapping. |
| `phone_number` | STRING | `phone_number` | STRING | Direct mapping. |
| `email` | STRING | `email` | STRING | Direct mapping. |
| `city` | STRING | `city` | STRING | Direct mapping. |
| `district` | STRING | `district` | STRING | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.3 `dim_agent`

> **Silver Source Table:** `silver_agent`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `agent_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `agent_id` | STRING | `agent_id` | STRING | Direct mapping. |
| `agent_name` | STRING | `agent_name` | STRING | Direct mapping. |
| `region` | STRING | `region` | STRING | Direct mapping. |
| `branch` | STRING | `branch` | STRING | Direct mapping. |
| `manager_name` | STRING | `manager_name` | STRING | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.4 `dim_provider`

> **Silver Source Table:** `silver_provider`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `provider_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Business key retained. |
| `provider_name` | STRING | `provider_name` | STRING | Direct mapping. |
| `provider_group` | STRING | `provider_group` | STRING | Direct mapping. |
| `is_active` | BOOLEAN | `is_active` | BOOLEAN | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.5 `dim_package`

> **Silver Source Table:** `silver_quotation` (distinct `package_code`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `package_key` | BIGINT | System-generated surrogate key. |
| `package_code` | STRING | `package_code` | STRING | `DISTINCT package_code` from `silver_quotation`. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.6 `dim_coverage`

> **Silver Source Table:** `silver_quotation_item` (distinct `coverage_type`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `coverage_key` | BIGINT | System-generated surrogate key. |
| `coverage_type` | STRING | `coverage_type` | STRING | `DISTINCT coverage_type` from `silver_quotation_item`. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.7 `dim_quotation`

> **Silver Source Table:** `silver_quotation`
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `quotation_key` | BIGINT | System-generated surrogate key. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Business key. |
| `quotation_id` | STRING | `quotation_number` | STRING | Direct mapping. Equals `quotation_id` if no display number exists. |
| `quotation_expiry_at` | TIMESTAMP | `quotation_expiry_date` | DATE | Cast `TIMESTAMP` to `DATE`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.8 `dim_policy`

> **Silver Source Table:** `silver_policy`
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `policy_key` | BIGINT | System-generated surrogate key. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Business key. |
| `policy_number` | STRING | `policy_number` | STRING | Direct mapping. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. |
| `policy_start_date` | DATE | `policy_start_date` | DATE | Direct mapping. |
| `policy_end_date` | DATE | `policy_end_date` | DATE | Direct mapping. |
| `premium_amount` | DECIMAL(18,2) | `premium_amount` | DECIMAL(18,2) | Direct mapping. |
| `issued_at` | TIMESTAMP | `issued_at` | TIMESTAMP | Direct mapping. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.9 `dim_quotation_status`

> **Silver Source Table:** `silver_quotation` (distinct `quotation_status`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `quotation_status_key` | BIGINT | System-generated surrogate key. |
| `quotation_status` | STRING | `quotation_status_code` | STRING | Direct mapping. |
| *(derived)* | - | `quotation_status_name` | STRING | `INITCAP(quotation_status_code)` |
| *(derived)* | - | `is_open` | BOOLEAN | `quotation_status_code IN ('QUOTED', 'ACCEPTED')` |
| *(derived)* | - | `is_accepted` | BOOLEAN | `quotation_status_code IN ('ACCEPTED', 'CONVERTED')` |
| *(derived)* | - | `is_converted` | BOOLEAN | `quotation_status_code = 'CONVERTED'` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.10 `dim_policy_status`

> **Silver Source Table:** `silver_policy` (distinct `policy_status`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `policy_status_key` | BIGINT | System-generated surrogate key. |
| `policy_status` | STRING | `policy_status_code` | STRING | Direct mapping. |
| *(derived)* | - | `policy_status_name` | STRING | `INITCAP(policy_status_code)` |
| *(derived)* | - | `status_group` | STRING | `CASE WHEN code IN ('ISSUED','ACTIVE') THEN 'Active' WHEN code = 'EXPIRED' THEN 'Closed' WHEN code = 'CANCELLED' THEN 'Cancelled' END` |
| *(derived)* | - | `is_active_policy` | BOOLEAN | `policy_status_code = 'ACTIVE'` |
| *(derived)* | - | `is_terminal_status` | BOOLEAN | `policy_status_code IN ('EXPIRED', 'CANCELLED')` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.11 `dim_payment_status`

> **Silver Source Table:** `silver_payment` (distinct `payment_status`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `payment_status_key` | BIGINT | System-generated surrogate key. |
| `payment_status` | STRING | `payment_status_code` | STRING | Direct mapping. |
| *(derived)* | - | `payment_status_name` | STRING | `INITCAP(payment_status_code)` |
| *(derived)* | - | `status_group` | STRING | `CASE WHEN code = 'PENDING' THEN 'Pending' WHEN code = 'PAID' THEN 'Successful' WHEN code = 'FAILED' THEN 'Failed' WHEN code = 'REFUNDED' THEN 'Refunded' END` |
| *(derived)* | - | `is_successful_payment` | BOOLEAN | `payment_status_code = 'PAID'` |
| *(derived)* | - | `is_refund_status` | BOOLEAN | `payment_status_code = 'REFUNDED'` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.12 `dim_payment_method`

> **Silver Source Table:** `silver_payment` (distinct `payment_method`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `payment_method_key` | BIGINT | System-generated surrogate key. |
| `payment_method` | STRING | `payment_method_code` | STRING | Standardize raw values: `Bank Transfer -> BANK_TRANSFER`, `Credit Card -> CREDIT_CARD`, `E-wallet -> E_WALLET`. |
| *(derived)* | - | `payment_method_name` | STRING | `CASE WHEN code = 'BANK_TRANSFER' THEN 'Bank Transfer' WHEN code = 'CREDIT_CARD' THEN 'Credit Card' WHEN code = 'E_WALLET' THEN 'E-wallet' END` |
| *(derived)* | - | `payment_method_group` | STRING | `CASE WHEN code = 'BANK_TRANSFER' THEN 'Offline/Direct' WHEN code = 'CREDIT_CARD' THEN 'Card' WHEN code = 'E_WALLET' THEN 'Digital' END` |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.13 `dim_cancellation_reason`

> **Silver Source Table:** `silver_cancellation` (distinct `cancellation_reason`)
> **SCD Type:** Type 1

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `cancellation_reason_key` | BIGINT | System-generated surrogate key. |
| `cancellation_reason` | STRING | `cancellation_reason` | STRING | Direct mapping. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

### 4.14 `dim_vehicle`

> **Silver Source Table:** `silver_vehicle`
> **SCD Type:** Type 2

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| *(pipeline generated)* | - | `vehicle_key` | BIGINT | System-generated surrogate key. New key per SCD2 version. |
| `vehicle_id` | STRING | `vehicle_id` | STRING | Direct mapping. Business key. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. |
| `plate_number` | STRING | `plate_number` | STRING | Direct mapping. |
| `vehicle_brand` | STRING | `vehicle_brand` | STRING | Direct mapping. |
| `vehicle_model` | STRING | `vehicle_model` | STRING | Direct mapping. |
| `manufacture_year` | INT | `manufacture_year` | INT | Direct mapping. |
| `vehicle_value` | DECIMAL(18,2) | `vehicle_value` | DECIMAL(18,2) | Direct mapping. |
| *(SCD logic)* | - | `effective_from` | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row. |
| *(SCD logic)* | - | `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| *(SCD logic)* | - | `is_current` | BOOLEAN | `true` for latest version. |
| *(SCD logic)* | - | `is_deleted` | BOOLEAN | `true` if record no longer present in Silver. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

---

## 5. Silver to Gold: Fact Tables

### 5.1 `fact_quotation`

> **Silver Source Table:** `silver_quotation`
> **Grain:** One row per quotation

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| `customer_id` | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `agent_id` | STRING | `agent_key` | BIGINT | Lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `provider_code` | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `package_code` | STRING | `package_key` | BIGINT | Lookup `dim_package` by `package_code`. |
| `quotation_status` | STRING | `quotation_status_key` | BIGINT | Lookup `dim_quotation_status` by `quotation_status_code`. |
| `quotation_at` | TIMESTAMP | `quotation_date_key` | INT | `CAST(FORMAT(quotation_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `quotation_expiry_at` | TIMESTAMP | `quotation_expiry_date_key` | INT | `CAST(FORMAT(quotation_expiry_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Degenerate dimension. |
| `agent_id` | STRING | `agent_id` | STRING | Direct mapping. Degenerate dimension. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Degenerate dimension. |
| `premium_amount` | DECIMAL(18,2) | `premium_amount` | DECIMAL(18,2) | `COALESCE(premium_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `silver_quotation.customer_id -> silver_vehicle.customer_id` (1-to-1 assumption), then lookup `dim_vehicle` by `vehicle_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. Default `-1` if not found.

---

### 5.2 `fact_quotation_item`

> **Silver Source Tables:** `silver_quotation_item` (primary), `silver_quotation` (header context)
> **Grain:** One row per coverage line item within a quotation

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `quotation_item_id` | STRING | `quotation_item_id` | STRING | Direct mapping. Degenerate dimension. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| *(via join to quotation)* | STRING | `quotation_key` | BIGINT | Join `quotation_item.quotation_id -> quotation.quotation_id`, then lookup `dim_quotation`. |
| *(via join to quotation)* | TIMESTAMP | `quotation_date_key` | INT | From `quotation.quotation_at` -> date key. |
| *(via join to quotation)* | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `agent_key` | BIGINT | Lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `package_key` | BIGINT | Lookup `dim_package` by `package_code`. |
| *(via join to quotation)* | STRING | `quotation_status_key` | BIGINT | Lookup `dim_quotation_status` by `quotation_status_code`. |
| `coverage_type` | STRING | `coverage_key` | BIGINT | Lookup `dim_coverage` by `coverage_type`. |
| `coverage_amount` | DECIMAL(18,2) | `coverage_amount` | DECIMAL(18,2) | `COALESCE(coverage_amount, 0)`. |
| `deductible_amount` | DECIMAL(18,2) | `deductible_amount` | DECIMAL(18,2) | `COALESCE(deductible_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Inherit `customer_id` from joined quotation, join to `silver_vehicle.customer_id`, then lookup `dim_vehicle` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`.

---

### 5.3 `fact_policy`

> **Silver Source Tables:** `silver_policy` (primary), `silver_quotation` (agent and package context)
> **Grain:** One row per issued policy

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_number` | STRING | `policy_number` | STRING | Direct mapping. Degenerate dimension. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Degenerate dimension. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `quotation_id` | STRING | `quotation_key` | BIGINT | Lookup `dim_quotation` by `quotation_id`. |
| `customer_id` | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`. |
| `provider_code` | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `agent_key` | BIGINT | Join `policy.quotation_id -> quotation.quotation_id`, then lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to quotation)* | STRING | `package_key` | BIGINT | Join `policy.quotation_id -> quotation.quotation_id`, then lookup `dim_package` by `package_code`. |
| `policy_status` | STRING | `policy_status_key` | BIGINT | Lookup `dim_policy_status` by `policy_status_code`. |
| `issued_at` | TIMESTAMP | `issued_date_key` | INT | `CAST(FORMAT(issued_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `policy_start_date` | DATE | `policy_start_date_key` | INT | `CAST(FORMAT(policy_start_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `policy_end_date` | DATE | `policy_end_date_key` | INT | `CAST(FORMAT(policy_end_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `premium_amount` | DECIMAL(18,2) | `issued_premium_amount` | DECIMAL(18,2) | `COALESCE(premium_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `policy.customer_id -> silver_vehicle.customer_id`, then lookup `dim_vehicle` by `vehicle_id` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`.

---

### 5.4 `fact_payment`

> **Silver Source Tables:** `silver_payment` (primary), `silver_policy` (context)
> **Grain:** One row per payment transaction

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `payment_id` | STRING | `payment_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `transaction_reference` | STRING | `transaction_reference` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `payment_status` | STRING | `payment_status_key` | BIGINT | Lookup `dim_payment_status` by `payment_status_code`. |
| `payment_method` | STRING | `payment_method_key` | BIGINT | Standardize -> lookup `dim_payment_method` by `payment_method_code`. |
| `payment_date` | DATE | `payment_date_key` | INT | `CAST(FORMAT(payment_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| *(via join to policy)* | STRING | `customer_key` | BIGINT | Join `payment.policy_id -> policy.policy_id`, lookup `dim_customer` WHERE `payment_date` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to policy)* | STRING | `provider_key` | BIGINT | Join `payment.policy_id -> policy.policy_id`, lookup `dim_provider` WHERE `payment_date` BETWEEN `effective_from` AND `effective_to`. |
| `payment_amount` | DECIMAL(18,2) | `payment_amount` | DECIMAL(18,2) | `COALESCE(payment_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `payment.policy_id -> policy.policy_id -> silver_vehicle.customer_id`, then lookup `dim_vehicle` WHERE `payment_date` BETWEEN `effective_from` AND `effective_to`.

---

### 5.5 `fact_cancellation`

> **Silver Source Tables:** `silver_cancellation` (primary), `silver_policy` (context)
> **Grain:** One row per policy cancellation event

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `cancellation_id` | STRING | `cancellation_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `cancellation_reason` | STRING | `cancellation_reason_key` | BIGINT | Lookup `dim_cancellation_reason` by `cancellation_reason`. |
| `cancellation_date` | DATE | `cancellation_date_key` | INT | `CAST(FORMAT(cancellation_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| *(via join to policy)* | STRING | `customer_key` | BIGINT | Join `cancellation.policy_id -> policy.policy_id`, lookup `dim_customer` WHERE `cancellation_date` BETWEEN `effective_from` AND `effective_to`. |
| *(via join to policy)* | STRING | `provider_key` | BIGINT | Join `cancellation.policy_id -> policy.policy_id`, lookup `dim_provider` WHERE `cancellation_date` BETWEEN `effective_from` AND `effective_to`. |
| `refund_amount` | DECIMAL(18,2) | `refund_amount` | DECIMAL(18,2) | `COALESCE(refund_amount, 0)`. |
| `_source_system` | STRING | `source_system` | STRING | Mapped from Silver audit column. |
| *(pipeline generated)* | - | `created_at` | TIMESTAMP | Gold insert timestamp. |
| *(pipeline generated)* | - | `updated_at` | TIMESTAMP | Gold update timestamp. |

> **Vehicle key resolution:**
> Join `cancellation.policy_id -> policy.policy_id -> silver_vehicle.customer_id`, then lookup `dim_vehicle` WHERE `cancellation_date` BETWEEN `effective_from` AND `effective_to`.

---

## 6. Silver Table Dependency Summary

| Gold Table | Primary Silver Source | Secondary Silver Sources |
| --- | --- | --- |
| `dim_date` | *(generated)* | - |
| `dim_customer` | `silver_customer` | - |
| `dim_agent` | `silver_agent` | - |
| `dim_provider` | `silver_provider` | - |
| `dim_package` | `silver_quotation` | - |
| `dim_coverage` | `silver_quotation_item` | - |
| `dim_quotation` | `silver_quotation` | - |
| `dim_policy` | `silver_policy` | - |
| `dim_quotation_status` | `silver_quotation` | - |
| `dim_policy_status` | `silver_policy` | - |
| `dim_payment_status` | `silver_payment` | - |
| `dim_payment_method` | `silver_payment` | - |
| `dim_cancellation_reason` | `silver_cancellation` | - |
| `dim_vehicle` | `silver_vehicle` | - |
| `fact_quotation` | `silver_quotation` | `silver_vehicle` |
| `fact_quotation_item` | `silver_quotation_item` | `silver_quotation`, `silver_vehicle` |
| `fact_policy` | `silver_policy` | `silver_quotation`, `silver_vehicle` |
| `fact_payment` | `silver_payment` | `silver_policy`, `silver_vehicle` |
| `fact_cancellation` | `silver_cancellation` | `silver_policy`, `silver_vehicle` |

---

## 7. Unknown Member Reference

All dimension lookups that fail to resolve must default to the unknown member row.

| Dimension | Unknown Key | Unknown Business Key | Notes |
| --- | --- | --- | --- |
| `dim_customer` | `-1` | `UNKNOWN` | Applied when `customer_id` is missing or not found. |
| `dim_agent` | `-1` | `UNKNOWN` | Applied when `agent_id` is missing. |
| `dim_provider` | `-1` | `UNKNOWN` | Applied when `provider_code` is missing or not found. |
| `dim_package` | `-1` | `UNKNOWN` | Applied when `package_code` is missing or not found. |
| `dim_coverage` | `-1` | `UNKNOWN` | Applied when `coverage_type` is missing or unmatched. |
| `dim_quotation` | `-1` | `UNKNOWN` | Applied when `quotation_id` is null or not found. |
| `dim_policy` | `-1` | `UNKNOWN` | Applied when `policy_id` is not found. |
| `dim_quotation_status` | `-1` | `UNKNOWN` | Applied when status code is not in reference set. |
| `dim_policy_status` | `-1` | `UNKNOWN` | Applied when status code is not in reference set. |
| `dim_payment_status` | `-1` | `UNKNOWN` | Applied when status code is not in reference set. |
| `dim_payment_method` | `-1` | `UNKNOWN` | Applied when method cannot be standardized or matched. |
| `dim_cancellation_reason` | `-1` | `UNKNOWN` | Applied when reason is null or unmatched. |
| `dim_vehicle` | `-1` | `UNKNOWN` | Applied when no vehicle can be resolved from customer context. |

---

## 8. Concerns and Decisions to Confirm

- Confirm Silver naming style to use everywhere: `silver_<entity>` (Lakehouse) or `silver.<entity>` (Warehouse).
- Confirm whether JSON fields `last_updated_at` should be normalized to `updated_at` for Silver policy, payment, and cancellation.
- Confirm the 1-to-1 assumption between customer and vehicle; if 1-to-many, add a bridge and update vehicle key resolution rules.
- Confirm whether `payment_date` and `cancellation_date` should be kept as `DATE` or promoted to `TIMESTAMP`.
- Confirm which date should drive SCD2 resolution for `dim_customer` and `dim_provider` in facts (event date vs issued date).

---

## 9. Revision History

| Version | Date | Author | Notes |
| --- | --- | --- | --- |
| 1.1 | 2026-06-01 | Data Engineering Team | Updated Silver to Gold mapping to match naming conventions and the Bronze-to-Silver spec. |
