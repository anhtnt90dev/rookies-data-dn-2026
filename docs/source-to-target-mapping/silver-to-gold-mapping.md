# Silver to Gold Layer Column Mapping

**Insurance Analytics - Dimensional Model (Star Schema)**

---

## 1. Purpose

This document defines the column-level mapping between the Silver layer (cleansed, typed, conformed tables) and the Gold layer (dimensional model). It aligns with the current Bronze-to-Silver mapping and naming conventions.

---

## 2. Naming Alignment and Assumptions

- Silver tables use Lakehouse prefix naming: `silver.<entity>` (if using schemas, replace with `silver.<entity>`).
- Timestamp columns end with `_at`; date-only columns end with `_date`.
- JSON-derived timestamps are already cast to `TIMESTAMP` in Silver (e.g., `payment_at`, `cancellation_at`).
- Provider active flag is standardized as `is_active` (BOOLEAN) in Silver.
- Metadata and audit columns `_batch_id`, `_source_system`, and `_source_name` are carried directly from the Silver layer to the Gold layer for traceability. The `_loaded_at` column is generated new at the Gold layer. 

- For fact tables, `_source_name` and `_loaded_at` are replaced by pipeline lineage and delete detection fields (`pipeline_run_id`, `is_deleted`, `deleted_at`, `delete_batch_id`).

---

## 3. General Transformation Standards

| Standard                   | Description                                                                                                                   |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Type casting               | Silver stores values in native types (STRING, INT, DECIMAL, DATE, TIMESTAMP). Gold casts only where noted.                    |
| Surrogate key resolution   | Fact table FK columns are resolved by lookup against the corresponding dimension using business keys.                         |
| SCD Type 2 resolution      | For Type 2 dimensions, facts resolve the correct version by matching event dates against `effective_from` and `effective_to`. |
| Unknown member default    | If a lookup fails, the FK defaults to `-1` (Unknown).                                                                         |
| Date key format            | Date columns convert to integer `YYYYMMDD` for `dim_date` FK resolution.                                                      |
| Null handling on measures | `COALESCE(value, 0)` for all numeric measures.                                                                                |
| Audit and Metadata columns | `created_at` and `updated_at` are mapped directly from Silver. Metadata columns are retained across all tables.               |

---

## 4. Silver to Gold: Dimension Tables

### 4.1 `dim_date`

> **Source:** Generated calendar (no Silver source table, metadata columns default to system tracking)

| Gold Column      | Type    | Generation Rule                     |
| ---------------- | ------- | ----------------------------------- |
| `date_key`       | INT     | `FORMAT(calendar_date, 'YYYYMMDD')` |
| `full_date`      | DATE    | Calendar date                       |
| `day_number`     | INT     | `DAY(full_date)`                    |
| `day_name`       | STRING  | `DAYNAME(full_date)`                |
| `week_number`    | INT     | `WEEKOFYEAR(full_date)`             |
| `month_number`   | INT     | `MONTH(full_date)`                  |
| `month_name`     | STRING  | `MONTHNAME(full_date)`              |
| `quarter_number` | INT     | `QUARTER(full_date)`                |
| `year_number`    | INT     | `YEAR(full_date)`                   |
| `year_month`     | STRING  | `FORMAT(full_date, 'YYYY-MM')`      |
| `is_weekend`     | BOOLEAN | `DAYOFWEEK(full_date) IN (1, 7)`    |

---

### 4.2 `dim_customer`

> **Silver Source Table:** `silver.customer`
> **SCD Type:** Type 2

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                 |
| ---------------------- | ----------- | ------------------------ | --------- | -------------------------------------------------------------- |
| _(pipeline generated)_ | -           | `customer_key`           | BIGINT    | System-generated surrogate key. New key per SCD2 version.      |
| `customer_id`          | STRING      | `customer_id`            | STRING    | Direct mapping. Business key retained.                         |
| `full_name`            | STRING      | `full_name`              | STRING    | Direct mapping.                                                |
| `gender`               | STRING      | `gender`                 | STRING    | Direct mapping.                                                |
| `dob`                  | DATE        | `dob`                    | DATE      | Direct mapping.                                                |
| `phone_number`         | STRING      | `phone_number`           | STRING    | Direct mapping.                                                |
| `email`                | STRING      | `email`                  | STRING    | Direct mapping.                                                |
| `city`                 | STRING      | `city`                   | STRING    | Direct mapping.                                                |
| `district`             | STRING      | `district`               | STRING    | Direct mapping.                                                |
| _(SCD logic)_          | -           | `effective_from`         | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row.            |
| _(SCD logic)_          | -           | `effective_to`           | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| _(SCD logic)_          | -           | `is_current`             | BOOLEAN   | `true` for latest version.                                     |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                              |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                              |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                              |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                       |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                              |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                              |

---

### 4.3 `dim_agent`

> **Silver Source Table:** `silver.agent`
> **SCD Type:** Type 2

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                 |
| ---------------------- | ----------- | ------------------------ | --------- | -------------------------------------------------------------- |
| _(pipeline generated)_ | -           | `agent_key`              | BIGINT    | System-generated surrogate key. New key per SCD2 version.      |
| `agent_id`             | STRING      | `agent_id`               | STRING    | Direct mapping.                                                |
| `agent_name`           | STRING      | `agent_name`             | STRING    | Direct mapping.                                                |
| `region`               | STRING      | `region`                 | STRING    | Direct mapping.                                                |
| `branch`               | STRING      | `branch`                 | STRING    | Direct mapping.                                                |
| `manager_name`         | STRING      | `manager_name`           | STRING    | Direct mapping.                                                |
| _(SCD logic)_          | -           | `effective_from`         | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row.            |
| _(SCD logic)_          | -           | `effective_to`           | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| _(SCD logic)_          | -           | `is_current`             | BOOLEAN   | `true` for latest version.                                     |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                              |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                              |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                              |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                       |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                              |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                              |

---

### 4.4 `dim_provider`

> **Silver Source Table:** `silver.provider`
> **SCD Type:** Type 2

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                 |
| ---------------------- | ----------- | ------------------------ | --------- | -------------------------------------------------------------- |
| _(pipeline generated)_ | -           | `provider_key`           | BIGINT    | System-generated surrogate key. New key per SCD2 version.      |
| `provider_code`        | STRING      | `provider_code`          | STRING    | Direct mapping. Business key retained.                         |
| `provider_name`        | STRING      | `provider_name`          | STRING    | Direct mapping.                                                |
| `provider_group`       | STRING      | `provider_group`         | STRING    | Direct mapping.                                                |
| `is_active`            | BOOLEAN     | `active_flag`            | INT       | Cast BOOLEAN to INT.                                           |
| _(SCD logic)_          | -           | `effective_from`         | TIMESTAMP | `COALESCE(updated_at, created_at)` from Silver row.            |
| _(SCD logic)_          | -           | `effective_to`           | TIMESTAMP | `9999-12-31 23:59:59` for current row; updated on new version. |
| _(SCD logic)_          | -           | `is_current`             | BOOLEAN   | `true` for latest version.                                     |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                              |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                              |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                              |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                       |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                              |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                              |

---

### 4.5 `dim_package`

> **Silver Source Table:** `silver.quotation` (distinct `package_code`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                   |
| ---------------------- | ----------- | ------------------------ | --------- | ------------------------------------------------ |
| _(pipeline generated)_ | -           | `package_key`            | BIGINT    | System-generated surrogate key.                  |
| `package_code`         | STRING      | `package_code`           | STRING    | `DISTINCT package_code` from `silver.quotation`. |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                         |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                |

---

### 4.6 `dim_coverage`

> **Silver Source Table:** `silver.quotation_item` (distinct `coverage_type`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                         |
| ---------------------- | ----------- | ------------------------ | --------- | ------------------------------------------------------ |
| _(pipeline generated)_ | -           | `coverage_key`           | BIGINT    | System-generated surrogate key.                        |
| `coverage_type`        | STRING      | `coverage_type`          | STRING    | `DISTINCT coverage_type` from `silver.quotation_item`. |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                      |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                      |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                      |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                               |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                      |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                      |

---

### 4.7 `dim_quotation`

> **Silver Source Table:** `silver.quotation`
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                     |
| ---------------------- | ----------- | ------------------------ | --------- | ------------------------------------------------------------------ |
| _(pipeline generated)_ | -           | `quotation_key`          | BIGINT    | System-generated surrogate key.                                    |
| `quotation_id`         | STRING      | `quotation_id`           | STRING    | Direct mapping. Business key.                                      |
| `quotation_expiry_at`  | TIMESTAMP   | `quotation_expiry_date`  | DATE      | Cast `TIMESTAMP` to `DATE`.                                        |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                                  |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                                  |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                                  |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                           |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                                  |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                                  |

---

### 4.8 `dim_policy`

> **Silver Source Table:** `silver.policy`
> **SCD Type:** Type 1

| Silver Column          | Silver Type   | Gold Column              | Gold Type     | Transform Rule                    |
| ---------------------- | ------------- | ------------------------ | ------------- | --------------------------------- |
| _(pipeline generated)_ | -             | `policy_key`             | BIGINT        | System-generated surrogate key.   |
| `policy_id`            | STRING        | `policy_id`              | STRING        | Direct mapping. Business key.     |
| -                      | -             | `created_at`             | TIMESTAMP     | Generate at Gold load time.        |
| `updated_at`           | TIMESTAMP     | `updated_at`             | TIMESTAMP     | Direct mapping from Silver layer. |
| `_batch_id`            | STRING        | `_batch_id`              | STRING        | Direct mapping from Silver layer. |
| `_loaded_at`           | TIMESTAMP     | `_loaded_at`             | TIMESTAMP     | Generate                          |
| `_source_system`       | STRING        | `_source_system`         | STRING        | Direct mapping from Silver layer. |
| `_source_name`         | STRING        | `_source_name`           | STRING        | Direct mapping from Silver layer. |

---

### 4.9 `dim_quotation_status`

> **Silver Source Table:** `silver.quotation` (distinct `quotation_status`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                      |
| ---------------------- | ----------- | ------------------------ | --------- | --------------------------------------------------- |
| _(pipeline generated)_ | -           | `quotation_status_key`   | BIGINT    | System-generated surrogate key.                     |
| `quotation_status`     | STRING      | `quotation_status_code`  | STRING    | Direct mapping.                                     |
| `created_at`           | TIMESTAMP   | `created_at`             | TIMESTAMP | Direct mapping from Silver layer.                   |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                   |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                   |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                            |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                   |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                   |

---

### 4.10 `dim_policy_status`

> **Silver Source Table:** `silver.policy` (distinct `policy_status`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                                                                                         |
| ---------------------- | ----------- | ------------------------ | --------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| _(pipeline generated)_ | -           | `policy_status_key`      | BIGINT    | System-generated surrogate key.                                                                                                        |
| `policy_status`        | STRING      | `policy_status_code`     | STRING    | Direct mapping.                                                                                                                        |
| -                      | -           | `created_at`             | TIMESTAMP | Generate at Gold load time.                                                                                                            |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                                                                                                      |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                                                                                                      |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                                                                                               |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                                                                                                      |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                                                                                                      |

---

### 4.11 `dim_payment_status`

> **Silver Source Table:** `silver.payment` (distinct `payment_status`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                                                                                                                 |
| ---------------------- | ----------- | ------------------------ | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| _(pipeline generated)_ | -           | `payment_status_key`     | BIGINT    | System-generated surrogate key.                                                                                                                                |
| `payment_status`       | STRING      | `payment_status_code`    | STRING    | Direct mapping.                                                                                                                                                |
| -                      | -           | `created_at`             | TIMESTAMP | Generate at Gold load time.                                                                                                                                    |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                                                                                                                              |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                                                                                                                              |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                                                                                                                       |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                                                                                                                              |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                                                                                                                              |

---

### 4.12 `dim_payment_method`

> **Silver Source Table:** `silver.payment` (distinct `payment_method`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                                                                                                                                                 |
| ---------------------- | ----------- | ------------------------ | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| _(pipeline generated)_ | -           | `payment_method_key`     | BIGINT    | System-generated surrogate key.                                                                                                                                |
| `payment_method`       | STRING      | `payment_method_code`    | STRING    | Standardize raw values: `Bank Transfer -> BANK_TRANSFER`, `Credit Card -> CREDIT_CARD`, `E-wallet -> E_WALLET`.                                                 |
| -                      | -           | `created_at`             | TIMESTAMP | Generate at Gold load time.                                                                                                                                    |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer.                                                                                                                              |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer.                                                                                                                              |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                                                                                                                                                       |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer.                                                                                                                              |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer.                                                                                                                              |

---

### 4.13 `dim_cancellation_reason`

> **Silver Source Table:** `silver.cancellation` (distinct `cancellation_reason`)
> **SCD Type:** Type 1

| Silver Column          | Silver Type | Gold Column              | Gold Type | Transform Rule                    |
| ---------------------- | ----------- | ------------------------ | --------- | --------------------------------- |
| _(pipeline generated)_ | -           | `cancellation_reason_key`| BIGINT    | System-generated surrogate key.   |
| `cancellation_reason`  | STRING      | `cancellation_reason`    | STRING    | Direct mapping.                   |
| -                      | -           | `created_at`             | TIMESTAMP | Generate at Gold load time.       |
| `updated_at`           | TIMESTAMP   | `updated_at`             | TIMESTAMP | Direct mapping from Silver layer. |
| `_batch_id`            | STRING      | `_batch_id`              | STRING    | Direct mapping from Silver layer. |
| `_loaded_at`           | TIMESTAMP   | `_loaded_at`             | TIMESTAMP | Generate                          |
| `_source_system`       | STRING      | `_source_system`         | STRING    | Direct mapping from Silver layer. |
| `_source_name`         | STRING      | `_source_name`           | STRING    | Direct mapping from Silver layer. |

---

### 4.14 `dim_vehicle`

> **Silver Source Table:** `silver.vehicle`
> **SCD Type:** Type 2

| Silver Column          | Silver Type   | Gold Column              | Gold Type     | Transform Rule                                                 |
| ---------------------- | ------------- | ------------------------ | ------------- | -------------------------------------------------------------- |
| _(pipeline generated)_ | -             | `vehicle_key`            | BIGINT        | System-generated surrogate key. New key per SCD2 version.      |
| `vehicle_id`           | STRING        | `vehicle_id`             | STRING        | Direct mapping. Business key.                                  |
| `customer_id`          | STRING        | `customer_id`            | STRING        | Direct mapping.                                                |
| `plate_number`         | STRING        | `plate_number`           | STRING        | Direct mapping.                                                |
| `vehicle_brand`        | STRING        | `vehicle_brand`          | STRING        | Direct mapping.                                                |
| `vehicle_model`        | STRING        | `vehicle_model`          | STRING        | Direct mapping.                                                |
| `manufacture_year`     | INT           | `manufacture_year`       | INT           | Direct mapping.                                                |
| `vehicle_value`        | DECIMAL(18,2) | `vehicle_value`          | DECIMAL(18,2) | Direct mapping.                                                |
| _(SCD logic)_          | -             | `effective_from`         | TIMESTAMP     | `COALESCE(updated_at, created_at)` from Silver row.            |
| _(SCD logic)_          | -             | `effective_to`           | TIMESTAMP     | `9999-12-31 23:59:59` for current row; updated on new version. |
| _(SCD logic)_          | -             | `is_current`             | BOOLEAN       | `true` for latest version.                                     |
| `created_at`           | TIMESTAMP     | `created_at`             | TIMESTAMP     | Direct mapping from Silver layer.                              |
| `updated_at`           | TIMESTAMP     | `updated_at`             | TIMESTAMP     | Direct mapping from Silver layer.                              |
| `_batch_id`            | STRING        | `_batch_id`              | STRING        | Direct mapping from Silver layer.                              |
| `_loaded_at`           | TIMESTAMP     | `_loaded_at`             | TIMESTAMP     | Generate                                                       |
| `_source_system`       | STRING        | `_source_system`         | STRING        | Direct mapping from Silver layer.                              |
| `_source_name`         | STRING        | `_source_name`           | STRING        | Direct mapping from Silver layer.                              |

---

## 5. Silver to Gold: Fact Tables

### 5.1 `fact_quotation`

> **Silver Source Table:** `silver.quotation`
> **Grain:** One row per quotation

| Silver Column          | Silver Type   | Gold Column              | Gold Type     | Transform Rule                                                                                             |
| ---------------------- | ------------- | ------------------------ | ------------- | ---------------------------------------------------------------------------------------------------------- |
| `quotation_id`         | STRING        | `quotation_key`          | BIGINT        | Lookup `dim_quotation` by `quotation_id`.                                                                  |
| `quotation_at`         | TIMESTAMP     | `quotation_date_key`     | INT           | `CAST(FORMAT(quotation_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`.                                        |
| `quotation_expiry_at`  | TIMESTAMP     | `quotation_expiry_date_key`| INT         | `CAST(FORMAT(quotation_expiry_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`.                                 |
| `customer_id`          | STRING        | `customer_key`           | BIGINT        | Lookup `dim_customer` by `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`.   |
| `agent_id`             | STRING        | `agent_key`              | BIGINT        | Lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`.          |
| `provider_code`        | STRING        | `provider_key`           | BIGINT        | Lookup `dim_provider` by `provider_code` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `package_code`         | STRING        | `package_key`            | BIGINT        | Lookup `dim_package` by `package_code`.                                                                    |
| `quotation_status`     | STRING        | `quotation_status_key`   | BIGINT        | Lookup `dim_quotation_status` by `quotation_status`.                                                       |
| `customer_id`          | STRING        | `vehicle_key`            | BIGINT        | Join `silver.quotation.customer_id -> silver.vehicle.customer_id` (1-to-1 assumption), then lookup `dim_vehicle` by `vehicle_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. Default `-1` if not found. |
| `premium_amount`       | DECIMAL(18,2) | `premium_amount`         | DECIMAL(18,2) | `COALESCE(premium_amount, 0)`.                                                                             |
| `quotation_id`         | STRING        | `quotation_id`           | STRING        | Direct mapping. Degenerate dimension.                                                                      |
| `customer_id`          | STRING        | `customer_id`            | STRING        | Direct mapping. Degenerate dimension.                                                                      |
| `agent_id`             | STRING        | `agent_id`               | STRING        | Direct mapping. Degenerate dimension.                                                                      |
| `provider_code`        | STRING        | `provider_code`          | STRING        | Direct mapping. Degenerate dimension.                                                                      |
| `_source_system`       | STRING        | `_source_system`         | STRING        | Direct mapping from Silver layer.                                                                          |
| `created_at`           | TIMESTAMP     | `created_at`             | TIMESTAMP     | Direct mapping from Silver layer.                                                                          |
| `updated_at`           | TIMESTAMP     | `updated_at`             | TIMESTAMP     | Direct mapping from Silver layer.                                                                          |
| `_batch_id`            | STRING        | `_batch_id`              | STRING        | Direct mapping from Silver layer.                                                                          |
| -                      | -             | `pipeline_run_id`        | STRING        | Pipeline-derived lineage field. Maps to `log.audit_session.pipeline_run_id`.                               |
| -                      | -             | `is_deleted`             | BOOLEAN       | Pipeline-derived field. Defaults to `false` on load; `deleted_at` and `delete_batch_id` remain `null` until a delete event is detected. |
| -                      | -             | `deleted_at`             | TIMESTAMP     | Pipeline-derived field. Defaults to `null` until delete detected.                                          |
| -                      | -             | `delete_batch_id`        | STRING        | Pipeline-derived field. Defaults to `null` until delete detected.                                          |
| -                      | -             | `converted_flag`         | BOOLEAN       | `true` if the quotation is converted to a policy (i.e. `quotation_id` exists in `silver.policy`), else `false`. |

---

### 5.2 `fact_quotation_item`

> **Silver Source Tables:** `silver.quotation_item` (primary), `silver.quotation` (header context)
> **Grain:** One row per coverage line item within a quotation

| Silver Column             | Silver Type   | Gold Column              | Gold Type     | Transform Rule                                                                                             |
| ------------------------- | ------------- | ------------------------ | ------------- | ---------------------------------------------------------------------------------------------------------- |
| _(via join to quotation)_ | STRING        | `quotation_key`          | BIGINT        | Join `quotation_item.quotation_id -> quotation.quotation_id`, then lookup `dim_quotation`.                 |
| _(via join to quotation)_ | TIMESTAMP     | `quotation_date_key`     | INT           | From `quotation.quotation_at` -> date key.                                                                 |
| _(via join to quotation)_ | STRING        | `customer_key`           | BIGINT        | Lookup `dim_customer` by `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`.   |
| _(via join to quotation)_ | STRING        | `agent_key`              | BIGINT        | Lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`.          |
| _(via join to quotation)_ | STRING        | `provider_key`           | BIGINT        | Lookup `dim_provider` by `provider_code` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| _(via join to quotation)_ | STRING        | `package_key`            | BIGINT        | Lookup `dim_package` by `package_code`.                                                                    |
| `coverage_type`           | STRING        | `coverage_key`           | BIGINT        | Lookup `dim_coverage` by `coverage_type`.                                                                  |
| _(via join to quotation)_ | STRING        | `quotation_status_key`   | BIGINT        | Lookup `dim_quotation_status` by `quotation_status_code`.                                                  |
| _(via join to quotation)_ | STRING        | `vehicle_key`            | BIGINT        | Lookup `dim_vehicle` via customer context (join quotation on `quotation_id` and vehicle on `customer_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`). Default `-1` if not found. |
| `coverage_amount`         | DECIMAL(18,2) | `coverage_amount`         | DECIMAL(18,2) | `COALESCE(coverage_amount, 0)`.                                                                             |
| `deductible_amount`       | DECIMAL(18,2) | `deductible_amount`       | DECIMAL(18,2) | `COALESCE(deductible_amount, 0)`.                                                                           |
| `quotation_item_id`       | STRING        | `quotation_item_id`      | STRING        | Direct mapping. Degenerate dimension.                                                                      |
| `quotation_id`            | STRING        | `quotation_id`           | STRING        | Direct mapping. Degenerate dimension.                                                                      |
| `created_at`              | TIMESTAMP     | `created_at`             | TIMESTAMP     | Direct mapping from Silver layer.                                                                          |
| `updated_at`              | TIMESTAMP     | `updated_at`             | TIMESTAMP     | Direct mapping from Silver layer.                                                                          |
| `_batch_id`               | STRING        | `_batch_id`              | STRING        | Direct mapping from Silver layer.                                                                          |
| `_source_system`          | STRING        | `_source_system`         | STRING        | Direct mapping from Silver layer.                                                                          |
| -                         | -             | `pipeline_run_id`        | STRING        | Pipeline-derived lineage field. Maps to `log.audit_session.pipeline_run_id`.                               |
| -                         | -             | `is_deleted`             | BOOLEAN       | Pipeline-derived field. Defaults to `false` on load; `deleted_at` and `delete_batch_id` remain `null` until a delete event is detected. |
| -                         | -             | `deleted_at`             | TIMESTAMP     | Pipeline-derived field. Defaults to `null` until delete detected.                                          |
| -                         | -             | `delete_batch_id`        | STRING        | Pipeline-derived field. Defaults to `null` until delete detected.                                          |

---

### 5.3 `fact_policy`

> **Silver Source Tables:** `silver.policy` (primary), `silver.quotation` (agent and package context)
> **Grain:** One row per issued policy

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `quotation_id` | STRING | `quotation_key` | BIGINT | Lookup `dim_quotation` by `quotation_id`. |
| `issued_at` | TIMESTAMP | `issued_date_key` | INT | `CAST(FORMAT(issued_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `policy_start_date` | DATE | `policy_start_date_key` | INT | `CAST(FORMAT(policy_start_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `policy_end_date` | DATE | `policy_end_date_key` | INT | `CAST(FORMAT(policy_end_date, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| `customer_id` | STRING | `customer_key` | BIGINT | Lookup `dim_customer` by `customer_id` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`. |
| _(via join to quotation)_ | STRING | `agent_key` | BIGINT | Join `policy.quotation_id -> quotation.quotation_id`, then lookup `dim_agent` by `agent_id` WHERE `quotation_at` BETWEEN `effective_from` AND `effective_to`. |
| `provider_code` | STRING | `provider_key` | BIGINT | Lookup `dim_provider` by `provider_code` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`. |
| _(via join to quotation)_ | STRING | `package_key` | BIGINT | Join `policy.quotation_id -> quotation.quotation_id`, then lookup `dim_package` by `package_code`. |
| `policy_status` | STRING | `policy_status_key` | BIGINT | Lookup `dim_policy_status` by `policy_status_code`. |
| `customer_id` | STRING | `vehicle_key` | BIGINT | Lookup `dim_vehicle` via customer context (join policy on `customer_id` and vehicle on `customer_id` WHERE `issued_at` BETWEEN `effective_from` AND `effective_to`). Default `-1` if not found. |
| `premium_amount` | DECIMAL(18,2) | `premium_amount` | DECIMAL(18,2) | `COALESCE(premium_amount, 0)`. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_number` | STRING | `policy_number` | STRING | Direct mapping. Degenerate dimension. |
| `quotation_id` | STRING | `quotation_id` | STRING | Direct mapping. Degenerate dimension. |
| `customer_id` | STRING | `customer_id` | STRING | Direct mapping. Degenerate dimension. |
| `provider_code` | STRING | `provider_code` | STRING | Direct mapping. Degenerate dimension. |
| `_source_system` | STRING | `_source_system` | STRING | Direct mapping from Silver layer. |
| - | - | `created_at` | TIMESTAMP | Generate at Gold load time. |
| `updated_at` | TIMESTAMP | `updated_at` | TIMESTAMP | Direct mapping from Silver layer. |
| `_batch_id` | STRING | `_batch_id` | STRING | Direct mapping from Silver layer. |
| - | - | `pipeline_run_id` | STRING | Pipeline-derived lineage field. Maps to `log.audit_session.pipeline_run_id`. |
| - | - | `is_deleted` | BOOLEAN | Pipeline-derived field. Defaults to `false` on load; `deleted_at` and `delete_batch_id` remain `null` until a delete event is detected. |
| - | - | `deleted_at` | TIMESTAMP | Pipeline-derived field. Defaults to `null` until delete detected. |
| - | - | `delete_batch_id` | STRING | Pipeline-derived field. Defaults to `null` until delete detected. |

---

### 5.4 `fact_payment`

> **Silver Source Tables:** `silver.payment` (primary), `silver.policy` (context)
> **Grain:** One row per payment transaction

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `payment_date` | TIMESTAMP | `payment_date_key` | INT | `CAST(FORMAT(payment_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| _(via join to policy)_ | TIMESTAMP | `issued_date_key` | INT | `CAST(FORMAT(policy.issued_at, 'yyyyMMdd') AS INT)`; lookup `dim_date` via join to `policy`. |
| _(via join to policy)_ | STRING | `customer_key` | BIGINT | Join `payment.policy_id -> policy.policy_id`, lookup `dim_customer` WHERE `payment_at` BETWEEN `effective_from` AND `effective_to`. |
| _(via join to policy)_ | STRING | `provider_key` | BIGINT | Join `payment.policy_id -> policy.policy_id`, lookup `dim_provider` WHERE `payment_at` BETWEEN `effective_from` AND `effective_to`. |
| `payment_status` | STRING | `payment_status_key` | BIGINT | Lookup `dim_payment_status` by `payment_status_code`. |
| `payment_method` | STRING | `payment_method_key` | BIGINT | Standardize -> lookup `dim_payment_method` by `payment_method_code`. |
| _(via join to policy)_ | STRING | `vehicle_key` | BIGINT | Lookup `dim_vehicle` via policy/customer context (join policy on `policy_id` and vehicle on `customer_id` WHERE `payment_at` BETWEEN `effective_from` AND `effective_to`). Default `-1` if not found. |
| `payment_amount` | DECIMAL(18,2) | `payment_amount` | DECIMAL(18,2) | `COALESCE(payment_amount, 0)`. |
| `payment_id` | STRING | `payment_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `transaction_reference` | STRING | `transaction_reference` | STRING | Direct mapping. Degenerate dimension. |
| `_source_system` | STRING | `_source_system` | STRING | Direct mapping from Silver layer. |
| - | - | `created_at` | TIMESTAMP | Generate at Gold load time. |
| `updated_at` | TIMESTAMP | `updated_at` | TIMESTAMP | Direct mapping from Silver layer. |
| `_batch_id` | STRING | `_batch_id` | STRING | Direct mapping from Silver layer. |
| - | - | `pipeline_run_id` | STRING | Pipeline-derived lineage field. Maps to `log.audit_session.pipeline_run_id`. |
| - | - | `is_deleted` | BOOLEAN | Pipeline-derived field. Defaults to `false` on load; `deleted_at` and `delete_batch_id` remain `null` until a delete event is detected. |
| - | - | `deleted_at` | TIMESTAMP | Pipeline-derived field. Defaults to `null` until delete detected. |
| - | - | `delete_batch_id` | STRING | Pipeline-derived field. Defaults to `null` until delete detected. |

---

### 5.5 `fact_cancellation`

> **Silver Source Tables:** `silver.cancellation` (primary), `silver.policy` (context)
> **Grain:** One row per policy cancellation event

| Silver Column | Silver Type | Gold Column | Gold Type | Transform Rule |
| --- | --- | --- | --- | --- |
| `policy_id` | STRING | `policy_key` | BIGINT | Lookup `dim_policy` by `policy_id`. |
| `cancellation_at` | TIMESTAMP | `cancellation_date_key` | INT | `CAST(FORMAT(cancellation_at, 'yyyyMMdd') AS INT)`; lookup `dim_date`. |
| _(via join to policy)_ | STRING | `customer_key` | BIGINT | Join `cancellation.policy_id -> policy.policy_id`, lookup `dim_customer` WHERE `cancellation_at` BETWEEN `effective_from` AND `effective_to`. |
| _(via join to policy)_ | STRING | `provider_key` | BIGINT | Join `cancellation.policy_id -> policy.policy_id`, lookup `dim_provider` WHERE `cancellation_at` BETWEEN `effective_from` AND `effective_to`. |
| `cancellation_reason` | STRING | `cancellation_reason_key` | BIGINT | Lookup `dim_cancellation_reason` by `cancellation_reason`. |
| _(via join to policy)_ | STRING | `vehicle_key` | BIGINT | Lookup `dim_vehicle` via policy/customer context (join policy on `policy_id` and vehicle on `customer_id` WHERE `cancellation_at` BETWEEN `effective_from` AND `effective_to`). Default `-1` if not found. |
| `refund_amount` | DECIMAL(18,2) | `refund_amount` | DECIMAL(18,2) | `COALESCE(refund_amount, 0)`. |
| `cancellation_id` | STRING | `cancellation_id` | STRING | Direct mapping. Degenerate dimension. |
| `policy_id` | STRING | `policy_id` | STRING | Direct mapping. Degenerate dimension. |
| `_source_system` | STRING | `_source_system` | STRING | Direct mapping from Silver layer. |
| - | - | `created_at` | TIMESTAMP | Generate at Gold load time. |
| `updated_at` | TIMESTAMP | `updated_at` | TIMESTAMP | Direct mapping from Silver layer. |
| `_batch_id` | STRING | `_batch_id` | STRING | Direct mapping from Silver layer. |
| - | - | `pipeline_run_id` | STRING | Pipeline-derived lineage field. Maps to `log.audit_session.pipeline_run_id`. |
| - | - | `is_deleted` | BOOLEAN | Pipeline-derived field. Defaults to `false` on load; `deleted_at` and `delete_batch_id` remain `null` until a delete event is detected. |
| - | - | `deleted_at` | TIMESTAMP | Pipeline-derived field. Defaults to `null` until delete detected. |
| - | - | `delete_batch_id` | STRING | Pipeline-derived field. Defaults to `null` until delete detected. |

---

## 6. Silver Table Dependency Summary

| Gold Table                | Primary Silver Source   | Secondary Silver Sources             |
| ------------------------- | ----------------------- | ------------------------------------ |
| `dim_date`                | _(generated)_           | -                                    |
| `dim_customer`            | `silver.customer`       | -                                    |
| `dim_agent`               | `silver.agent`          | -                                    |
| `dim_provider`            | `silver.provider`       | -                                    |
| `dim_package`             | `silver.quotation`      | -                                    |
| `dim_coverage`            | `silver.quotation_item` | -                                    |
| `dim_quotation`           | `silver.quotation`      | -                                    |
| `dim_policy`              | `silver.policy`         | -                                    |
| `dim_quotation_status`    | `silver.quotation`      | -                                    |
| `dim_policy_status`       | `silver.policy`         | -                                    |
| `dim_payment_status`      | `silver.payment`        | -                                    |
| `dim_payment_method`      | `silver.payment`        | -                                    |
| `dim_cancellation_reason` | `silver.cancellation`   | -                                    |
| `dim_vehicle`             | `silver.vehicle`        | -                                    |
| `fact_quotation`          | `silver.quotation`      | `silver.vehicle`                     |
| `fact_quotation_item`     | `silver.quotation_item` | `silver.quotation`, `silver.vehicle` |
| `fact_policy`             | `silver.policy`         | `silver.quotation`, `silver.vehicle` |
| `fact_payment`            | `silver.payment`        | `silver.policy`, `silver.vehicle`    |
| `fact_cancellation`       | `silver.cancellation`   | `silver.policy`, `silver.vehicle`    |

---

## 7. Unknown Member Reference

All dimension lookups that fail to resolve must default to the unknown member row.

| Dimension                 | Unknown Key | Unknown Business Key | Notes                                                          |
| ------------------------- | ----------- | -------------------- | -------------------------------------------------------------- |
| `dim_customer`            | `-1`        | `UNKNOWN`            | Applied when `customer_id` is missing or not found.            |
| `dim_agent`               | `-1`        | `UNKNOWN`            | Applied when `agent_id` is missing.                            |
| `dim_provider`            | `-1`        | `UNKNOWN`            | Applied when `provider_code` is missing or not found.          |
| `dim_package`             | `-1`        | `UNKNOWN`            | Applied when `package_code` is missing or not found.           |
| `dim_coverage`            | `-1`        | `UNKNOWN`            | Applied when `coverage_type` is missing or unmatched.          |
| `dim_quotation`           | `-1`        | `UNKNOWN`            | Applied when `quotation_id` is null or not found.              |
| `dim_policy`              | `-1`        | `UNKNOWN`            | Applied when `policy_id` is not found.                         |
| `dim_quotation_status`    | `-1`        | `UNKNOWN`            | Applied when status code is not in reference set.              |
| `dim_policy_status`       | `-1`        | `UNKNOWN`            | Applied when status code is not in reference set.              |
| `dim_payment_status`      | `-1`        | `UNKNOWN`            | Applied when status code is not in reference set.              |
| `dim_payment_method`      | `-1`        | `UNKNOWN`            | Applied when method cannot be standardized or matched.         |
| `dim_cancellation_reason` | `-1`        | `UNKNOWN`            | Applied when reason is null or unmatched.                      |
| `dim_vehicle`             | `-1`        | `UNKNOWN`            | Applied when no vehicle can be resolved from customer context. |

---

## 8. Revision History

| Version | Date       | Author                | Notes                                                                                     |
| ------- | ---------- | --------------------- | ----------------------------------------------------------------------------------------- |
| 1.1     | 2026-06-01 | Data Engineering Team | Updated Silver to Gold mapping to match naming conventions and the Bronze-to-Silver spec. |
| 1.2     | 2026-06-01 | Data Engineering Team | Integrated metadata columns (_batch_id, _loaded_at, etc.) and direct Silver mapping rules.|
| 1.3     | 2026-06-04 | Data Engineering Team | Updated _loaded_at to be generated new at each layer.                                     |
