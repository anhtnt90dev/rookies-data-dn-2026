# Silver Layer Data Quality Rules

This document defines standardized data types, validation rules, and cleansing actions for the Silver Layer of the insurance data platform.

## Bronze to Silver Transformation Scope

The Bronze layer stores raw ingested data from source systems with minimal transformation.

The Silver layer applies:

* Data cleansing
* Standardization
* Validation
* Deduplication
* Referential integrity checks

to produce trusted and analytics-ready datasets.

Invalid or non-recoverable records are redirected to the `log.invalid_record` table for auditing and troubleshooting purposes.

---

## Error Handling: log.invalid_record

`log.invalid_record` is the physical table used by the Lakehouse implementation to capture records that fail validation or transformation rules during Silver layer processing. It maps to the canonical `Pipeline_Error` concept in the project baseline.

| Column | Data Type | Description |
|---|---|---|
| `id` | BIGINT | Unique invalid record identifier |
| `table_session_id` | BIGINT | Related table execution session identifier |
| `layer` | VARCHAR(20) | Layer where the validation failure occurred |
| `target_table` | VARCHAR(100) | Target table associated with the failed record. Maps to `source_name` in the `Pipeline_Error` baseline concept. |
| `record_key` | VARCHAR(255) | Business key or primary key of the failed record |
| `raw_data` | TEXT | Original record content. Maps to `raw_payload` in the `Pipeline_Error` baseline concept. |
| `error_column` | VARCHAR(100) | Column that failed validation |
| `error_reason` | TEXT | Validation or processing error description |
| `created_at` | TIMESTAMP | Record creation timestamp |

> **Baseline field mapping note:**
> The following `Pipeline_Error` baseline fields are covered by the pipeline execution context via `log.audit_session` and `log.table_session`, not stored redundantly in `log.invalid_record`:
> - `batch_id` → available via `table_session_id → log.table_session.batch_id`
> - `source_system` → available via `table_session_id → log.table_session.source_system`

---

## Incremental Processing Rule

All source tables support incremental processing.

Change detection must be based on:

```sql
COALESCE(updated_date, created_date)
```

Business rules:

* If `updated_date` exists, use `updated_date`.
* If `updated_date` is NULL, use `created_date`.
* Incremental extraction processes records where:

```sql
COALESCE(updated_date, created_date) > last_successful_watermark
```

This ensures both newly inserted and updated records are captured correctly.

---

# DATABASE: INSURANCE_CRM_DB

## quotation

| Column                | Data Type     | Validation Rule                                                    | Cleansing Action                                         |
| --------------------- | ------------- | ------------------------------------------------------------------ | -------------------------------------------------------- |
| quotation_id          | STRING        | not null, unique                                                   | remove duplicates; null → log.invalid_record             |
| customer_id           | STRING        | not null, valid FK                                                 | invalid FK/null → log.invalid_record                     |
| agent_id              | STRING        | not null, valid FK                                                 | invalid FK → log.invalid_record                          |
| provider_code         | STRING        | not null, valid FK                                                 | invalid FK/null → log.invalid_record                     |
| quotation_date        | DATE          | valid date, format yyyy-MM-dd                                      | convert to ISO 8601 format; invalid → log.invalid_record |
| quotation_status      | STRING        | allowed values: QUOTED · ACCEPTED · REJECTED · EXPIRED · CONVERTED | uppercase standardization; invalid → log.invalid_record  |
| package_code          | STRING        | not null                                                           | uppercase standardization; null → log.invalid_record     |
| premium_amount        | DECIMAL(18,2) | > 0                                                                | round to 2 decimals; invalid → log.invalid_record        |
| quotation_expiry_date | DATE          | format yyyy-MM-dd, >= quotation_date                               | convert to ISO 8601 format; invalid → log.invalid_record |
| created_date          | TIMESTAMP     | not null, valid date, <= current_date                              | convert to ISO 8601 format; invalid → log.invalid_record |
| updated_date          | TIMESTAMP     | NULLABLE, valid date, <= current_date                              | convert to ISO 8601 format; invalid → log.invalid_record |

---

## quotation_item

| Column            | Data Type     | Validation Rule                       | Cleansing Action                                         |
| ----------------- | ------------- | ------------------------------------- | -------------------------------------------------------- |
| quotation_item_id | STRING        | not null, unique                      | remove duplicates; null → log.invalid_record             |
| quotation_id      | STRING        | not null, valid FK                    | invalid FK/null → log.invalid_record                     |
| coverage_type     | STRING        | not null                              | uppercase standardization; null → log.invalid_record     |
| coverage_amount   | DECIMAL(18,2) | > 0                                   | round to 2 decimals; invalid → log.invalid_record        |
| deductible_amount | DECIMAL(18,2) | >= 0 and < coverage_amount            | round to 2 decimals; invalid → log.invalid_record        |
| created_date      | TIMESTAMP     | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → log.invalid_record |
| updated_date      | TIMESTAMP     | NULLABLE, valid date, <= current_date | convert to ISO 8601 format; invalid → log.invalid_record |

---

# DATABASE: INSURANCE_POLICY_DB

## policy_info

| Column            | Data Type     | Validation Rule                                                 | Cleansing Action                                        |
| ----------------- | ------------- | --------------------------------------------------------------- | ------------------------------------------------------- |
| policy_id         | STRING        | not null, unique PK                                             | remove duplicates; null → log.invalid_record            |
| quotation_id      | STRING        | not null, valid FK                                              | invalid FK/null → log.invalid_record                    |
| customer_id       | STRING        | not null, valid FK                                              | invalid FK/null → log.invalid_record                    |
| provider_code     | STRING        | not null, valid FK                                              | uppercase standardization; invalid → log.invalid_record |
| policy_number     | STRING        | not null, unique, pattern match                                 | trim whitespace; null/duplicate → log.invalid_record    |
| policy_start_date | DATE          | not null, valid date, <= policy_end_date                        | ISO 8601 format; invalid → log.invalid_record           |
| policy_end_date   | DATE          | not null, valid date, > policy_start_date                       | ISO 8601 format; invalid → log.invalid_record           |
| policy_status     | STRING        | not null, allowed values: ISSUED · ACTIVE · EXPIRED · CANCELLED | uppercase standardization; invalid → log.invalid_record |
| premium_amount    | DECIMAL(18,2) | not null, > 0                                                   | round to 2 decimals; invalid → log.invalid_record       |
| issued_date       | DATE          | not null, valid date, <= today                                  | ISO 8601 format; future date → log.invalid_record       |
| last_updated      | TIMESTAMP     | nullable, valid timestamp                                       | ISO 8601 format                                         |
| operation_type    | STRING        | allowed values: I · U · D                                       | invalid → log.invalid_record                            |
| batch_date        | DATE          | valid date                                                      | ISO 8601 format                                         |
| source_system     | STRING        | not null                                                        | uppercase standardization                               |
| source_file       | STRING        | nullable                                                        | trim whitespace                                         |
| batch_id          | STRING        | not null                                                        | null → log.invalid_record                               |
| raw_payload       | TEXT          | optional                                                        | stored as-is; used for error tracing                    |

---

## cancellation

| Column              | Data Type     | Validation Rule                            | Cleansing Action                                               |
| ------------------- | ------------- | ------------------------------------------ | -------------------------------------------------------------- |
| cancellation_id     | STRING        | not null, unique PK                        | remove duplicates; null → log.invalid_record                   |
| policy_id           | STRING        | not null, valid FK                         | invalid FK/null → log.invalid_record                           |
| cancellation_date   | DATE          | not null, valid date, >= policy_start_date | ISO 8601 format; invalid → log.invalid_record                  |
| cancellation_reason | STRING        | nullable                                   | trim whitespace                                                |
| refund_amount       | DECIMAL(18,2) | >= 0, numeric                              | round to 2 decimals; null → 0; negative → log.invalid_record   |
| last_updated        | TIMESTAMP     | nullable, valid timestamp                  | ISO 8601 format                                                |
| operation_type      | STRING        | allowed values: I · U · D                  | invalid → log.invalid_record                                   |
| batch_date          | DATE          | valid date                                 | ISO 8601 format                                                |
| source_system       | STRING        | not null                                   | uppercase standardization                                      |
| source_file         | STRING        | nullable                                   | trim whitespace                                                |
| batch_id            | STRING        | not null                                   | null → log.invalid_record                                      |
| raw_payload         | TEXT          | optional                                   | stored as-is; used for error tracing                           |

---

## payment

| Column                | Data Type     | Validation Rule                                                | Cleansing Action                                        |
| --------------------- | ------------- | -------------------------------------------------------------- | ------------------------------------------------------- |
| payment_id            | STRING        | not null, unique PK                                            | remove duplicates; null → log.invalid_record            |
| policy_id             | STRING        | not null, valid FK                                             | invalid FK/null → log.invalid_record                    |
| payment_date          | DATE          | not null, valid date, <= today                                 | ISO 8601 format; future/null → log.invalid_record       |
| payment_method        | STRING        | allowed values: CREDIT CARD · BANK TRANSFER · CASH · E-WALLET  | uppercase standardization                               |
| payment_status        | STRING        | not null, allowed values: PENDING · PAID · FAILED · REFUNDED   | uppercase standardization; invalid → log.invalid_record |
| payment_amount        | DECIMAL(18,2) | not null, > 0                                                  | round to 2 decimals; invalid → log.invalid_record       |
| transaction_reference | STRING        | not null, unique                                               | trim whitespace; null/duplicate → log.invalid_record    |
| last_updated          | TIMESTAMP     | nullable, valid timestamp                                      | ISO 8601 format                                         |
| operation_type        | STRING        | allowed values: I · U · D                                      | invalid → log.invalid_record                            |
| batch_date            | DATE          | valid date                                                     | ISO 8601 format                                         |
| source_system         | STRING        | not null                                                       | uppercase standardization                               |
| source_file           | STRING        | nullable                                                       | trim whitespace                                         |
| batch_id              | STRING        | not null                                                       | null → log.invalid_record                               |
| raw_payload           | TEXT          | optional                                                       | stored as-is; used for error tracing                    |

---

# Source-to-Canonical Status Mapping

## Quotation Status Mapping

| Source Status | Canonical Status | Notes |
| ------------- | ---------------- | ----- |
| QUOTED        | QUOTED           | No change |
| ACCEPTED      | ACCEPTED         | No change |
| REJECTED      | REJECTED         | No change |
| EXPIRED       | EXPIRED          | No change |
| CONVERTED     | CONVERTED        | `CONVERTED` is retained as a valid quotation status in the Silver layer. During Gold ETL, `converted_flag = true` is derived for any quotation where `quotation_status = CONVERTED`. Both the status and the flag are maintained independently. |

## Policy Status Mapping

| Source Status | Canonical Status | Notes     |
| ------------- | ---------------- | --------- |
| ISSUED        | ISSUED           | No change |
| ACTIVE        | ACTIVE           | No change |
| EXPIRED       | EXPIRED          | No change |
| CANCELLED     | CANCELLED        | No change |

## Payment Status Mapping

| Source Status | Canonical Status | Notes     |
| ------------- | ---------------- | --------- |
| PENDING       | PENDING          | No change |
| PAID          | PAID             | No change |
| FAILED        | FAILED           | No change |
| REFUNDED      | REFUNDED         | No change |

---

# Standard Date & Timestamp Format

| Data Type | Standard Format         | Example                 |
| --------- | ----------------------- | ----------------------- |
| DATE      | yyyy-MM-dd              | 2026-05-26              |
| TIMESTAMP | yyyy-MM-dd HH:mm:ss.SSS | 2026-05-26 08:49:15.063 |