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

Invalid or non-recoverable records are redirected to the `Error_Record` table for auditing and troubleshooting purposes.

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

| Column                | Data Type     | Validation Rule                                                    | Cleansing Action                                   |
| --------------------- | ------------- | ------------------------------------------------------------------ | -------------------------------------------------- |
| quotation_id          | STRING        | not null, unique                                                   | remove duplicates; null → Error_Record             |
| customer_id           | STRING        | not null, valid FK                                                 | invalid FK/null → Error_Record                     |
| agent_id              | STRING        | not null, valid FK                                                 | invalid FK → Error_Record                          |
| provider_code         | STRING        | not null, valid FK                                                 | invalid FK/null → Error_Record                     |
| quotation_date        | DATE          | valid date, format yyyy-MM-dd                                      | convert to ISO 8601 format; invalid → Error_Record |
| quotation_status      | STRING        | allowed values: QUOTED · ACCEPTED · REJECTED · EXPIRED · CONVERTED | uppercase standardization; invalid → Error_Record  |
| package_code          | STRING        | not null                                                           | uppercase standardization; null → Error_Record     |
| premium_amount        | DECIMAL(18,2) | > 0                                                                | round to 2 decimals; invalid → Error_Record        |
| quotation_expiry_date | DATE          | format yyyy-MM-dd, >= quotation_date                               | convert to ISO 8601 format; invalid → Error_Record |
| created_date          | TIMESTAMP     | not null, valid date, <= current_date                              | convert to ISO 8601 format; invalid → Error_Record |
| updated_date          | TIMESTAMP     | NULLABLE, valid date, <= current_date                              | convert to ISO 8601 format; invalid → Error_Record |

---

## quotation_item

| Column            | Data Type     | Validation Rule                       | Cleansing Action                                   |
| ----------------- | ------------- | ------------------------------------- | -------------------------------------------------- |
| quotation_item_id | STRING        | not null, unique                      | remove duplicates; null → Error_Record             |
| quotation_id      | STRING        | not null, valid FK                    | invalid FK/null → Error_Record                     |
| coverage_type     | STRING        | not null                              | uppercase standardization; null → Error_Record     |
| coverage_amount   | DECIMAL(18,2) | > 0                                   | round to 2 decimals; invalid → Error_Record        |
| deductible_amount | DECIMAL(18,2) | >= 0 and < coverage_amount            | round to 2 decimals; invalid → Error_Record        |
| created_date      | TIMESTAMP     | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record |
| updated_date      | TIMESTAMP     | NULLABLE, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record |

---

# DATABASE: INSURANCE_POLICY_DB

## policy_info

| Column            | Data Type     | Validation Rule                                                 | Cleansing Action                                  |
| ----------------- | ------------- | --------------------------------------------------------------- | ------------------------------------------------- |
| policy_id         | STRING        | not null, unique PK                                             | remove duplicates; null → Error_Record            |
| quotation_id      | STRING        | not null, valid FK                                              | invalid FK/null → Error_Record                    |
| customer_id       | STRING        | not null, valid FK                                              | invalid FK/null → Error_Record                    |
| provider_code     | STRING        | not null, valid FK                                              | uppercase standardization; invalid → Error_Record |
| policy_number     | STRING        | not null, unique, pattern match                                 | trim whitespace; null/duplicate → Error_Record    |
| policy_start_date | DATE          | not null, valid date, <= policy_end_date                        | ISO 8601 format; invalid → Error_Record           |
| policy_end_date   | DATE          | not null, valid date, > policy_start_date                       | ISO 8601 format; invalid → Error_Record           |
| policy_status     | STRING        | not null, allowed values: ISSUED · ACTIVE · EXPIRED · CANCELLED | uppercase standardization; invalid → Error_Record |
| premium_amount    | DECIMAL(18,2) | not null, > 0                                                   | round to 2 decimals; invalid → Error_Record       |
| issued_date       | DATE          | not null, valid date, <= today                                  | ISO 8601 format; future date → Error_Record       |
| last_updated      | TIMESTAMP     | nullable, valid timestamp                                       | ISO 8601 format                                   |
| operation_type    | STRING        | allowed values: I · U · D                                       | invalid → Error_Record                            |
| batch_date        | DATE          | valid date                                                      | ISO 8601 format                                   |
| source_system     | STRING        | not null                                                        | uppercase standardization                         |

---

## cancellation

| Column              | Data Type     | Validation Rule                            | Cleansing Action                                       |
| ------------------- | ------------- | ------------------------------------------ | ------------------------------------------------------ |
| cancellation_id     | STRING        | not null, unique PK                        | remove duplicates; null → Error_Record                 |
| policy_id           | STRING        | not null, valid FK                         | invalid FK/null → Error_Record                         |
| cancellation_date   | DATE          | not null, valid date, >= policy_start_date | ISO 8601 format; invalid → Error_Record                |
| cancellation_reason | STRING        | nullable                                   | trim whitespace                                        |
| refund_amount       | DECIMAL(18,2) | >= 0, numeric                              | round to 2 decimals; null → 0; negative → Error_Record |
| last_updated        | TIMESTAMP     | nullable, valid timestamp                  | ISO 8601 format                                        |
| operation_type      | STRING        | allowed values: I · U · D                  | invalid → Error_Record                                 |
| batch_date          | DATE          | valid date                                 | ISO 8601 format                                        |
| source_system       | STRING        | not null                                   | uppercase standardization                              |

---

## payment

| Column                | Data Type     | Validation Rule                                               | Cleansing Action                                  |
| --------------------- | ------------- | ------------------------------------------------------------- | ------------------------------------------------- |
| payment_id            | STRING        | not null, unique PK                                           | remove duplicates; null → Error_Record            |
| policy_id             | STRING        | not null, valid FK                                            | invalid FK/null → Error_Record                    |
| payment_date          | DATE          | not null, valid date, <= today                                | ISO 8601 format; future/null → Error_Record       |
| payment_method        | STRING        | allowed values: CREDIT CARD · BANK TRANSFER · CASH · E-WALLET | uppercase standardization                         |
| payment_status        | STRING        | not null, allowed values: PENDING · PAID · FAILED · REFUNDED  | uppercase standardization; invalid → Error_Record |
| payment_amount        | DECIMAL(18,2) | not null, > 0                                                 | round to 2 decimals; invalid → Error_Record       |
| transaction_reference | STRING        | not null, unique                                              | trim whitespace; null/duplicate → Error_Record    |
| last_updated          | TIMESTAMP     | nullable, valid timestamp                                     | ISO 8601 format                                   |
| operation_type        | STRING        | allowed values: I · U · D                                     | invalid → Error_Record                            |
| batch_date            | DATE          | valid date                                                    | ISO 8601 format                                   |
| source_system         | STRING        | not null                                                      | uppercase standardization                         |

---

# Status Standardization Rules

## Quotation Status

| Allowed Value |
| ------------- |
| QUOTED        |
| ACCEPTED      |
| REJECTED      |
| EXPIRED       |
| CONVERTED     |

## Policy Status

| Allowed Value |
| ------------- |
| ISSUED        |
| ACTIVE        |
| EXPIRED       |
| CANCELLED     |

## Payment Status

| Allowed Value |
| ------------- |
| PENDING       |
| PAID          |
| FAILED        |
| REFUNDED      |

---

# Standard Date & Timestamp Format

| Data Type | Standard Format         | Example                 |
| --------- | ----------------------- | ----------------------- |
| DATE      | yyyy-MM-dd              | 2026-05-26              |
| TIMESTAMP | yyyy-MM-dd HH:mm:ss.SSS | 2026-05-26 08:49:15.063 |
