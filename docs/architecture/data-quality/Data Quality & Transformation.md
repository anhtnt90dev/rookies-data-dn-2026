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

# DATABASE: INSURANCE_CRM_DB

## customers

| Column       | Data Type | Validation Rule                               | Cleansing Action                                   |
| ------------ | --------- | --------------------------------------------- | -------------------------------------------------- |
| customer_id  | STRING    | not null, unique                              | remove duplicates; null → Error_Record             |
| full_name    | STRING    | not null                                      | trim spaces; null → Error_Record                   |
| gender       | STRING    | valid values (Male, Female, Other)            | capitalize standardization                         |
| dob          | DATE      | valid date, format yyyy-MM-dd, < current_date | convert to ISO 8601 format; invalid → Error_Record |
| phone_number | STRING    | must contain exactly 10 digits                | trim spaces; invalid → Error_Record                |
| email        | STRING    | valid email format                            | lowercase standardization; invalid → Error_Record  |
| city         | STRING    | nullable                                      | capitalize standardization                         |
| district     | STRING    | nullable                                      | capitalize standardization                         |
| created_date | TIMESTAMP | valid date, <= current_date                   | convert to ISO 8601 format; invalid → Error_Record |

---

## vehicle

| Column           | Data Type     | Validation Rule                       | Cleansing Action                                       |
| ---------------- | ------------- | ------------------------------------- | ------------------------------------------------------ |
| vehicle_id       | STRING        | not null, unique                      | remove duplicates; null → Error_Record                 |
| customer_id      | STRING        | not null, valid FK                    | invalid FK/null → Error_Record                         |
| plate_number     | STRING        | not null, unique                      | uppercase standardization; invalid/null → Error_Record |
| vehicle_brand    | STRING        | nullable                              | capitalize standardization                             |
| vehicle_model    | STRING        | nullable                              | capitalize standardization                             |
| manufacture_year | INT           | valid year, <= current_year           | invalid values → Error_Record                          |
| vehicle_value    | DECIMAL(18,2) | > 0                                   | invalid values → Error_Record                          |
| created_date     | TIMESTAMP     | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record     |
| updated_date     | TIMESTAMP     | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record     |

---

## agents

| Column       | Data Type | Validation Rule                       | Cleansing Action                                   |
| ------------ | --------- | ------------------------------------- | -------------------------------------------------- |
| agent_id     | STRING    | not null, unique                      | remove duplicates; null → Error_Record             |
| agent_name   | STRING    | nullable                              | trim spaces                                        |
| region       | STRING    | nullable                              | capitalize standardization                         |
| branch       | STRING    | nullable                              | capitalize standardization                         |
| manager_name | STRING    | nullable                              | capitalize standardization                         |
| created_date | TIMESTAMP | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record |

---

## insurance_providers

| Column         | Data Type | Validation Rule                       | Cleansing Action                                          |
| -------------- | --------- | ------------------------------------- | --------------------------------------------------------- |
| provider_code  | STRING    | not null, unique                      | uppercase standardization; duplicates/null → Error_Record |
| provider_name  | STRING    | not null                              | trim spaces; null → Error_Record                          |
| provider_group | STRING    | not null                              | capitalize standardization; null → Error_Record           |
| active_flag    | INT       | valid values (0,1)                    | invalid → Error_Record                                    |
| created_date   | TIMESTAMP | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record        |
| updated_date   | TIMESTAMP | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record        |

---

## quotation

| Column                | Data Type     | Validation Rule                                               | Cleansing Action                                   |
| --------------------- | ------------- | ------------------------------------------------------------- | -------------------------------------------------- |
| quotation_id          | STRING        | not null, unique                                              | remove duplicates; null → Error_Record             |
| customer_id           | STRING        | not null, valid FK                                            | invalid FK/null → Error_Record                     |
| agent_id              | STRING        | not null, valid FK                                            | invalid FK → Error_Record                          |
| provider_code         | STRING        | not null, valid FK                                            | invalid FK/null → Error_Record                     |
| quotation_date        | DATE          | valid date, format yyyy-MM-dd                                 | convert to ISO 8601 format; invalid → Error_Record |
| quotation_status      | STRING        | valid values (QUOTED, ACCEPTED, REJECTED, EXPIRED, CONVERTED) | uppercase standardization; invalid → Error_Record  |
| package_code          | STRING        | not null                                                      | uppercase standardization; null → Error_Record     |
| premium_amount        | DECIMAL(18,2) | > 0                                                           | round to 2 decimals; invalid → Error_Record        |
| quotation_expiry_date | DATE          | format yyyy-MM-dd, >= quotation_date                          | convert to ISO 8601 format; invalid → Error_Record |
| created_date          | TIMESTAMP     | not null, valid date, <= current_date                         | convert to ISO 8601 format; invalid → Error_Record |
| updated_date          | TIMESTAMP     | not null, valid date, <= current_date                         | convert to ISO 8601 format; invalid → Error_Record |

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
| updated_date      | TIMESTAMP     | not null, valid date, <= current_date | convert to ISO 8601 format; invalid → Error_Record |

---

# DATABASE: INSURANCE_POLICY_DB

## policy_info

| Column            | Data Type     | Validation Rule                           | Cleansing Action                                       |
| ----------------- | ------------- | ----------------------------------------- | ------------------------------------------------------ |
| policy_id         | STRING        | not null, unique PK                       | remove duplicates; null → Error_Record                 |
| quotation_id      | STRING        | not null, valid FK                        | invalid FK/null → Error_Record                         |
| customer_id       | STRING        | not null, valid FK                        | invalid FK/null → Error_Record                         |
| provider_code     | STRING        | not null, valid FK                        | uppercase standardization; invalid → Error_Record      |
| policy_number     | STRING        | not null, unique, pattern match           | trim whitespace; null/duplicate → Error_Record         |
| policy_start_date | DATE          | not null, valid date, <= policy_end_date  | ISO 8601 format; invalid/null → Error_Record           |
| policy_end_date   | DATE          | not null, valid date, > policy_start_date | ISO 8601 format; invalid/null → Error_Record           |
| policy_status     | STRING        | not null, allowed values                  | uppercase standardization; invalid/null → Error_Record |
| premium_amount    | DECIMAL(18,2) | not null, > 0, numeric                    | round to 2 decimals; invalid/null → Error_Record       |
| issued_date       | DATE          | not null, valid date, <= today            | ISO 8601 format; future date → Error_Record            |

---

## cancellation

| Column              | Data Type     | Validation Rule                            | Cleansing Action                                       |
| ------------------- | ------------- | ------------------------------------------ | ------------------------------------------------------ |
| cancellation_id     | STRING        | not null, unique PK                        | remove duplicates; null → Error_Record                 |
| policy_id           | STRING        | not null, valid FK                         | invalid FK/null → Error_Record                         |
| cancellation_date   | DATE          | not null, valid date, >= policy_start_date | ISO 8601 format; invalid/null → Error_Record           |
| cancellation_reason | STRING        | nullable                                   |                                                        |
| refund_amount       | DECIMAL(18,2) | >= 0, numeric                              | round to 2 decimals; null → 0; negative → Error_Record |

---

## payment

| Column                | Data Type     | Validation Rule                | Cleansing Action                                       |
| --------------------- | ------------- | ------------------------------ | ------------------------------------------------------ |
| payment_id            | STRING        | not null, unique PK            | remove duplicates; null → Error_Record                 |
| policy_id             | STRING        | not null, valid FK             | invalid FK/null → Error_Record                         |
| payment_date          | DATE          | not null, valid date, <= today | ISO 8601 format; future/null → Error_Record            |
| payment_method        | STRING        | allowed values                 | uppercase standardization                              |
| payment_status        | STRING        | not null, allowed values       | uppercase standardization; null/invalid → Error_Record |
| payment_amount        | DECIMAL(18,2) | not null, > 0, numeric         | round to 2 decimals; invalid/null → Error_Record       |
| transaction_reference | STRING        | not null, unique               | trim whitespace; null/duplicate → Error_Record         |

---

# Standard Date & Timestamp Format

| Data Type | Standard Format         | Example                 |
| --------- | ----------------------- | ----------------------- |
| DATE      | yyyy-MM-dd              | 2026-05-26              |
| TIMESTAMP | yyyy-MM-dd HH:mm:ss.SSS | 2026-05-26 08:49:15.063 |
