Gold Layer Fact Tables
Field Design Document
Insurance Analytics — Dimensional Model (Star Schema)


# Gold Layer Fact Tables

## Field Design Document

### Insurance Analytics — Dimensional Model (Star Schema)

# 1. Purpose

This document defines the proposed Gold Layer Fact Table structures for the Insurance Analytics dimensional model. It captures the field-level design of the three core fact tables derived from the Gold star schema.

The design identifies foreign keys (dimension references), measures, degenerate dimensions, audit fields, and grain declarations for each fact table. These definitions serve as the authoritative reference for downstream BI development, data validation, and pipeline engineering.

**Scope of this document**

* fact_policy
* fact_payment
* fact_cancellation

---

# 2. General Fact Table Design Standards

| Standard              | Description                                                                |
| --------------------- | -------------------------------------------------------------------------- |
| Grain                 | Each fact table is defined at the most atomic level of its business event. |
| Surrogate Keys        | All dimension references use surrogate keys (`BIGINT`).                    |
| Date Keys             | All dates reference `dim_date` using `YYYYMMDD` integer keys.              |
| Measures              | Only additive or semi-additive numeric values are stored as measures.      |
| Degenerate Dimensions | Business identifiers required for traceability remain in fact tables.      |
| Audit & CDC           | All facts include audit, batch lineage, and soft-delete tracking fields.   |
| Soft Delete           | Deleted records are retained and managed using `is_deleted`.               |

---

# 3. Common Technical Columns

All fact tables share the following audit and metadata columns.

| Column          | Type      | Description                                        |
| --------------- | --------- | -------------------------------------------------- |
| _source_system  | STRING    | Source system name.                                |
| _batch_id       | STRING    | Processing batch identifier.                       |
| pipeline_run_id | STRING    | Pipeline execution identifier.                     |
| is_deleted      | TINYINT   | Soft delete flag (0 = Active, 1 = Deleted).        |
| deleted_at      | TIMESTAMP | Timestamp when the record was marked as deleted.   |
| delete_batch_id | STRING    | Batch identifier that processed the delete event.  |
| created_at      | TIMESTAMP | Timestamp when the row was first loaded into Gold. |
| updated_at      | TIMESTAMP | Timestamp when the row was last updated in Gold.   |

---

# 4. Fact Table Structures

## 4.1 fact_policy

**Grain:** One row per issued insurance policy

**Source:** policy_info (Silver Layer)

| Column                | Type          | FK / Measure           | Description                                                              |
| --------------------- | ------------- | ---------------------- | ------------------------------------------------------------------------ |
| policy_key            | BIGINT        | FK → dim_policy        | Shared policy dimension surrogate key.                                   |
| policy_id             | STRING        | Degenerate Dimension   | Source policy identifier retained for traceability and grain validation. |
| quotation_key         | BIGINT        | FK → dim_quotation     | Reference to the originating quotation.                                  |
| customer_key          | BIGINT        | FK → dim_customer      | Reference to the customer who holds the policy.                          |
| provider_key          | BIGINT        | FK → dim_provider      | Reference to the insurance provider.                                     |
| agent_key             | BIGINT        | FK → dim_agent         | Reference to the agent who issued the policy.                            |
| package_key           | BIGINT        | FK → dim_package       | Reference to the insurance package selected.                             |
| policy_status_key     | BIGINT        | FK → dim_policy_status | Reference to the current policy status.                                  |
| vehicle_key           | BIGINT        | FK → dim_vehicle       | Reference to the insured vehicle.                                        |
| issued_date_key       | INT           | FK → dim_date          | Policy issue date.                                                       |
| policy_start_date_key | INT           | FK → dim_date          | Policy start date.                                                       |
| policy_end_date_key   | INT           | FK → dim_date          | Policy end date.                                                         |
| premium_amount        | DECIMAL(18,2) | Measure                | Gross premium amount charged for the policy.                             |

### Design Notes

* `agent_key` and `package_key` are resolved through `quotation_id → quotation` during Gold ETL processing.

---

## 4.2 fact_payment

**Grain:** One row per payment transaction against a policy

**Source:** payment (Silver Layer)

| Column                | Type          | FK / Measure            | Description                                                              |
| --------------------- | ------------- | ----------------------- | ------------------------------------------------------------------------ |
| policy_key            | BIGINT        | FK → dim_policy         | Shared policy dimension surrogate key.                                   |
| payment_id            | STRING        | Degenerate Dimension    | Source payment identifier retained for traceability.                     |
| policy_id             | STRING        | Degenerate Dimension    | Source policy identifier retained for traceability and grain validation. |
| payment_date_key      | INT           | FK → dim_date           | Payment date.                                                            |
| customer_key          | BIGINT        | FK → dim_customer       | Reference to the customer making the payment.                            |
| provider_key          | BIGINT        | FK → dim_provider       | Reference to the insurance provider receiving payment.                   |
| payment_status_key    | BIGINT        | FK → dim_payment_status | Reference to the payment status.                                         |
| payment_method_key    | BIGINT        | FK → dim_payment_method | Reference to the payment method used.                                    |
| vehicle_key           | BIGINT        | FK → dim_vehicle        | Reference to the insured vehicle.                                        |
| payment_amount        | DECIMAL(18,2) | Measure                 | Total amount paid in the transaction.                                    |
| transaction_reference | STRING        | Degenerate Dimension    | Unique payment transaction reference.                                    |

### Design Notes

* `customer_key`, `provider_key`, and `vehicle_key` are resolved through `policy_id → policy_info` during Gold ETL processing.

---

## 4.3 fact_cancellation

**Grain:** One row per policy cancellation event

**Source:** cancellation (Silver Layer)

| Column                  | Type          | FK / Measure                 | Description                                                              |
| ----------------------- | ------------- | ---------------------------- | ------------------------------------------------------------------------ |
| policy_key              | BIGINT        | FK → dim_policy              | Shared policy dimension surrogate key.                                   |
| cancellation_id         | STRING        | Degenerate Dimension         | Source cancellation identifier retained for traceability.                |
| policy_id               | STRING        | Degenerate Dimension         | Source policy identifier retained for traceability and grain validation. |
| cancellation_date_key   | INT           | FK → dim_date                | Cancellation date.                                                       |
| customer_key            | BIGINT        | FK → dim_customer            | Reference to the customer whose policy was cancelled.                    |
| provider_key            | BIGINT        | FK → dim_provider            | Reference to the insurance provider involved.                            |
| cancellation_reason_key | BIGINT        | FK → dim_cancellation_reason | Reference to the cancellation reason.                                    |
| vehicle_key             | BIGINT        | FK → dim_vehicle             | Reference to the insured vehicle.                                        |
| refund_amount           | DECIMAL(18,2) | Measure                      | Refund amount issued upon cancellation.                                  |

### Design Notes

* `customer_key`, `provider_key`, and `vehicle_key` are resolved through `policy_id → policy_info` during Gold ETL processing.

---

# 5. Dimension Reference Summary

| Dimension Table         | fact_policy                  | fact_payment              | fact_cancellation         |
| ----------------------- | ---------------------------- | ------------------------- | ------------------------- |
| dim_date                | 3x (issued, start, end)      | 1x (payment_date)         | 1x (cancellation_date)    |
| dim_customer            | Yes                          | Yes (resolved via policy) | Yes (resolved via policy) |
| dim_agent               | Yes (resolved via quotation) | —                         | —                         |
| dim_provider            | Yes (resolved via quotation) | Yes (resolved via policy) | Yes (resolved via policy) |
| dim_package             | Yes (resolved via quotation) | —                         | —                         |
| dim_quotation           | Yes                          | —                         | —                         |
| dim_policy              | Yes                          | Yes                       | Yes                       |
| dim_policy_status       | Yes                          | —                         | —                         |
| dim_payment_status      | —                            | Yes                       | —                         |
| dim_payment_method      | —                            | Yes                       | —                         |
| dim_cancellation_reason | —                            | —                         | Yes                       |
| dim_vehicle             | Yes (resolved via quotation) | Yes (resolved via policy) | Yes (resolved via policy) |

* Resolved through policy relationship during Gold ETL.

---

# 6. Measures Summary

| Fact Table        | Measure        | Type          | Description                              |
| ----------------- | -------------- | ------------- | ---------------------------------------- |
| fact_policy       | premium_amount | DECIMAL(18,2) | Gross policy premium amount.             |
| fact_payment      | payment_amount | DECIMAL(18,2) | Amount paid per transaction.             |
| fact_cancellation | refund_amount  | DECIMAL(18,2) | Refund amount issued after cancellation. |

---

# 7. Open Items and Recommendations

| # | Item                                                  | Recommendation                                                                                          |
| - | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 1 | Vehicle context in payment and cancellation analytics | Confirm reporting requirements. Vehicle slicing currently resolves through policy relationships.        |
| 2 | Soft-delete handling                                  | All KPI calculations should filter `is_deleted = 0`.                                                    |
| 3 | Audit and CDC lineage                                 | Standardize usage of `_batch_id`, `pipeline_run_id`, and `delete_batch_id` across all Gold fact tables. |

---

# 8. Revision History

| Version | Date       | Author           | Notes                                                                                                                                                                    |
| ------- | ---------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1.0     | 2026-06-02 | Data Engineering | Initial fact table design document.                                                                                                                                      |
| 1.1     | 2026-06-02 | Data Engineering | Added dim_policy integration, degenerate identifiers, audit and CDC fields, vehicle dimension support, resolved relationship notes, and documentation alignment updates. |

**Document Location**

`docs/data-modeling/dimensional-design/gold-layer-fact-tables.md`
