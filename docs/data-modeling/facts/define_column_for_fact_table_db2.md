# Gold Layer Fact Tables — Field Design Document

### Insurance Analytics — Dimensional Model (Star Schema)

---

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

**Metadata name mapping (baseline alignment):**
| Implementation Field | Baseline Field |
|---------------------|----------------|
| _source_system | source_system |
| _batch_id | batch_id |

| Column          | Type      | Description                                        |
| --------------- | --------- | -------------------------------------------------- |
| _source_system   | STRING    | Source system name.                                |
| _batch_id        | STRING    | Processing batch identifier.                       |
| pipeline_run_id | STRING    | pipeline_run_id maps to the session identifier stored in log.audit_session                   |
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
| policy_number             | STRING        | Degenerate Dimension   | source policy number retained for traceability |
| quotation_id             | STRING        | Degenerate Dimension   | originating quotation identifier |
| customer_id             | STRING        | Degenerate Dimension   | source customer identifier |
| provider_code             | STRING        | Degenerate Dimension   | source provider identifier/code |
| quotation_key         | BIGINT        | FK → dim_quotation     | Reference to the originating quotation.                                  |
| customer_key          | BIGINT        | FK → dim_customer      | Reference to the customer who holds the policy.                          |
| provider_key          | BIGINT        | FK → dim_provider      | Reference to the insurance provider (direct from `policy_info.provider_code`). |
| agent_key             | BIGINT        | FK → dim_agent         | Reference to the agent who issued the policy.                            |
| package_key           | BIGINT        | FK → dim_package       | Reference to the insurance package selected.                             |
| policy_status_key     | BIGINT        | FK → dim_policy_status | Reference to the current policy status.                                  |
| vehicle_key           | BIGINT        | FK → dim_vehicle       | Reference to the insured vehicle (resolved via `policy_info.customer_id → vehicle.customer_id`). |
| issued_date_key       | INT           | FK → dim_date          | Policy issue date.                                                       |
| policy_start_date_key | INT           | FK → dim_date          | Policy start date.                                                       |
| policy_end_date_key   | INT           | FK → dim_date          | Policy end date.                                                         |
| premium_amount        | DECIMAL(18,2) | Measure                | Gross premium amount charged for the policy.                             |

### Design Notes

* `agent_key` is resolved via `policy_info.quotation_id → quotation.agent_id → dim_agent.agent_key`.
*  `package_key` is resolved via `policy_info.quotation_id → quotation.package_code → dim_package.package_key`.
* `provider_key` is resolved directly from `policy_info.provider_code → dim_provider.provider_key`.
* `vehicle_key` is resolved via `policy_info.customer_id → vehicle.customer_id → dim_vehicle.vehicle_key`.

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
| issued_date_key       | INT           | FK → dim_date           | Policy issued date key. Materialized during Gold ETL from policy_info.issued_date using payment.policy_id -> policy_info.policy_id, then mapped to dim_date.date_key. Used to calculate M-22 Average Payment Time without a fact-to-fact relationship in Power BI. |
| customer_key          | BIGINT        | FK → dim_customer       | Reference to the customer making the payment.                            |
| provider_key          | BIGINT        | FK → dim_provider       | Reference to the insurance provider receiving payment.                   |
| payment_status_key    | BIGINT        | FK → dim_payment_status | Reference to the payment status.                                         |
| payment_method_key    | BIGINT        | FK → dim_payment_method | Reference to the payment method used.                                    |
| vehicle_key           | BIGINT        | FK → dim_vehicle        | Reference to the insured vehicle (resolved via `payment.policy_id → policy_info.customer_id → vehicle.customer_id`). |
| payment_amount        | DECIMAL(18,2) | Measure                 | Total amount paid in the transaction.                                    |
| transaction_reference | STRING        | Degenerate Dimension    | Unique payment transaction reference.                                    |

### Design Notes

Dimension keys are resolved during Gold ETL processing using the policy relationship chain:

* `customer_key`:
  `payment.policy_id → policy_info.customer_id → dim_customer.customer_key`

* `provider_key`:
  `payment.policy_id → policy_info.provider_code → dim_provider.provider_key`

* `vehicle_key`:
  `payment.policy_id → policy_info.customer_id → vehicle.customer_id → dim_vehicle.vehicle_key`

* `issued_date_key`:
  `payment.policy_id → policy_info.policy_id → policy_info.issued_date → dim_date.date_key`
  Materialized during Gold ETL to support M-22 Average Payment Time without a fact-to-fact relationship in Power BI.

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
| vehicle_key             | BIGINT        | FK → dim_vehicle             | Reference to the insured vehicle (resolved via `cancellation.policy_id → policy_info.customer_id → vehicle.customer_id`). |
| refund_amount           | DECIMAL(18,2) | Measure                      | Refund amount issued upon cancellation.                                  |

### Design Notes

Dimension keys are resolved during Gold ETL processing using the policy relationship chain:

* `customer_key`:
  `cancellation.policy_id → policy_info.customer_id → dim_customer.customer_key`

* `provider_key`:
  `cancellation.policy_id → policy_info.provider_code → dim_provider.provider_key`

* `vehicle_key`:
  `cancellation.policy_id → policy_info.customer_id → vehicle.customer_id → dim_vehicle.vehicle_key`

---

# 5. Dimension Reference Summary

| Dimension Table         | fact_policy                                      | fact_payment                                                                        | fact_cancellation                                                                        |
| ----------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| dim_date                | 3x (issued, start, end)                          | 2x (issued, payment_date)                                                                   | 1x (cancellation_date)                                                                   |
| dim_customer            | Yes                                              | Yes (resolved via `payment.policy_id → policy_info`)                               | Yes (resolved via `cancellation.policy_id → policy_info`)                               |
| dim_agent               | Yes (resolved via quotation)                     | —                                                                                   | —                                                                                        |
| dim_provider            | Yes (direct from `policy_info.provider_code`)    | Yes (resolved via `payment.policy_id → policy_info`)                               | Yes (resolved via `cancellation.policy_id → policy_info`)                               |
| dim_package             | Yes (resolved via quotation)                     | —                                                                                   | —                                                                                        |
| dim_quotation           | Yes                                              | —                                                                                   | —                                                                                        |
| dim_policy              | Yes                                              | Yes                                                                                 | Yes                                                                                      |
| dim_policy_status       | Yes                                              | —                                                                                   | —                                                                                        |
| dim_payment_status      | —                                                | Yes                                                                                 | —                                                                                        |
| dim_payment_method      | —                                                | Yes                                                                                 | —                                                                                        |
| dim_cancellation_reason | —                                                | —                                                                                   | Yes                                                                                      |
| dim_vehicle             | Yes (via `policy_info.customer_id → vehicle.customer_id`) | Yes (via `payment.policy_id → policy_info.customer_id → vehicle.customer_id`) | Yes (via `cancellation.policy_id → policy_info.customer_id → vehicle.customer_id`) |

---

# 6. Measures Summary

| Fact Table        | Measure        | Type          | Description                              |
| ----------------- | -------------- | ------------- | ---------------------------------------- |
| fact_policy       | premium_amount | DECIMAL(18,2) | Gross policy premium amount.             |
| fact_payment      | payment_amount | DECIMAL(18,2) | Amount paid per transaction.             |
| fact_cancellation | refund_amount  | DECIMAL(18,2) | Refund amount issued after cancellation. |
