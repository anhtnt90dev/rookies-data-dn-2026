Gold Layer Fact Tables
Field Design Document
Insurance Analytics — Dimensional Model (Star Schema)


# 1. Purpose
This document defines the proposed Gold Layer Fact Table structures for the Insurance Analytics dimensional model. It captures the field-level design of the three core fact tables derived from the star schema ERD.
The design identifies foreign keys (dimension references), measures, and grain declarations for each fact table. These definitions serve as the authoritative reference for downstream BI development, data validation, and pipeline engineering.

Scope of this document — the following three fact tables:
Fact_Policy
Fact_Payment
Fact_Cancellation

# 2. General Fact Table Design Standards

# 3. Common Technical Columns
All three fact tables share the following audit and metadata columns:


# 4. Fact Table Structures
## 4.1 Fact_Policy

Grain: One row per issued insurance policy
Source: policy_info (Silver Layer)

## 4.2 Fact_Payment

Grain: One row per payment transaction against a policy
Source: payment (Silver Layer)

## 4.3 Fact_Cancellation

Grain: One row per policy cancellation event
Source: cancellation (Silver Layer)

# 5. Dimension Reference Summary
The table below lists all dimension tables referenced across the three fact tables, showing which facts consume each dimension.


# 6. Measures Summary
The following measures are defined across the three fact tables. All measures are additive unless stated otherwise.


# 7. Open Items and Recommendations

# 8. Revision History

| Column | Type | Description |
|---|---|---|
| source_system | STRING | Source system name (e.g., core_insurance). |
| created_at | TIMESTAMP | Timestamp when the fact row was first loaded into Gold. |
| updated_at | TIMESTAMP | Timestamp when the fact row was last updated in Gold. |

**fact_policy**
| Column | Type | FK / Measure | Description |
|---|---|---|---|
| customer_key | BIGINT | FK → dim_customer | Reference to the customer who holds the policy. |
| provider_key | BIGINT | FK → dim_provider | Reference to the insurance provider. |
| policy_status_key | BIGINT | FK → dim_policy_status | Reference to the current policy status. |
| policy_start_date_key | INT | FK → dim_date | Date key for the policy start date (YYYYMMDD). |
| policy_end_date_key | INT | FK → dim_date | Date key for the policy end date (YYYYMMDD). |
| issued_date_key | INT | FK → dim_date | Date key for the policy issue date (YYYYMMDD). |
| quotation_key | BIGINT | FK → dim_quotation | Reference to the originating quotation. |
| agent_key | BIGINT | FK → aim_agent | Reference to the agent who issued the policy. |
| package_key | BIGINT | FK → dim_package | Reference to the insurance package selected. |
| issued_premium_amount | DECIMAL(18,2) | Measure (Additive) | Gross premium amount charged for the policy. |
| source_system | STRING | Audit | Source system identifier. |
| created_at | TIMESTAMP | Audit | Gold load timestamp. |
| updated_at | TIMESTAMP | Audit | Gold last update timestamp. |

**fact_payment**
| Column | Type | FK / Measure | Description |
|---|---|---|---|
| payment_date_key | INT | FK → dim_date | Date key for the payment date (YYYYMMDD). |
| payment_status_key | BIGINT | FK → dim_payment_status | Reference to the payment status (e.g., Successful, Failed, Pending). |
| payment_method_key | BIGINT | FK → dim_payment_method | Reference to the payment method used. |
| customer_key | BIGINT | FK → dim_customer | Reference to the customer making the payment. |
| provider_key | BIGINT | FK → dim_provider | Reference to the insurance provider receiving payment. |
| payment_amount | DECIMAL(18,2) | Measure (Additive) | Total amount paid in the transaction. |
| transaction_reference | STRING | Degenerate Dimension | Unique transaction reference from source system. Stored directly (no associated dimension). |
| source_system | STRING | Audit | Source system identifier. |
| created_at | TIMESTAMP | Audit | Gold load timestamp. |
| updated_at | TIMESTAMP | Audit | Gold last update timestamp. |

**fact_cancellation**
| Column | Type | FK / Measure | Description |
|---|---|---|---|
| cancellation_reason_key | BIGINT | FK → dim_cancellation_reason | Reference to the reason for cancellation. |
| cancellation_date_key | INT | FK → dim_date | Date key for the cancellation date (YYYYMMDD). |
| provider_key | BIGINT | FK → dim_provider | Reference to the insurance provider involved. |
| customer_key | BIGINT | FK → dim_customer | Reference to the customer whose policy was cancelled. |
| refund_amount | DECIMAL(18,2) | Measure (Additive) | Refund amount issued upon cancellation. May be 0 if no refund applies. |
| source_system | STRING | Audit | Source system identifier. |
| created_at | TIMESTAMP | Audit | Gold load timestamp. |
| updated_at | TIMESTAMP | Audit | Gold last update timestamp. |

**Dimension Usage by Fact Tables**
| Dimension Table | fact_policy | fact_payment | fact_cancellation |
|---|---|---|---|
| dim_date | 3x (start, end, issued) | 1x (payment_date) | 1x (cancellation_date) |
| dim_customer | Yes | Yes | Yes |
| dim_provider | Yes | Yes | Yes |
| dim_policy_status | Yes | — | — |
| dim_agent | Yes | — | — |
| dim_package | Yes | — | — |
| dim_quotation | Yes | — | — |
| dim_payment_status | — | Yes | — |
| dim_payment_method | — | Yes | — |
| dim_cancellation_reason | — | — | Yes |
