# Gold Layer Temporal, Referential & Business Logic Integrity Standards

This document defines the strict referential, temporal, and business logic standards for data entering the Gold Layer (Reporting-Ready Star Schema). 

These standards serve a dual purpose:
1. **For ETL Development & Code Review**: Ensures that PySpark loading pipelines correctly resolve dimension keys, handle SCD Type 2 point-in-time joins, and log anomalies without silent data drops.
2. **For Source Data Specification**: Defines the rules for generating upstream source data (CRM systems and JSON transaction files) to ensure they form a realistic, consistent, and logically valid insurance business history. Conforming to these rules guarantees that data flowing through Bronze, Silver, and Gold layers resolves correctly without producing unresolved keys (`-1`) or breaking downstream dashboard metrics.

---

## 1. Core Principles of Source Data Integrity

To maintain a consistent and realistic insurance database, any upstream data source or test dataset must strictly adhere to the following core rules:

*   **Pre-existence Rule**: Every entity (Customer, Agent, Provider, Vehicle) must be registered in the system before any transactions (Quotations, Policies, Payments, Cancellations) referencing them are created.
*   **Referential Completeness**: Transactions must never reference non-existent business IDs (such as an unknown `agent_id` or `customer_id`), except when explicitly creating negative test cases to test error-handling pipelines.
*   **Linear Chronology**: The lifecycle of a single policy must flow logically forward in time:
    $$\text{Quotation Date} \le \text{Policy Start Date} \le \text{Payment/Activation Date} \le \text{Cancellation/Expiry Date}$$

---

## 2. Temporal Integrity Constraint (Pre-existence Rule)

An entity (Agent, Customer, Provider, or Vehicle) must be registered in the system before generating transactions. Therefore, the dimension's creation date must be chronologically prior to or equal to any transaction date referencing it.

### Checklist Items:
*   [ ] **Creation Order Validation**: Ensure the PySpark join or lookup logic guarantees that:
    $$\text{Dimension.created\_date} \le \text{Fact.transaction\_date}$$
    *   `dim_customer.effective_from` $\le$ `fact_quotation.quotation_date` (or `quotation_at`)
    *   `dim_agent.effective_from` $\le$ `fact_quotation.quotation_date` (or `quotation_at`)
    *   `dim_provider.effective_from` $\le$ `fact_quotation.quotation_date` (or `quotation_at`)
    *   `dim_vehicle.effective_from` $\le$ `fact_quotation.quotation_date` (based on customer vehicle resolution)
*   [ ] **Business Rule Enforcement**: Verify that if a transaction's timestamp is strictly before the dimension entity's creation timestamp, the row is flagged. It should not be matched to a normal dimension surrogate key.

### Query to Detect Violations statically (for ad-hoc checks):
```sql
SELECT 
    f.policy_id,
    f.policy_start_date,
    d.provider_code,
    d.created_date AS provider_created_date
FROM gold.fact_policy f
JOIN gold.dim_provider d ON f.provider_key = d.provider_key
WHERE d.created_date > f.policy_start_date;
```

---

## 3. Zero Tolerance for Unresolved Keys in Standard Flows

Surrogate keys in fact tables must resolve to valid dimensions and must not fallback to `-1` (Unknown) unless the business key in the source system is legitimately NULL.

### Checklist Items:
*   [ ] **Inner/Left Join Assertions**: In the loading notebooks, verify that lookups against SCD1 or SCD2 dimensions do not use a blind `.fillna(-1)` or `coalesce(lookup_key, -1)` for cases where the business key was populated.
*   [ ] **Active Validation**: If `silver.policy.agent_id` is NOT NULL, then the resolved `agent_key` in `gold.fact_policy` must NOT be `-1`.
*   [ ] **Error Tracing**: Any lookup returning `-1` for a non-null business key must be intercepted in the code and logged as a lookup failure.

---

## 4. SCD Type 2 Point-in-Time Join Contiguity

For dimensions that track historical changes (e.g., `dim_customer`, `dim_provider`, `dim_agent`, `dim_vehicle`), fact records must join to the dimension version that was active at the exact time of the transaction.

### Checklist Items:
*   [ ] **Non-Overlapping Ranges**: Verify that the point-in-time join condition is structured exactly as:
    ```python
    # Ensure transaction date falls between effective dates of the dimension version
    (fact_df["transaction_date"] >= dim_df["effective_from"]) & \
    (fact_df["transaction_date"] <= dim_df["effective_to"])
    ```
*   [ ] **Uniqueness of Lookup**: Confirm that for any given business key and transaction timestamp, only **one** dimension row is matched. Overlapping ranges in SCD2 table rows will cause row explosion in the fact table.
*   [ ] **Open-Ended Active Records**: Active dimension records must have an `effective_to` set to a high-bound default date (e.g., `9999-12-31 23:59:59`) to ensure joins do not fail for transactions occurring after the latest update.

---

## 5. Late-Arriving Dimension Event Detection

A late-arriving dimension occurs when a transaction is ingested before the corresponding master data record has reached the Gold layer.

### Checklist Items:
*   [ ] **Temporal Mismatch Identification**: If a lookup fails because the transaction timestamp is older than the earliest `effective_from` of the dimension record:
    *   Do **NOT** write surrogate key = `-1` silently to Gold.
    *   Route the transaction to `log.invalid_record` with the error category `TEMPORAL_MISMATCH_ERROR`.
*   [ ] **Required Fields for Logging**:
    *   `layer`: `'GOLD'`
    *   `target_table`: Name of the fact table (e.g. `'gold.fact_policy'`).
    *   `record_key`: The transaction primary key (e.g. `policy_id`).
    *   `error_column`: The failing key column (e.g. `provider_key`).
    *   `error_reason`: `'Transaction date [YYYY-MM-DD] is prior to dimension registration date [YYYY-MM-DD] for business key [KEY]'`.
    *   `raw_data`: Full JSON payload of the Silver record.

---

## 6. Business Lifecycle & Status Transition Rules

Data sources must maintain consistent entity states and transition pathways across the entire policy lifecycle.

### 6.1 Quotation Status Transition Flow
*   Quotations progress through: `QUOTED` $\rightarrow$ `ACCEPTED` $\rightarrow$ `CONVERTED` (or terminal `REJECTED`/`EXPIRED`).
*   **Conversion Contiguity**:
    *   If a quotation has status `CONVERTED`, the `converted_flag` in `fact_quotation` must be `true`, and a corresponding policy record MUST exist in `silver.policy` with a matching `quotation_id`.
    *   If a quotation has status `ACCEPTED`, `REJECTED`, or `EXPIRED`, no corresponding policy record should exist in `silver.policy`.
    *   For `EXPIRED` quotations, the quotation date plus validity period must be less than the current simulation date.

### 6.2 Policy & Payment Status Integration
*   Policies progress through: `ISSUED` $\rightarrow$ `ACTIVE` $\rightarrow$ `EXPIRED` (or `CANCELLED`).
*   **Payment Triggered Activation**:
    *   A policy in `ISSUED` status indicates the contract is created but coverage is not yet active (e.g., because payment is `PENDING` or `FAILED`).
    *   A policy status becomes `ACTIVE` if and only if:
        1. At least one payment transaction associated with the policy has status `PAID`.
        2. The current simulation date is $\ge$ `policy_start_date`.
    *   An `ACTIVE` policy whose current simulation date exceeds `policy_end_date` transitions naturally to `EXPIRED` status.
*   **Cancellation Integrity**:
    *   If a policy status is `CANCELLED`, a corresponding cancellation record MUST exist in `silver.cancellation`.
    *   The `cancellation_at` timestamp must satisfy:
        $$\text{policy\_start\_date} \le \text{cancellation\_at} \le \text{policy\_end\_date}$$
    *   The `refund_amount` in the cancellation record must be pro-rated based on the remaining unused policy term, and must not exceed the original premium:
        $$\text{refund\_amount} = \text{premium\_amount} \times \frac{\text{policy\_end\_date} - \text{cancellation\_at}}{\text{policy\_end\_date} - \text{policy\_start\_date}}$$

---

## 7. Dashboard Metric & Business Data Distribution Requirements

To ensure that downstream Power BI dashboards display realistic, high-fidelity business patterns and valid metric calculations, the source data must satisfy the following distribution requirements:

### 7.1 Realistic Funnel Conversions (Dashboard 1)
*   **Status Distribution**: Avoid 100% conversion rates which make funnel visualizations meaningless. Maintain a realistic sales funnel ratio:
    *   `Total Quotations` (M-01) $= 100\%$
    *   `Accepted Quotations` (M-02) $\approx 55\% - 65\%$ of Total Quotations
    *   `Policies Issued` (M-03) $\approx 35\% - 45\%$ of Total Quotations (representing $\approx 60\% - 75\%$ of Accepted)
    *   `Policies In Force` (M-11) $\approx 30\% - 40\%$ of Total Quotations (representing $\approx 85\% - 95\%$ of Issued)
*   **Funnel Gaps**:
    *   Ensure there are quotations with status `ACCEPTED` that have **not** been converted to policies, creating a realistic **NTU (Not Taken Up) Gap** (M-32) representing lost opportunities in the final stages.
    *   Ensure there are policies with status `ISSUED` that are **not** `ACTIVE` (e.g. because their payment is `PENDING`), creating a **Funnel Drop-off: Issued $\rightarrow$ In Force** (M-13).

### 7.2 Premium Payment & Collection Tracking (Dashboard 2)
*   **Payment Collections**: Ensure a mix of payment statuses to display collection health:
    *   $\approx 85\% - 90\%$ of payment transactions should be `PAID` (Total Collected Premium).
    *   $\approx 5\% - 10\%$ should be `PENDING` (Pending Payments).
    *   $\approx 5\%$ should be `FAILED` (Failed Payment Rate).
*   **Payment Aging Distribution**: To populate the payment aging horizontal bar charts (M-16 to M-20), pending payments must have a realistic distribution of `aging_days` (difference between payment due/issued date and current date):
    *   **0-7 Days**: $\approx 40\%$ of pending payments (recent bills)
    *   **8-30 Days**: $\approx 30\%$ of pending payments (mid-term collections)
    *   **31-60 Days**: $\approx 15\%$ of pending payments (overdue/at risk)
    *   **61-90 Days**: $\approx 10\%$ of pending payments (high risk)
    *   **>90 Days**: $\approx 5\%$ of pending payments (very high risk/potential write-offs)
*   **Average Payment Time**: Paid payments must have payment dates ranging from 0 to 15 days after the policy issue date, resulting in a realistic **Average Payment Time (Days)** (M-22) (e.g., average of 3-5 days).

### 7.3 Agent Performance & Regional Variance
*   **Regional Allocations**: Agents must be distributed across multiple regions (e.g., Hanoi, HCMC, Da Nang) in `dim_agent`.
*   **Performance Differences**:
    *   Do not distribute conversion rates uniformly. Designate some agents as high performers (e.g., conversion rate $\approx 45\% - 55\%$) and others as lower performers (e.g., conversion rate $\approx 15\% - 25\%$).
    *   Ensure that the regional conversion rates differ (e.g., Hanoi average $\approx 35\%$, HCMC average $\approx 30\%$). This ensures the **Agent vs Regional Benchmark** KPI (N2-06) generates meaningful positive and negative delta metrics for coaching/incentive analysis.

### 7.4 Provider Market Share & Pricing Strategy
*   **Provider Distribution**: Map policies across multiple providers (e.g., Bao Viet - BV, Liberty, PVI, MIC) in `dim_provider`.
*   **Provider Strategy Matrix**:
    *   **Bao Viet (BV)**: High volume share ($\approx 40\% - 45\%$), high conversion rate ($\approx 35\% - 40\%$), but lower average premium (e.g., average $\approx 3,000,000$ VND).
    *   **Liberty**: Lower volume share ($\approx 15\% - 20\%$), lower conversion rate ($\approx 20\% - 25\%$), but high average premium (e.g., average $\approx 8,000,000$ VND), indicating premium segment targeting.
    *   This differentiation directly feeds the **Conversion Rate by Provider** (N1-01), **Avg Premium by Provider** (N1-02), and **Volume Share by Provider** (N1-03) donut and bar charts.

### 7.5 Time Intelligence & Trend Integrity
*   **Timeline Coverage**: The generated source transactions must span at least **two full calendar years** (e.g., 2025 and 2026) to ensure:
    *   Year-over-Year (YoY) comparison measures (M-20 to M-24, M-09 in DB2) calculate correctly instead of returning blank.
    *   Growth metrics like **YoY Quotation Growth %** (M-21) and **YoY Premium Growth %** (M-23) show realistic, non-zero growth rates (e.g., $+10\%$ to $+20\%$).
*   **Seasonality Patterns**: Transaction volume should vary by month (e.g., higher volume during year-end renewal periods in November/December and post-Tet periods in February/March) to create realistic trend lines for **Quotation Count by Month** (M-15) and **Policies Issued by Month** (M-16).
