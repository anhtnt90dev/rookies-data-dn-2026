# Gold Layer Temporal & Referential Integrity Code Review Checklist

This document defines the code review standards and logic verification guidelines for PySpark loading pipelines in the Gold Layer. 

### Dual Purpose:
1.  **For ETL Developers & Reviewers**: To keep the runtime validation notebook (`nb_gold_validate_reconciliation_dev`) focused strictly on quantitative metrics and structure, these temporal and referential logic checks are verified statically in the ETL source code during Pull Request (PR) reviews.
2.  **For Test Data Engineers (Task 150 - CRM & JSON Prep)**: This checklist serves as a reference manual to **prompt AI models** to generate realistic, logically consistent synthetic test data. Conforming to these rules ensures that generated source data has correct chronological timelines and referential relationships, preventing downstream ingestion from throwing `-1` surrogate keys or temporal lookup errors.

---

## AI Prompt Template for Test Data Generation

When using AI (e.g. Gemini, ChatGPT) to generate mock CRM tables or JSON source files, prepend the following prompt snippet to ensure data logic sanity:

```markdown
Please generate synthetic insurance data (CRM tables: customer, agent; JSON files: policy, quotation, payment, cancellation) ensuring strict referential and temporal consistency:
1. PRE-EXISTENCE RULE: For every transaction (quotation, policy, payment, cancellation), ensure the referenced customer_id, agent_id, and provider_code are created BEFORE the transaction date.
   - Example: agent.created_date <= quotation.quotation_date
   - Example: customer.created_date <= policy.policy_start_date
2. REFERENTIAL INTEGRITY: Do not generate transactions referencing non-existent business IDs (agent_id, customer_id, provider_code, policy_id, quotation_id) unless explicitly creating a negative test case.
3. TIMELINE INTEGRITY:
   - quotation_date <= policy_start_date
   - policy_start_date <= policy_end_date
   - payment_date must fall between policy_start_date and policy_end_date.
   - cancellation_date must be >= policy_start_date.
```

---

## 1. Temporal Integrity Constraint (Pre-existence Rule)

An entity (Agent, Customer, or Provider) must be registered in the system before generating transactions. Therefore, the dimension's creation date must be chronologically prior to or equal to any transaction date referencing it.

### Checklist Items:
*   [ ] **Creation Order Validation**: Ensure the PySpark join or lookup logic guarantees that:
    $$\text{Dimension.created\_date} \le \text{Fact.transaction\_date}$$
    *   *Example*: `dim_agent.created_date` (or `effective_from`) $\le$ `fact_quotation.quotation_date`
    *   *Example*: `dim_provider.created_date` (or `effective_from`) $\le$ `fact_policy.policy_start_date`
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

## 2. Zero Tolerance for Unresolved Keys in Standard Flows

Surrogate keys in fact tables must resolve to valid dimensions and must not fallback to `-1` (Unknown) unless the business key in the source system is legitimately NULL.

### Checklist Items:
*   [ ] **Inner/Left Join Assertions**: In the loading notebooks, verify that lookups against SCD1 or SCD2 dimensions do not use a blind `.fillna(-1)` or `coalesce(lookup_key, -1)` for cases where the business key was populated.
*   [ ] **Active Validation**: If `silver.policy.agent_id` is NOT NULL, then the resolved `agent_key` in `gold.fact_policy` must NOT be `-1`.
*   [ ] **Error Tracing**: Any lookup returning `-1` for a non-null business key must be intercepted in the code and logged as a lookup failure.

---

## 3. SCD Type 2 Point-in-Time Join Contiguity

For dimensions that track historical changes (e.g., `dim_customer`, `dim_provider`), fact records must join to the dimension version that was active at the exact time of the transaction.

### Checklist Items:
*   [ ] **Non-Overlapping Ranges**: Verify that the point-in-time join condition is structured exactly as:
    ```python
    # Ensure transaction date falls between effective dates of the dimension version
    (fact_df["transaction_date"] >= dim_df["effective_from"]) & \
    (fact_df["transaction_date"] <= dim_df["effective_to"])
    ```
*   [ ] **Uniqueness of Lookup**: Confirm that for any given business key and transaction timestamp, only **one** dimension row is matched. Overlapping ranges in SCD2 table rows will cause row explosion in the fact table.
*   [ ] **Open-Ended Active Records**: Active dimension records must have an `effective_to` set to a high-bound default date (e.g., `9999-12-31`) to ensure joins do not fail for transactions occurring after the latest update.

---

## 4. Late-Arriving Dimension Event Detection

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
