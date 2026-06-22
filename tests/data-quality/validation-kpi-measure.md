# KPI Validation Report

## Validation Overview

The KPI validation process was conducted in two phases:

### Phase 1 – KPI Investigation & Root Cause Analysis

Objective:

* Validate KPI calculations against Gold Layer data.
* Identify discrepancies between dashboard results and expected business outcomes.
* Determine whether issues originated from:

  * Business KPI definition
  * Data logic implementation
  * Data model relationship
  * Source data quality

### Phase 2 – KPI Revalidation

Objective:

* Verify all fixes applied during Phase 1.
* Re-execute SQL validation queries.
* Compare Power BI results against Gold Layer data.
* Confirm all KPI calculations align with approved business rules.

---

# Phase 1 – Findings

## A. Business KPI Definition Issues

The following issues were identified where KPI calculation logic did not align with the intended business definition.

| KPI                         | Issue Description                                                                                                                        | Impact                                                            | Resolution                                                     |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------- |
| Conversion Rate             | Business definition required conversion based on actual policy issuance, while dashboard logic relied on quotation conversion flag only. | Conversion rate did not reflect actual issued policies.           | KPI definition reviewed and aligned with business expectation. |
| Policies Issued Rate        | KPI interpretation was unclear between issued policies and accepted quotations.                                                          | Potential misinterpretation of dashboard results.                 | Business rule clarified and validated.                         |
| Acceptance Rate             | Required validation of accepted and converted quotation statuses included in calculation.                                                | Acceptance percentage could differ from expected business result. | Confirmed accepted status mapping with business logic.         |
| Quote-to-Accept Rate        | Validation required confirmation of accepted quotation population and denominator logic.                                                 | KPI interpretation inconsistency.                                 | Logic reviewed and approved.                                   |
| Agent vs Regional Benchmark | Benchmark calculation methodology was not clearly defined.                                                                               | Agent comparison result could be misleading.                      | Business rule clarified prior to implementation.               |

---

## B. Data Logic Issues

The following issues were identified in KPI implementation or supporting data logic.

| Area                  | Issue Description                                                                           | Impact                                         | Resolution                                |
| --------------------- | ------------------------------------------------------------------------------------------- | ---------------------------------------------- | ----------------------------------------- |
| Conversion Measures   | Some validation queries used TRUE/FALSE logic while source data stored values as 1/0.       | Incorrect conversion calculations.             | Updated logic to use converted_flag = 1.  |
| Date Relationships    | Incorrect date key usage identified during monthly KPI validation.                          | Monthly trends could return incorrect results. | Verified and aligned date relationships.  |
| KPI Aggregation Logic | Validation required review of numerator and denominator calculations for rate-based KPIs.   | Potential KPI mismatches.                      | SQL and DAX logic aligned.                |
| Agent Metrics         | Agent-level calculations required validation of grouping logic and aggregation methodology. | Inconsistent agent performance metrics.        | Aggregation logic reviewed and validated. |
| Status-Based KPIs     | Rejected and expired quotation calculations required status mapping verification.           | Incorrect operational metrics.                 | Status mapping confirmed.                 |
| Provider Metrics      | Provider-level calculations required validation against quotation and policy sources.       | Inaccurate provider performance reporting.     | Data aggregation reviewed and validated.  |

---

## C. Data Quality & Model Validation

| Area                          | Issue Description                                                               | Resolution                           |
| ----------------------------- | ------------------------------------------------------------------------------- | ------------------------------------ |
| Fact-to-Dimension Mapping     | Validation performed to ensure all fact records correctly mapped to dimensions. | Mapping validated successfully.      |
| Quotation-Policy Relationship | Validation performed to confirm quotation-to-policy conversion relationship.    | Relationship validated successfully. |
| Date Dimension Mapping        | Verified date key mapping across quotation and policy facts.                    | Mapping validated successfully.      |
| Unknown Dimension Records     | Verified handling of Unknown dimension members in reporting.                    | Confirmed expected behavior.         |
| Duplicate Records             | Checked quotation and policy counts for duplicate record impact.                | No material issue identified.        |

---

# Phase 2 – Revalidation

After all issues identified during Phase 1 were reviewed and resolved, KPI validation was performed again using approved SQL validation queries.

## Validation Scope

### Executive Dashboard

#### Overview

* Total Quotations
* Accepted Quotations
* Policies Issued
* Conversion Rate
* Total Written Premium (VND)
* Policies In Force
* Avg Written Premium per Policy (VND)
* Active Customers
* Acceptance Rate
* Policies Issued Rate

#### Provider & Product

* Volume Share by Provider
* Quotations by Package
* Conv Rate by Provider
* Avg Premium by Provider

#### Agent Performance

* Active Agents
* Average Conversion Rate (Agent)
* Policies Issued by Agent
* Conv Rate by Agent
* Top Agent Name (Policies)
* Best Conv Rate Agent Name
* Agent vs Regional Benchmark

#### Geography & Operations

* Policies Issued by Month
* Conversion Rate by Month
* Lost Rate by Month
* Rejected Quotations
* Expired Quotations
* Quote-to-Accept Rate
* Rejected Quotations (Ops View)
* Expired Quotations (Ops View)
* Conv Rate by Region
* Lost Deals by City

### Operational Dashboard

* Policies Issued
* Total Written Premium
* Total Payments Collected
* Collection Rate
* Cancellation Rate
* Expired Policies
* Active Policies
* AVG Premium/Policy
* Policies Expiring Soon
* Total Payments
* Payment Amount
* Successful Payment Rate
* Failed Payment Rate
* AVG Payment Value
* Active Providers
* Top Premium Provider
* Best Collection Rate
* Highest Cancel Rate

---

# Validation Result

| Dashboard             | Total KPIs | Validation Result |
| --------------------- | ---------- | ----------------- |
| Executive Dashboard   | 31         | Passed            |
| Operational Dashboard | 18         | Passed            |
| Total                 | 49         | Passed            |

All KPI calculations were successfully revalidated against Gold Layer SQL queries after issue resolution. Power BI results matched the approved business logic and source data calculations.
