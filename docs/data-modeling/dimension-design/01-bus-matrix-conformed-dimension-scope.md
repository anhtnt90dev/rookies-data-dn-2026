# Task 118: Define Bus Matrix and Conformed Dimension Scope

## 1. Purpose

This document defines the Bus Matrix for the Insurance Analytics Gold Layer. The Bus Matrix aligns the agreed fact tables with shared conformed dimensions so that the dimensional model can support consistent reporting across quotation, quotation item, policy, payment, and cancellation analytics.

This document is the design input for:

- Task 14: Define dimension table structures
- Task 15: Define surrogate key strategy
- Task 85: Define SCD handling approach
- Star Schema ERD review
- Fact table relationship review
- Semantic model relationship design

## 2. Key Modeling Assumptions

| Assumption                                                                                 | Design Impact                                                                                                                            |
| ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| One customer has exactly one vehicle within the project scope.                             | `Dim_Vehicle` can be resolved from `customer_id` and shared by quotation, quotation item, policy, payment, and cancellation facts.       |
| The model follows five fact tables defined in `Grain_Fact_Tables_Ver_1.3.docx`.            | Dimension design must support `Fact_Quotation`, `Fact_Quotation_Item`, `Fact_Policy`, `Fact_Payment`, and `Fact_Cancellation`.           |
| Fact tables should not be directly connected to each other in the semantic model.          | Shared dimensions such as `Dim_Quotation` and `Dim_Policy` are used to analyze related processes without direct fact-to-fact joins.      |
| One logical `Dim_Date` is used as a role-playing dimension.                                | Multiple date keys can exist in facts, for example `quotation_date_key`, `issued_date_key`, `payment_date_key`, `cancellation_date_key`. |
| Status values are modeled as small conformed dimensions.                                   | Status dimensions provide consistent slicers, status grouping, and KPI filters.                                                          |
| Source systems include CRM SQL sources and JSON-based policy/payment/cancellation sources. | Dimension design must support both relational source keys and incremental metadata.                                                      |

## 3. Source-to-Dimension Context

| Source Area                   | Source Object         | Main Dimension Usage                                           |
| ----------------------------- | --------------------- | -------------------------------------------------------------- |
| CRM SQL                       | `customers`           | `Dim_Customer`, `Dim_Region`                                   |
| CRM SQL                       | `agents`              | `Dim_Agent`, `Dim_Region`                                      |
| CRM SQL                       | `insurance_providers` | `Dim_Provider`                                                 |
| CRM SQL                       | `vehicle`             | `Dim_Vehicle`                                                  |
| CRM SQL                       | `quotation`           | `Dim_Quotation`, `Dim_Package`, `Dim_Quote_Status`, `Dim_Date` |
| CRM SQL                       | `quotation_item`      | `Dim_Coverage`                                                 |
| Policy JSON / policy DB       | `policy_info`         | `Dim_Policy`, `Dim_Policy_Status`, `Dim_Date`                  |
| Payment JSON / payment DB     | `payment`             | `Dim_Payment_Status`, `Dim_Payment_Method`, `Dim_Date`         |
| Cancellation JSON / policy DB | `cancellation`        | `Dim_Cancellation_Reason`, `Dim_Date`                          |

## 4. Fact Table Scope

| Business Process   | Fact Table            | Fact Type                  | Fact Grain                                            | Fact Purpose                                                                                                                                                                                                             |
| ------------------ | --------------------- | -------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Quotation          | `Fact_Quotation`      | Transaction fact           | One row per quotation / quote ID                      | Supports quote-level analytics such as total quotations, accepted quotations, conversion rate, premium amount, average premium, provider performance, agent performance, package analysis, and recent quotation details. |
| Quotation Coverage | `Fact_Quotation_Item` | Transaction fact           | One row per quotation coverage item                   | Supports coverage-level analytics, including coverage type, coverage amount, and deductible amount.                                                                                                                      |
| Policy Lifecycle   | `Fact_Policy`         | Accumulating snapshot fact | One row per policy / policy number                    | Tracks policy lifecycle after a quotation becomes a policy, including issued, active, expired, and cancelled states.                                                                                                     |
| Payment            | `Fact_Payment`        | Transaction fact           | One row per payment transaction / payment attempt     | Tracks payment operations including payment amount, status, method, date, and processing result.                                                                                                                         |
| Cancellation       | `Fact_Cancellation`   | Transaction fact           | One row per cancellation event / cancellation request | Tracks cancellation events, cancellation reason, cancellation date, and refund amount.                                                                                                                                   |

## 5. Dimension Declaration

| Dimension Table           | Dimension Type                   | SCD Type | Dimension Grain                         | Business Key               | Surrogate Key             |
| ------------------------- | -------------------------------- | -------: | --------------------------------------- | -------------------------- | ------------------------- |
| `Dim_Date`                | Conformed / role-playing         |   No SCD | One row per calendar date               | `full_date`                | `date_key`                |
| `Dim_Customer`            | Conformed                        |   Type 2 | One row per customer version            | `customer_id`              | `customer_key`            |
| `Dim_Vehicle`             | Conformed                        |   Type 2 | One row per vehicle version             | `vehicle_id`               | `vehicle_key`             |
| `Dim_Agent`               | Conformed                        |   Type 2 | One row per agent version               | `agent_id`                 | `agent_key`               |
| `Dim_Provider`            | Conformed                        |   Type 2 | One row per provider version            | `provider_code`            | `provider_key`            |
| `Dim_Region`              | Conformed reference              |   Type 1 | One row per normalized reporting region | `region_code`              | `region_key`              |
| `Dim_Package`             | Conformed reference              |   Type 1 | One row per insurance package code      | `package_code`             | `package_key`             |
| `Dim_Coverage`            | Reference                        |   Type 1 | One row per coverage type               | `coverage_type`            | `coverage_key`            |
| `Dim_Quotation`           | Transaction identifier dimension |   Type 1 | One row per quotation                   | `quotation_id`             | `quotation_key`           |
| `Dim_Policy`              | Transaction identifier dimension |   Type 1 | One row per policy                      | `policy_id`                | `policy_key`              |
| `Dim_Quote_Status`        | Status mini-dimension            |   Type 1 | One row per quotation status            | `quote_status_code`        | `quote_status_key`        |
| `Dim_Policy_Status`       | Status mini-dimension            |   Type 1 | One row per policy status               | `policy_status_code`       | `policy_status_key`       |
| `Dim_Payment_Status`      | Status mini-dimension            |   Type 1 | One row per payment status              | `payment_status_code`      | `payment_status_key`      |
| `Dim_Payment_Method`      | Reference                        |   Type 1 | One row per payment method              | `payment_method_code`      | `payment_method_key`      |
| `Dim_Cancellation_Reason` | Reference                        |   Type 1 | One row per cancellation reason         | `cancellation_reason_code` | `cancellation_reason_key` |

## 6. Bus Matrix

Legend:

- `X` = Directly connected dimension.
- `X*` = Resolved through shared business context, but stored as a fact foreign key in Gold for reporting convenience.
- `Date role` = Uses `Dim_Date` with one or more role-playing date keys.

| Business Process   | Fact Table            | Fact Grain                          |      Date | Customer | Vehicle | Agent | Provider | Region | Package | Coverage | Quotation | Policy | Quote Status | Policy Status | Payment Status | Payment Method | Cancellation Reason |
| ------------------ | --------------------- | ----------------------------------- | --------: | -------: | ------: | ----: | -------: | -----: | ------: | -------: | --------: | -----: | -----------: | ------------: | -------------: | -------------: | ------------------: |
| Quotation          | `Fact_Quotation`      | One row per quotation               |         X |        X |       X |     X |        X |      X |       X |          |         X |        |            X |               |                |                |                     |
| Quotation Coverage | `Fact_Quotation_Item` | One row per quotation coverage item |         X |        X |       X |     X |        X |      X |       X |        X |         X |        |            X |               |                |                |                     |
| Policy Lifecycle   | `Fact_Policy`         | One row per policy                  | Date role |        X |       X |   X\* |        X |      X |     X\* |          |       X\* |      X |              |             X |                |                |                     |
| Payment            | `Fact_Payment`        | One row per payment transaction     |         X |      X\* |     X\* |   X\* |      X\* |    X\* |     X\* |          |           |      X |              |           X\* |              X |              X |                     |
| Cancellation       | `Fact_Cancellation`   | One row per cancellation event      |         X |      X\* |     X\* |   X\* |      X\* |    X\* |     X\* |          |           |      X |              |           X\* |                |                |                   X |

## 7. Role-Playing Date Usage

| Fact Table            | Date Key Column             | Source Date                            | Business Meaning                       |
| --------------------- | --------------------------- | -------------------------------------- | -------------------------------------- |
| `Fact_Quotation`      | `quotation_date_key`        | `quotation.quotation_date`             | Date when quotation was created.       |
| `Fact_Quotation`      | `quotation_expiry_date_key` | `quotation.quotation_expiry_date`      | Date when quotation expires.           |
| `Fact_Quotation_Item` | `quotation_date_key`        | inherited from quotation header        | Date of the related quotation.         |
| `Fact_Policy`         | `issued_date_key`           | `policy_info.issued_date`              | Date when policy was issued.           |
| `Fact_Policy`         | `policy_start_date_key`     | `policy_info.policy_start_date`        | Policy coverage start date.            |
| `Fact_Policy`         | `policy_end_date_key`       | `policy_info.policy_end_date`          | Policy coverage end date.              |
| `Fact_Policy`         | `cancelled_date_key`        | derived from cancellation if available | Date when policy was cancelled.        |
| `Fact_Payment`        | `payment_date_key`          | `payment.payment_date`                 | Date when payment attempt occurred.    |
| `Fact_Cancellation`   | `cancellation_date_key`     | `cancellation.cancellation_date`       | Date when cancellation event occurred. |

## 8. Conformed Dimension Scope

| Conformed Dimension | Shared By                                                                                   | Scope                                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `dim_date`          | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Common calendar and role-playing date analysis.                                                         |
| `dim_customer`      | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Customer-level quotation, policy, payment, and cancellation analysis.                                   |
| `dim_vehicle`       | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Vehicle-level analysis, based on the assumption that each customer has one vehicle.                     |
| `dim_agent`         | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Agent performance and downstream policy/payment/cancellation analysis through quotation/policy context. |
| `dim_provider`      | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Provider performance across quotation, policy, payment, and cancellation.                               |
| `dim_region`        | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Shared reporting region for regional trend and performance analysis.                                    |
| `dim_package`       | `fact_quotation`, `fact_quotation_item`, `fact_policy`, `fact_payment`, `fact_cancellation` | Package-level quotation and downstream lifecycle analysis.                                              |
| `dim_quotation`     | `fact_quotation`, `fact_quotation_item`                                                     | Allows quotation header and quotation item analysis without direct fact-to-fact joins.                  |
| `dim_policy`        | `fact_policy`, `fact_payment`, `fact_cancellation`                                          | Allows policy, payment, and cancellation analysis without direct fact-to-fact joins.                    |

## 9. Dimensions Limited to Specific Processes

| Dimension                 | Used By                                                     | Reason                                                                     |
| ------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------- |
| `Dim_Coverage`            | `Fact_Quotation_Item`                                       | Coverage type belongs to quotation item grain, not quotation header grain. |
| `Dim_Quote_Status`        | `Fact_Quotation`, `Fact_Quotation_Item`                     | Quotation status describes the quotation lifecycle.                        |
| `Dim_Policy_Status`       | `Fact_Policy`, downstream facts as inherited policy context | Policy status describes policy lifecycle state.                            |
| `Dim_Payment_Status`      | `Fact_Payment`                                              | Payment status describes payment transaction outcome.                      |
| `Dim_Payment_Method`      | `Fact_Payment`                                              | Payment method exists only in payment source.                              |
| `Dim_Cancellation_Reason` | `Fact_Cancellation`                                         | Cancellation reason exists only in cancellation source.                    |

## 10. Output

This document is the output for **Task 118: Define Bus Matrix and conformed dimension scope**.
