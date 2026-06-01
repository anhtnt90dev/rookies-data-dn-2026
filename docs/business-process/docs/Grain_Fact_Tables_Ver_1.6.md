# Business Processes and Fact Grain - Version 1.6

## Approach

I define only one fact table type for this model: Transaction fact.

This version keeps five fact tables because the updated source and ERD separate quotation header data, quotation item data, policy data, payment transaction data, and cancellation event data. Fact_Quotation owns quote-level premium and conversion analysis. Fact_Quotation_Item keeps coverage-level detail such as coverage type, coverage amount, and deductible amount. Fact_Policy records policy-level information after a quotation becomes a policy. Fact_Payment records payment attempts, and Fact_Cancellation records cancellation events with cancellation reason and refund amount.. Dim_Region is removed as a standalone dimension because region is handled as an attribute of Dim_Agent in the current schema.

Transaction facts are used for quotation, quotation item, policy, payment, and cancellation because each fact records a business record or event at its own grain. A periodic snapshot and accumulating snapshot fact table are not selected at this stage because the current dashboard and source design do not require daily/monthly stored balance snapshots or one row that accumulates all lifecycle milestones.

## Grain Identification

| Fact Table | Grain | Fact Type | Business Logic |
| --- | --- | --- | --- |
| Fact_Quotation | One row per quotation / quote ID | Transaction fact | Supports quote-level analytics such as total quotations, accepted quotations, conversion rate, premium amount, average premium, provider performance, agent performance, package analysis and recent quotation details. |
| Fact_Quotation_Item | One row per quotation coverage item | Transaction fact | Supports coverage-level analytics because one quotation can contain multiple coverage items with different coverage types, coverage amounts, and deductible amounts. |
| Fact_Policy | One row per policy / policy number | Transaction fact | Records one policy created from an accepted quotation, including policy status, policy period, issued date, premium amount. Cancellation events are handled separately in Fact_Cancellation. |
| Fact_Payment | One row per payment transaction / payment attempt | Transaction fact | Tracks payment operations after a policy exists, including payment amount, payment status, payment method, payment date, related policy. |
| Fact_Cancellation | One row per cancellation event / cancellation request | Transaction fact | Tracks cancellation events after a policy exists, including cancellation date, cancellation reason, refund amount, related policy. |

## Fact Table Reasoning

Fact_Quotation is kept at quotation header grain because the premium amount is stored at quotation level, not quotation item level. This fact table is the clean source for quote-level dashboard metrics such as total quotations, accepted quotations, conversion rate, quote premium, average premium, provider performance, agent performance, package analysis and recent quotation details. If only the item-level fact were used, premium amount could be overcounted when one quotation has multiple coverage items unless an allocation rule is defined.

Fact_Quotation_Item is added at quotation coverage item grain because the source system contains quotation item records. This preserves lower-level details such as coverage type, coverage amount, and deductible amount. In the current ERD, it is kept lean by connecting to Dim_Coverage and Dim_Quotation only, because Dim_Quotation links the item back to the quotation header without repeating all quotation header dimensions on the item fact.

Fact_Policy is created because the second business process is policy creation and policy-level tracking. Once a customer accepts a quotation and signs an agreement, the quotation becomes an insurance policy. In this version, Fact_Policy is classified as a transaction fact because it stores one row per policy record, including policy status, policy period, issued date, premium amount, related policy identifier. Cancellation event details are modeled separately in Fact_Cancellation, while payment details are modeled separately in Fact_Payment. This keeps Fact_Policy focused on policy-level analysis without mixing payment or cancellation event grain.

Fact_Payment is created because the third business process is payment collection and payment monitoring. Payments are collected and tracked through operational systems after policies are created. Dashboard 02 focuses on payment processing activities, payment collection performance, pending payments, failed payments, success rate, average payment time, payment method, and recent payment details. The grain is one row per payment attempt, because each payment can have a different amount, date, method, status, result, policy.

Fact_Cancellation is added because the source structure contains a separate cancellation table and Dashboard 02 includes cancellation analysis. Although Fact_Policy can count cancelled policies through policy status, it cannot cleanly answer cancellation-event questions such as why a policy was cancelled, when the cancellation happened, and how much refund was issued. Fact_Cancellation therefore uses one row per cancellation event or cancellation request and keeps cancellation-specific measures such as refund amount separate from the policy fact. This avoids mixing cancellation transaction grain into Fact_Policy.

## Dimension Context for Star Schema ERD

| Fact Table | Connected Dimensions | Why These Dimensions Are Used |
| --- | --- | --- |
| Fact_Quotation | Dim_Date, Dim_Customer, Dim_Provider, Dim_Agent, Dim_Package, Dim_Quotation_Status, Dim_Quotation | These dimensions describe the quotation header context: when the quote was created, who requested it, which provider/agent/package it belongs to, what quotation status it has, and which quotation identifier it represents. Region analysis is handled through Dim_Agent because region is an agent attribute in the current schema. |
| Fact_Quotation_Item | Dim_Coverage, Dim_Quotation | This fact focuses on coverage-item context. Dim_Coverage describes the coverage type, while Dim_Quotation links each item back to the quotation header without using a direct fact-to-fact relationship or repeating all quotation header dimensions. |
| Fact_Policy | Dim_Date, Dim_Customer, Dim_Provider, Dim_Agent, Dim_Package, Dim_Policy_Status, Dim_Policy | These dimensions support policy-level analysis and allow policies to be analyzed by date, customer, provider, agent, package, policy status, and policy identifier. |
| Fact_Payment | Dim_Date, Dim_Customer, Dim_Provider, Dim_Payment_Status, Dim_Payment_Method, Dim_Policy | These dimensions support payment operation analysis by payment date, customer, provider, payment status, payment method, and related policy. |
| Fact_Cancellation | Dim_Date, Dim_Customer, Dim_Provider, Dim_Cancellation_Reason, Dim_Policy | These dimensions support cancellation analysis by cancellation date, customer, provider, cancellation reason, and related policy. Refund amount remains a measure in Fact_Cancellation. |

## ERD Adjustment Rules

The ERD should keep Fact_Quotation and Fact_Quotation_Item as separate fact tables. This is not duplication because the two facts have different grains: Fact_Quotation is quote header level, while Fact_Quotation_Item is coverage item level. Dim_Coverage should connect only to Fact_Quotation_Item, because coverage context belongs to the item grain. Fact_Quotation_Item should also connect to Dim_Quotation so item rows can be grouped back to the quotation header without requiring a direct fact-to-fact relationship.

Dim_Region is removed as a standalone dimension because region is already represented as an attribute of Dim_Agent in the current source/schema. Region-based reporting can still be supported by using Dim_Agent attributes such as region or branch, instead of creating another region dimension that may duplicate the same context.

The ERD should keep Fact_Cancellation because cancellation is stored as a separate source table and contains its own cancellation date, cancellation reason, and refund amount. Dim_Cancellation_Reason should connect only to Fact_Cancellation. Dim_Policy_Status should remain connected to Fact_Policy because it describes the policy status, not the cancellation event itself.

Fact tables should not be directly connected to each other as the main semantic model design. Fact_Quotation and Fact_Quotation_Item share Dim_Quotation, while Fact_Policy, Fact_Payment, and Fact_Cancellation share Dim_Policy. This allows related business processes to be analyzed together without using direct fact-to-fact filtering. All normal dimension-to-fact relationships should be one-to-many, with the dimension on the one side and the fact on the many side.

Only one Dim_Date is needed at the logical ERD level. It can act as a role-playing date dimension for quotation date, policy issue/start/end dates, payment date, and cancellation date. Implementation in Power BI may later duplicate date roles or use inactive relationships, but the conceptual star schema can keep one shared Dim_Date.

## Final Summary

The model now keeps five fact tables: Fact_Quotation, Fact_Quotation_Item, Fact_Policy, Fact_Payment, and Fact_Cancellation. Fact_Quotation represents the sales quote header and owns quote-level premium/conversion analysis. Fact_Quotation_Item represents the coverage details inside each quotation and supports coverage-level analysis through Dim_Coverage and Dim_Quotation. Fact_Policy represents the accepted business contract and policy-level record. Fact_Payment represents the financial transaction after the policy exists. Fact_Cancellation represents the cancellation event after the policy exists and supports cancellation reason and refund analysis. The updated ERD removes standalone Dim_Region and keeps quotation item relationships lean to avoid repeating quotation header context.
