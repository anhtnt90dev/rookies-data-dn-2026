# Dashboard Review Guide – Quotation Conversion & Sales Analytics

## Overview

This document provides the resources required for reviewing the **Quotation Conversion & Sales Analytics Dashboard**, including the Power BI Service report, Semantic Model, and KPI Measures.

### Development Status

The primary deliverables for this sprint are the **Semantic Model** and **KPI Measure Layer**. To support early validation and accelerate dashboard development in the next sprint, a draft dashboard has also been prepared with approximately **80% of the planned visual layouts and chart structures** completed.

Please review the semantic model, KPI calculations, business logic, and dashboard design to ensure alignment before final dashboard implementation.

---

## Power BI Service

**Dashboard Link:**

> [\[Insert Power BI Service URL here\]](https://app.powerbi.com/groups/9199cf3f-9e5c-41b4-a4be-8eecf20f108d/reports/92070815-9831-44df-affb-ecd0c38482e3?ctid=039815dd-00ae-48d7-adbb-090e93dcbc01&pbi_source=linkShare)

### Main Components

* Overview
* Policy Operations
* Payment Monitoring
* Provider Performance
* Detail Records

---

## Semantic Model

The semantic model has been developed and reviewed to support the reporting requirements for the Quotation Conversion & Sales Analytics Dashboard.

### Model Coverage

The model includes:

* 5 fact tables
* 14 dim tables
* KPIs & measures calculation

### Key Objectives

* Provide a single source of truth for dashboard reporting.
* Support KPI calculations through reusable measures.
* Enable drill-down and cross-filter analysis.
* Ensure consistency across all report pages.

---

## KPI Measures

The KPI measures have been implemented and reviewed within the semantic model.

### Example KPIs

| KPI                  | Description                                 |
| -------------------- | ------------------------------------------- |
| Active Policies     | Number of policies still active          |
| Policies Issued  | Number of policies issued in the period         |
| Cancelled Policies | Number of policies cancelled in the period           |
| Payment Collection Rate      | Percentage of payment transactions successfully collected |
| Total Written Premium    | Total insurance premium written (VND)              |
| Total Collected Premium   | Total insurance premium collected (VND)       |

---

## Review Checklist

Please review the following areas:

### Semantic Model

* [ ] Table relationships
* [ ] Data consistency
* [ ] Naming conventions
* [ ] Measure calculations

### Dashboard

* [ ] KPI accuracy
* [ ] Filter functionality
* [ ] Visual interactions
* [ ] Navigation flow
* [ ] Layout and user experience

---

## Supporting Files

### Semantic Model Relationship

> [[Insert Semantic Model Relationship Link](https://github.com/anhtnt90dev/rookies-data-dn-2026/blob/dev/docs/data-modeling/dimensional-design/05-powerbi-relationship-design.md)]

### KPI Measures Documentation

> [[Insert KPI Measures DB2 Documentation Link](https://github.com/anhtnt90dev/rookies-data-dn-2026/blob/dev/docs/business-process/diagram/business-measure-kpi/define-business-measures-and-kpis-db2.md)]

---

## Notes

Please provide feedback regarding:

* Semantic model relationship
* Data accuracy
* KPIs & Measures accuracy
* Visual design

Thank you for reviewing.
