# Data Integration Process - Lakehouse to Power BI Dashboard

## Overview

This document describes the end-to-end process for integrating data from the Lakehouse into the Power BI Dashboard.

The dashboard pages, visualizations, KPI measures, and report layouts have already been developed. This document focuses on loading data from the Lakehouse, preparing and validating the semantic model, configuring relationships, validating data quality, and refreshing the dashboard.

---

# Architecture Overview

```text
Source Data
    │
    ▼
Pipeline
(Bronze → Silver → Gold)
    │
    ▼
Lakehouse
(Fact & Dimension Tables)
    │
    ▼
Power BI Semantic Model
    │
    ▼
Data Validation
& Relationship Configuration
    │
    ▼
KPI Measures
    │
    ▼
Power BI Dashboard
```

---

# Process Flow

## Step 1 – Connect to Lakehouse

### Objective

Connect the Power BI Semantic Model to the required Lakehouse tables.

### Actions

1. Open Power BI Desktop.
2. Select **Get Data**.
3. Connect to the target Lakehouse.
4. Select the required Fact and Dimension tables.
5. Load tables into the Semantic Model.

### Status

Data successfully loaded from the Lakehouse into Power BI.

---

## Step 2 – Validate Table Structure

### Objective

Ensure imported tables follow project naming standards and can be mapped correctly within the semantic model.

### Actions

* Review imported tables.
* Verify naming conventions.
* Validate table mapping against semantic model requirements.

### Naming Convention Standards

| Table Type      | Prefix  | Example          |
| --------------- | ------- | ---------------- |
| Fact Table      | `fact_` | `fact_quotation` |
| Dimension Table | `dim_`  | `dim_customer`   |

### Naming Rules

* Use lowercase naming conventions.
* Use underscores (`_`) to separate words.
* Fact tables contain transactional data.
* Dimension tables contain descriptive attributes.

### Validation Finding – Table Mapping Mismatch

#### Expected Table Names

| Expected Table Name |
| ------------------- |
| dim_customer        |
| dim_agent           |
| fact_quotation      |

#### Actual Imported Tables

| Imported Table Name |
| ------------------- |
| gold_dim_customer   |
| gold_dim_agent      |
| gold_fact_quotation |

#### Impact

* Existing semantic model mappings cannot be reused directly.
* Manual table renaming is required.
* Additional Power Query transformations are needed.

#### Resolution

Rename imported tables to align with semantic model standards.

```text
gold_dim_customer
        ↓
dim_customer
```

### Status

Table naming issues identified and corrected.

---

## Step 3 – Data Preparation

### Objective

Prepare and standardize data before semantic model validation.

### Actions

Review and update:

* Column names
* Null values
* Duplicate records
* Unused columns
* Relationship key columns

### Data Cleansing Activities

#### Column Standardization

Business and key columns were standardized to align with semantic model naming conventions.

#### Null Value Validation

Mandatory business keys and relationship columns were reviewed to ensure no invalid null values existed.

#### Duplicate Record Validation

Duplicate records were identified and removed based on business key definitions to maintain data consistency.

#### Unused Column Removal

The following technical and operational metadata columns were identified as unnecessary for reporting and analytics purposes:

* created_at
* updated_at
* effective_from
* effective_to
* is_current
* active_flag
* _batch_id
* _source_system
* delete_batch_id
* deleted_at
* is_deleted

These columns were removed to:

* Reduce model complexity.
* Improve dataset refresh performance.
* Optimize memory consumption.
* Expose only business-relevant attributes to report consumers.

### Status

Data structure reviewed, standardized, and cleansed successfully.

---

## Step 4 – Configure Data Types

### Objective

Ensure all columns use appropriate data types for reporting and calculations.

### Actions

Review data types for all key and measure columns.

### Example

| Column           | Data Type      |
| ---------------- | -------------- |
| customer_key     | Whole Number   |
| agent_key        | Whole Number   |
| policy_key       | Whole Number   |
| quotation_amount | Decimal Number |
| quotation_date   | Date           |

### Status

Data types configured successfully.

---

## Step 5 – Data Quality and Referential Integrity Validation

### Objective

Validate data quality and relationship readiness before building the Star Schema model.

### Validation Checklist

| Validation Item          | Description                                         |
| ------------------------ | --------------------------------------------------- |
| Business Key Consistency | Verify source identifiers are consistent            |
| Surrogate Key Mapping    | Verify Fact and Dimension tables use matching keys  |
| Null Key Check           | Verify no null relationship keys exist              |
| Unknown Key Check        | Verify no unexpected values exist                   |
| Referential Integrity    | Verify Fact records can connect to Dimension tables |

### Validation Findings

The following issues were identified during validation:

#### Issue 1 – Policy Key Mapping Mismatch

Fact tables were using policy surrogate keys that did not match the corresponding keys stored in the policy dimension table.

##### Impact

* Relationships could not be created correctly.
* Policy-related filtering and drill-down operations were affected.
* Policy KPI calculations could become inaccurate.

#### Issue 2 – Agent Lookup Failure

Some quotation records contained unmatched agent keys due to lookup failures during the Silver-to-Gold transformation process.

##### Impact

* Fact records could not connect to the Agent dimension.
* Agent performance reporting became incomplete.
* Agent-related KPI calculations were impacted.

### Root Cause Investigation

Initial analysis indicates that the issues originated during the Silver-to-Gold transformation process.

### Investigation Areas

* Surrogate key generation logic.
* Lookup transformation logic.
* Fact-to-Dimension key mapping validation.
* Gold layer table generation process.

### Status

Data quality issues identified and currently under investigation.

---

## Step 6 – Create Relationships

### Objective

Build the Star Schema model.

### Star Schema Design

#### Dimension Tables

* dim_customer
* dim_agent
* dim_policy
* dim_provider
* dim_vehicle
* dim_date

#### Fact Tables

* fact_quotation
* fact_policy

### Planned Relationships

| From                      | To                          | Cardinality |
| ------------------------- | --------------------------- | ----------- |
| dim_customer.customer_key | fact_quotation.customer_key | 1 : Many    |
| dim_agent.agent_key       | fact_quotation.agent_key    | 1 : Many    |
| dim_policy.policy_key     | fact_policy.policy_key      | 1 : Many    |
| dim_provider.provider_key | fact_policy.provider_key    | 1 : Many    |
| dim_vehicle.vehicle_key   | fact_policy.vehicle_key     | 1 : Many    |
| dim_date.date_key         | fact_quotation.date_key     | 1 : Many    |

### Relationship Settings

* Cross Filter Direction: Single
* Relationship Status: Active

### Current Status

Relationship creation is partially completed.

Several relationships remain pending until the data quality issues identified during validation are resolved.

---

## Step 7 – Validate Semantic Model

### Objective

Verify that the semantic model is ready for reporting and analytics.

### Validation Checklist

* Tables loaded successfully.
* Relationships active.
* No relationship conflicts.
* Data types validated.
* KPI measures functioning correctly.
* Referential integrity confirmed.

### Current Status

Validation pending.

Awaiting completion of data correction and relationship validation activities.

---

## Step 8 – Refresh Dashboard

### Objective

Refresh dashboard data after semantic model validation is completed.

### Actions

1. Refresh Semantic Model.
2. Validate refresh completion.
3. Review dashboard pages.
4. Verify KPI calculations.
5. Verify slicers and filters.
6. Verify drill-through functionality.

### Refresh Configuration

| Configuration Item | Value                         |
| ------------------ | ----------------------------- |
| Refresh Type       | Manual                        |
| Refresh Dependency | Lakehouse Pipeline Completion |

### Expected Result

* KPI cards display correct values.
* Charts display accurate results.
* Filters operate correctly.
* Drill-through functionality works as expected.

### Current Status

Pending completion of data validation and relationship verification.

---

# Current Data Flow Status

```text
Lakehouse
    │
    ▼
Load Tables
    │
    ▼
Validate Structure
    │
    ▼
Prepare Data
    │
    ▼
Validate Data Quality
    │
    ├── Policy Key Mapping Mismatch
    │
    └── Agent Lookup Failure
    │
    ▼
Investigate Silver → Gold Logic
    │
    ▼
Fix Data Quality Issues
    │
    ▼
Create Relationships
    │
    ▼
Validate Semantic Model
    │
    ▼
Refresh Dashboard
```

---

# Current Outcome

## Completed

* Connected Lakehouse tables to the Power BI Semantic Model.
* Reviewed and standardized table naming conventions.
* Prepared and cleansed data.
* Configured data types.
* Performed data quality validation.
* Identified semantic model and relationship issues.

## In Progress

* Investigating policy surrogate key mismatches.
* Investigating agent lookup failures.
* Reviewing Silver-to-Gold transformation logic.
* Resolving data quality issues.

## Pending

* Relationship validation.
* Semantic model validation.
* Dashboard refresh.
* Final business verification.
