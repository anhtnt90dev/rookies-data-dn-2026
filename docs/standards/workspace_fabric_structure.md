# Microsoft Fabric Workspace Folder Structure Standard

## Purpose

This document defines a recommended folder structure and naming convention for Microsoft Fabric projects. The goal is to:

- **Improve Collaboration:** Standardize where items are placed so team members can easily locate resources.
- **Standardize Development:** Ensure consistency across different environments and workspaces.
- **Simplify Deployments:** Align with CI/CD and deployment pipelines.
- **Support Medallion Architecture:** Provide clear segregation for Bronze, Silver, and Gold data layers.
- **Isolate Assets:** Separate operational and utility assets from primary data ingestion and transformation assets.

---

# Workspace Structure

Below is the standard folder hierarchy for a Microsoft Fabric workspace. This structure leverages Workspace Folders to group related items (such as Lakehouses, Warehouses, Pipelines, Notebooks, and Semantic Models).

```text
Fabric-Workspace/
│
├── Governance/
│   ├── Standards/
│   ├── Naming-Conventions/
│   ├── Architecture/
│   └── Documentation/
│
├── Source-Control/
│   ├── Deployment/
│   ├── CICD/
│   └── Release-Notes/
│
├── Ingestion/
│   ├── Pipelines/
│   ├── Dataflows/
│   ├── Notebooks/
│   └── Config/
│
├── Bronze/
│   ├── Pipelines/
│   └── Notebooks/
│
├── Silver/
│   ├── Pipelines/
│   └── Notebooks/
│
├── Gold/
│   ├── Pipelines/
│   ├── Notebooks/
│   └── Semantic-Models/
│
├── Shared/
│   ├── Notebooks/
│   ├── Libraries/
│   ├── Functions/
│   └── Utilities/
│
├── Monitoring/
│   ├── Logs/
│   ├── Alerts/
│   ├── Audit/
│   └── Data-Quality/
│
├── Config/
│   └── Mapping/
│
└── Lakehouse/
    ├── lh_insurance_dev (Lakehouse)
 
```

---

# Lakehouse Structure (Files Section)

Within each Lakehouse item, data is stored in either **Tables** (for Delta tables) or **Files** (for raw files/landing areas). The folder structure below applies to the **Files** section of the respective Lakehouses.

## Bronze Layer (`lh_bronze`)

Contains raw, unmodified data ingested from source systems. Data here is organized by source system and entity.

```text
lh_bronze/Files/
│
├── erp/
│   ├── customer/
│   ├── policy/
│   └── vehicle/
│
├── crm/
│   ├── contact/
│   ├── lead/
│   └── quotation/
│
├── api/
│   └── payment/
│
├── files/
└── audit/
```

### Bronze Layer Rules
> [!IMPORTANT]
> - **No business transformation:** Keep data exactly as received from source systems.
> - **Preserve source schema:** Do not rename columns or modify data types at this stage.
> - **Append-only:** Maintain history by using append-only load patterns.
> - **Ingestion metadata:** Capture loading timestamps and source file paths.

---

## Silver Layer (`lh_silver`)

Contains validated, cleansed, and standardized data. Data is structured as conformed tables (Delta format) organized by business domain or entity group.

```text
lh_silver/Files/ (Staging/Reference files if any)
│
├── customer/
├── policy/
├── vehicle/
├── payment/
└── reference/
```

### Silver Layer Rules
> [!IMPORTANT]
> - **Deduplication:** Remove duplicate records from incoming streams.
> - **Standardization:** Standardize formats (e.g., date formats, uppercase strings, phone numbers).
> - **Business Validation:** Apply data quality checks and route invalid records to quarantine.
> - **Null Handling:** Replace null values with default placeholders where appropriate.
> - **Conformed Entities:** Build clean, normalized tables.

---

## Gold Layer (`lh_gold` / `wh_sales`)

Contains business-ready datasets optimized for analytical reporting. Data is modeled using a Star Schema (Facts and Dimensions).

```text
lh_gold/Files/ (Exported models/static extracts)
│
├── sales/
│   ├── fact_payment/
│   ├── dim_customer/
│   └── dim_vehicle/
│
├── finance/
│   └── fact_cancellation/
│
└── executive/
```

### Gold Layer Rules
> [!IMPORTANT]
> - **Star Schema:** Model data into dimensions (`dim_`) and facts (`fact_`).
> - **KPI Calculations:** Calculate business metrics and KPIs here.
> - **Reporting Optimization:** Structure and index tables for fast BI query execution.
> - **Power BI Consumption:** Gold tables are the primary source for Power BI Semantic Models.

---

# Notebook Organization

Notebooks are organized inside workspace folders by their operational layer. Their naming conventions must match the respective step:

```text
Notebooks/
│
├── ingestion/
├── bronze/
├── silver/
├── gold/
├── shared/
└── testing/
```

### Example Notebook Names
- `nb_ing_customer_load` (Ingesting raw customer data to Bronze landing)
- `nb_brz_customer_standardize` (Loading Bronze files to Delta tables)
- `nb_slv_customer_cleansing` (Cleansing and deduplicating customer data)
- `nb_gld_sales_mart` (Aggregating sales metrics for Gold)

---

# Pipeline Organization

Data Factory pipelines are partitioned by frequency and layer.

```text
Pipelines/
│
├── master/
├── ingestion/
├── bronze/
├── silver/
├── gold/
└── monitoring/
```

### Example Pipeline Names
- `pl_master_daily_load`
- `pl_ing_customer`
- `pl_slv_customer_transform`
- `pl_gld_sales_mart`

---

# Configuration Management

All pipeline configurations, connection parameters, and metadata should be managed outside notebook/pipeline code, using tables inside the configuration storage.

```text
Config/
│
├── source_system/
├── pipeline_control/
├── retry_policy/
├── watermark/
└── data_quality/
```

### Configured Elements Include:
- Source connection strings and API endpoints
- Incremental load watermarks
- Retry limits and timeout settings
- Execution logs and dependency configurations

---

# Naming Standards Summary

To maintain uniformity across Microsoft Fabric workspaces, adhere to the following prefixes and cases:

| Asset Type | Prefix | Case Style | Example |
| :--- | :--- | :--- | :--- |
| **Lakehouse** | `lh_` | `lower_snake_case` | `lh_bronze` |
| **Warehouse** | `wh_` | `lower_snake_case` | `wh_sales` |
| **Pipeline** | `pl_` | `lower_snake_case` | `pl_ing_customer` |
| **Notebook** | `nb_` | `lower_snake_case` | `nb_ing_customer` |
| **Semantic Model** | `sm_` | `lower_snake_case` | `sm_sales` |

---

# Recommended Team Ownership

| Area | Owner Role | Primary Responsibility |
| :--- | :--- | :--- |
| **Ingestion** | Data Engineer | Extracting data from source systems to Bronze |
| **Bronze** | Data Engineer | Storage and append-only loading of raw data |
| **Silver** | Data Engineer | Cleansing, transformation, and validation |
| **Gold** | Analytics Engineer | Dimensional modeling and star schema preparation |
| **Semantic Models** | BI Developer | Power BI modeling, DAX measures, and reporting |
| **Governance** | Solution Architect | Folder structures, security, standards enforcement |
| **Monitoring** | Data Operations | Alerting, pipeline failures, performance tuning |

---

# Key Principles

1. **Immutable Bronze:** Keep the Bronze layer raw and immutable. Do not alter data schemas or values.
2. **Business Validation in Silver:** Perform all cleaning, standardization, and quality gate checks in Silver.
3. **Trusted Gold:** Only publish verified, business-approved, conformed data in the Gold layer.
4. **Centralize Reusable Code:** Share utility functions and libraries inside the `Shared/` folder.
5. **Separate Config from Code:** Never hardcode paths, credentials, or watermarks.
6. **Consistent Naming:** Always use the defined naming prefixes and snake_case format.
7. **Document Everything:** Keep mapping documents and pipeline specs updated.
8. **Logging & Audit:** Log every execution status, row counts, and data quality issues.
9. **Design for Restartability:** Ensure pipelines can be safely rerun from failure without duplicating data.
10. **Version Control:** Commit all workspace items and definitions to the Git repository.
