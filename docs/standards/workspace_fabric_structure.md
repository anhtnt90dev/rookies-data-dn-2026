# Microsoft Fabric Workspace Folder Structure Standard

## The root folder for Fabric Git Integration artifacts is `fabric/`.

## Fabric Git Integration Artifacts

Files under `fabric/` are Fabric Git Integration artifacts exported from the DEV workspace.

The `.platform` files contain Fabric-managed metadata such as `logicalId`.
These IDs are workspace-specific and should only be committed for real workspace artifacts, not generic templates.

## Purpose

This document defines the recommended Microsoft Fabric workspace and repository folder structure for the Insurance Analytics project.

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

# Repository path examples.

```text
fabric/Bronze/<NotebookName>.Notebook/
fabric/Silver/<NotebookName>.Notebook/
fabric/Gold/<NotebookName>.Notebook/
fabric/Lakehouse/<LakehouseName>.Lakehouse/
fabric/Pipelines/<PipelineName>.DataPipeline/
fabric/SemanticModels/<SemanticModelName>.SemanticModel/
docs/standards/workspace_fabric_structure.md
```

# Lakehouse Structure (Files Section)

Within each Lakehouse item, data is stored in either **Tables** (for Delta tables) or **Files** (for raw files/landing areas). The folder structure below applies to the **Files** section of the respective Lakehouses.

## Lakehouse  (`lh_insurance_dev`)

Contains raw, unmodified data ingested from source systems. Data here is organized by source system and entity.

```text
lh_insurance_dev/Files/
├── landing/
│   ├── crm/
│   │   ├── customers/full/<batch_date>/
│   │   ├── quotation/full/<batch_date>/
│   │   └── quotation_item/full/<batch_date>/
│   ├── policy/
│   │   ├── policy_info/full/<batch_date>/
│   │   ├── payment/incremental/<batch_date>/
│   │   └── cancellation/incremental/<batch_date>/
│   └── json/
│       ├── full/
│       └── incremental/
├── audit/
└── quarantine/
```
# Table layout

```text
lh_insurance_dev/Tables/
├── bronze_customers
├── bronze_quotation
├── bronze_payment
├── silver_customers
├── silver_quotation
├── silver_payment
├── dim_customer
├── dim_date
├── dim_policy
├── fact_quotation
├── fact_policy
├── fact_payment
└── fact_cancellation
```


