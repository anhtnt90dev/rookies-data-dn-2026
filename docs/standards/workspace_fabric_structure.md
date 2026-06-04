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
├── bronze/
│   ├── customer
│   ├── quotation
│   └── payment
├── silver/
│   ├── customer
│   ├── quotation
│   └── payment
├── gold/
│   ├── dim_customer
│   ├── dim_date
│   ├── dim_policy
│   ├── fact_quotation
│   ├── fact_policy
│   ├── fact_payment
│   └── fact_cancellation
├── log/
│   ├── audit_session
│   ├── audit_table_session
│   ├── audit_detail
│   ├── invalid_record
│   └── retry_log
└── cfg/
    ├── watermark
    ├── source_table
    ├── next_run_mode
    ├── dim_fact_table
    └── source_dim_fact
```

## Canonical Control and Audit Table 

To ensure consistency across architecture designs and physical implementations, the following mapping defines how canonical control and audit concepts correspond to physical tables under the `cfg` and `log` schemas :

| Canonical Concept | Physical Table / Schema | Description / Purpose |
| :--- | :--- | :--- |
| `Job_Config` | `cfg.source_table` | Stores source configurations, data formats, paths, keys, and table-level ingestion metadata. |
| `Watermark` | `cfg.watermark` | Stores the latest processed watermark value used for incremental extraction from Source to Bronze. |
| `Batch_Log` | `log.audit_session` | Stores batch/session-level execution status, pipeline metadata, and run metrics. |
| `Pipeline_Log` | `log.audit_table_session`<br>`log.audit_detail` | `log.audit_table_session` tracks table-level layer execution status (Bronze, Silver, Gold).<br>`log.audit_detail` tracks layer-level metrics (source/inserted/updated/deleted/rejected row counts). |
| `Pipeline_Error` | `log.invalid_record` | Stores records that fail validation, schema compliance, or transformation rules. |
| N/A | `log.retry_log` | Stores retry attempt details and transient execution errors. |
| N/A | `cfg.next_run_mode` | Stores the next execution mode and recovery context. |
| N/A | `cfg.dim_fact_table` | Stores Gold-layer table configuration. |
| N/A | `cfg.source_dim_fact` | Defines the relationship mapping between source tables and Gold tables. |




