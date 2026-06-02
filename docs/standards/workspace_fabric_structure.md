# Microsoft Fabric Workspace Folder Structure Standard

## Root point for workspace is foler fabric/*

## Purpose

This document defines a recommended folder structure 

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

## Lakehouse  (`lh_insurance_dev`)

Contains raw, unmodified data ingested from source systems. Data here is organized by source system and entity.

```text
lh_insurance_dev/Files/
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


