# Project Architecture

This folder serves as the central hub for the system architecture documentation, technical blueprints, and diagrams of the **Insurance Analytics** platform.

---

## 1. Directory Structure

- [data-quality/](data-quality/) – Data validation, schema compliance, and layer transition rules.
- [diagrams/](diagrams/) – Source Mermaid diagram definitions (`.mermaid`).
- [exports/](exports/) – Exported image assets (`.png`) for diagrams.
- [team-1/](team-1/) – Implementation strategies for audit logging, data workflow control, and Gold layer ingestion.
- [team-2/](team-2/) – Platform accessibility and Fabric security designs.

---

## 2. Key Architecture Documents

| Document | Topic | Description |
| :--- | :--- | :--- |
| [Data Quality & Transformation](data-quality/data-quality-and-transformation.md) | Data Quality | Data quality framework, validations (nulls, range bounds), quarantine rules, and SCD logic. |
| [Audit Logging MVP Implementation](team-1/audit-logging-mvp-implementation.md) | Audit & Logs | Logging strategy, audit session schemas, and run status tables. |
| [Data Workflow Control Strategy](team-1/data-workflow-control-strategy.md) | Ingestion Orchestration | Watermarking, batch processing control tables, and retry strategies. |
| [Gold Fact Ingestion Notes](team-1/gold-fact-ingestion-implementation-notes.md) | Gold Layer Ingestion | Details on fact table loading, schema validation, and key generation. |
| [Platform Accessibility on Fabric](team-2/design-data-platform-accessibility-on-fabric.md) | Security & Access | User roles, Workspace permissions, and Row-Level Security (RLS) policies. |

---

## 3. Diagrams

Diagram configurations are stored as Mermaid text files for easy editing and tracking. Exported PNG files are located in [exports/](exports/).

*   **End-to-End Pipeline Design:**
    *   Definition: [end-to-end-pipeline-design.mermaid](diagrams/end-to-end-pipeline-design.mermaid)
    *   Exported Image: [pipeline-design-end-to-end.png](exports/pipeline-design-end-to-end.png)
*   **Architecture Layer Design:**
    *   Definition: [architecture-layer.mermaid](diagrams/architecture-layer.mermaid)
    *   Exported Image: [architecture-pipeline-insurance-architecture-layer.png](exports/architecture-pipeline-insurance-architecture-layer.png)

> [!TIP]
> To preview and edit `.mermaid` files, you can use the [Mermaid Live Editor](https://mermaid.live) or install the Mermaid Preview extension in your IDE.
