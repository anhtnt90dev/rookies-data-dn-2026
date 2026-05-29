# Rookie Data DN 2026

## Project Overview

This repository is used to manage documentation, source code, SQL scripts, configuration files, test assets, and project deliverables for the MockProject insurance analytics solution.

Pipeline implementation and execution will happen in Microsoft Fabric. GitHub is used as the main version control location for project artifacts. All meaningful changes should be committed through branches and reviewed by Pull Request before being merged.

## Repository Structure

```text
rookie-data-dn-2026/
|-- README.md
|-- .gitignore
|-- docs/
|   |-- business-process/
|   |   `-- diagrams/
|   |-- architecture/
|   |-- data-modeling/
|   |-- source-to-target-mapping/
|   |-- project-management/
|   `-- final-deliverables/
|-- fabric/
|-- sql/
|   |-- source/
|   |-- lakehouse/
|   |-- etl-control/
|   `-- validation/
|-- src/
|   |-- ingestion/
|   |-- transformation/
|   |-- quality/
|   `-- utilities/
|-- config/
|-- tests/
|   |-- data-quality/
|   |-- reconciliation/
|   `-- pipeline-tests/
`-- archive/
```

## Folder Usage

### `docs/`

Stores project documentation and reviewable deliverables.

| Folder                            | Purpose                                                                                                  |
| --------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `docs/business-process/`          | Business process notes, lifecycle documentation, and business flow explanations.                         |
| `docs/business-process/diagrams/` | Business process diagrams, sequence diagrams, BPMN diagrams, and exported images.                        |
| `docs/architecture/`              | Solution architecture, data platform architecture, workspace design, and layer responsibility documents. |
| `docs/data-modeling/`             | Star schema design, dimension/fact design, grain definition, and ERD-related documents.                  |
| `docs/source-to-target-mapping/`  | Source-to-target mapping documents and mapping specifications.                                           |
| `docs/project-management/`        | Project conventions, planning documents, task guidelines, and team working agreements.                   |
| `docs/final-deliverables/`        | Final reviewed outputs prepared for PO, Tech Lead, PM, or client review.                                 |

### `fabric/`

Stores Microsoft Fabric items synchronized or exported through Fabric Git Integration.

Do not manually split this folder into many custom subfolders. Fabric will manage its own Git Integration structure. Keeping only the root-level `fabric/` folder prevents conflicts between manually created folders and Fabric-generated folders.

Examples of Fabric-related assets may include:

- Data pipelines
- Notebooks
- Lakehouse-related metadata
- Semantic model artifacts
- Deployment-related Fabric items

### `sql/`

Stores SQL scripts used for source setup, lakehouse objects, ETL control, and validation.

| Folder             | Purpose                                                                                       |
| ------------------ | --------------------------------------------------------------------------------------------- |
| `sql/source/`      | Source database schema scripts, source table creation scripts, and seed data scripts.         |
| `sql/lakehouse/`   | Lakehouse table creation scripts and analytical table definitions.                            |
| `sql/etl-control/` | ETL control tables, audit tables, pipeline controller scripts, and related stored procedures. |
| `sql/validation/`  | SQL scripts for reconciliation, row count checks, duplicate checks, and data validation.      |

### `src/`

Stores reusable source code that is not directly managed as a Fabric item.

| Folder                | Purpose                                                                 |
| --------------------- | ----------------------------------------------------------------------- |
| `src/ingestion/`      | Ingestion logic, helper scripts, and reusable ingestion modules.        |
| `src/transformation/` | Transformation logic and reusable business/data transformation modules. |
| `src/quality/`        | Data quality checks, validation helpers, and rule-based quality code.   |
| `src/utilities/`      | Common utilities, shared functions, and helper code.                    |

### `config/`

Stores configuration templates and non-sensitive configuration files.

Do not commit secrets, passwords, tokens, access keys, or private connection strings. Use sample/template files when needed, such as:

```text
config/pipeline_config.example.json
config/source_config.example.json
```

### `tests/`

Stores test scripts and test assets.

| Folder                  | Purpose                                              |
| ----------------------- | ---------------------------------------------------- |
| `tests/data-quality/`   | Data quality test cases.                             |
| `tests/reconciliation/` | Source-to-target reconciliation tests.               |
| `tests/pipeline-tests/` | Pipeline test cases and execution validation assets. |

### `archive/`

Stores deprecated or replaced project files that should be kept for traceability but should not be used as the current version.

## Empty Folder Tracking

Git does not track empty folders. To keep the agreed structure visible in GitHub, each empty folder contains a `.gitkeep` file.

When a folder later contains real files, the `.gitkeep` file can be removed if it is no longer needed.

## Naming Rules

Use consistent names for folders and files:

- Use lowercase letters.
- Use hyphens for folder and file names where possible.
- Avoid spaces in file names.
- Keep names short but meaningful.
- Include task ID or version when useful.

Examples:

```text
task-112-description-ac.md
project-structure-and-folder-organization-guideline.docx
sequence-diagram-insurance-process-v3.png
source-to-target-mapping-customer.md
```

## Git and Pull Request Rules

Before creating a Pull Request:

1. Put files in the correct folder.
2. Avoid creating duplicate folders with similar meanings.
3. Do not commit generated cache files, temporary files, or local environment files.
4. Do not commit credentials or secrets.
5. Update `README.md` if the folder convention changes.
6. Mention the changed folders or artifacts in the PR description.

Recommended PR description format:

```md
## Summary

Briefly describe what this PR adds or changes.

## Changes

- Added/updated files or folders.
- Mentioned important documents, SQL scripts, Fabric assets, or code.

## Review Notes

Mention what reviewers should focus on.
```

## Microsoft Fabric and GitHub Integration

Fabric execution happens in Microsoft Fabric, while GitHub is used for source control and review.

The `fabric/` folder is reserved for Fabric Git Integration output. The team should avoid manually creating custom internal folders under `fabric/`. Fabric-generated structure should be kept as the source of truth after Git Integration is configured.

This prevents conflicts between manually organized files and Fabric-generated files.
