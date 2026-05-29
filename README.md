# Rookie Data DN 2026 - MockProject

## 1. Project Overview

This repository is used to manage documentation, source code, SQL scripts, Fabric-related assets, configuration files, tests, and final deliverables for the MockProject insurance analytics solution.

The project uses Microsoft Fabric for pipeline execution, Lakehouse storage, semantic model development, and analytics delivery. GitHub is used as the central version control system so that all project artifacts can be reviewed, tracked, and merged through Pull Requests.

The purpose of this repository structure is to:

- Keep project documents, diagrams, SQL scripts, code, and Fabric assets organized.
- Avoid duplicated or scattered files across different branches.
- Reduce merge conflicts caused by inconsistent folder structures.
- Make Pull Requests easier to review.
- Support smooth collaboration across all team members.
- Prepare a clear structure for future Fabric deployment and source control alignment.

---

## 2. Repository Structure

```text
rookie-data-dn-2026/
|-- README.md
|-- docs/
|   |-- business-process/
|                   |-- diagrams/
|   |-- architecture/
|   |-- data-modeling/
|   |-- source-to-target-mapping/
|   |-- project-management/
|   `-- final-deliverables/
|-- fabric/
|   |-- pipelines/
|   |-- notebooks/
|   |-- lakehouse/
|   |-- semantic-model/
|   `-- deployment/
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

---

## 3. Folder Usage Guideline

### 3.1 `docs/`

The `docs/` folder stores project documentation and analysis artifacts. Files in this folder should describe business understanding, solution design, architecture decisions, data modeling, mapping logic, project management notes, and final deliverables.

#### `docs/business-process/`

Stores documents related to the insurance business process, including quotation, policy issuance, payment, cancellation, refund, and business rule analysis.

Example files:

```text
business-process-overview.md
quotation-policy-payment-lifecycle.md
business-rules-and-assumptions.md
```

#### `docs/business-process/diagrams/`

Stores business process diagrams and related visual files.

Example files:

```text
sequence_diagram_insurance_processes_version3.png
insurance_business_process.puml
bpmn_insurance_process.drawio
```

#### `docs/architecture/`

Stores solution architecture documents, data platform design, Fabric workspace design, Lakehouse architecture, security/access design, and deployment architecture.

Example files:

```text
solution-architecture.md
data-platform-accessibility-design.md
fabric-workspace-and-lakehouse-design.md
```

#### `docs/data-modeling/`

Stores dimensional modeling documents, star schema design, fact/dimension definitions, surrogate key strategy, ERD, and KPI grain definitions.

Example files:

```text
dimension-design.md
fact-table-design.md
star-schema-erd.png
surrogate-key-strategy.md
```

#### `docs/source-to-target-mapping/`

Stores mapping documents between source systems and target Lakehouse or analytical tables.

Example files:

```text
source-to-target-mapping-customer.md
source-to-target-mapping-quotation.md
source-to-target-mapping-policy-payment.md
```

#### `docs/project-management/`

Stores task descriptions, acceptance criteria, sprint notes, review notes, and project coordination documents.

Example files:

```text
task-112-description-ac.md
sprint-1-planning-notes.md
review-comments-summary.md
```

#### `docs/final-deliverables/`

Stores final reviewed deliverables that are ready for submission or PO/PM review.

Example files:

```text
final_output_v2.docx
project-structure-and-folder-organization-guideline.docx
final-architecture-design.pdf
```

---

### 3.2 `fabric/`

The `fabric/` folder stores assets related to Microsoft Fabric development and deployment. This folder should reflect the work that will be implemented or synchronized with Fabric where applicable.

#### `fabric/pipelines/`

Stores pipeline definitions, exported pipeline metadata, or documentation related to Fabric Data Pipelines.

Example files:

```text
customer-ingestion-pipeline.json
quotation-ingestion-pipeline.json
pipeline-dependency-notes.md
```

#### `fabric/notebooks/`

Stores Fabric notebooks or notebook-related source files used for ingestion, transformation, validation, and utility processing.

Example files:

```text
bronze_to_silver_customer.ipynb
silver_to_gold_policy.ipynb
data_quality_validation.ipynb
```

#### `fabric/lakehouse/`

Stores Lakehouse-related definitions, table design notes, folder path conventions, and Delta table maintenance notes.

Example files:

```text
lakehouse-folder-convention.md
bronze-silver-gold-table-list.md
delta-maintenance-strategy.md
```

#### `fabric/semantic-model/`

Stores semantic model documentation, model relationship notes, measure definitions, hierarchy design, and Power BI/Fabric model-related files.

Example files:

```text
semantic-model-relationships.md
kpi-measures.md
rls-design.md
```

#### `fabric/deployment/`

Stores deployment-related documentation or scripts for moving assets between environments or preparing deployment packages.

Example files:

```text
deployment-checklist.md
fabric-deployment-notes.md
environment-configuration.md
```

---

### 3.3 `sql/`

The `sql/` folder stores SQL scripts used for source simulation, Lakehouse table creation, ETL control, and validation.

#### `sql/source/`

Stores SQL scripts used to create or simulate source operational systems.

Example files:

```text
insurance_source_db_task_115_ver2.sql
create_source_customer_table.sql
create_source_quotation_table.sql
```

#### `sql/lakehouse/`

Stores SQL scripts for Lakehouse tables, analytical schemas, dimensional tables, and Gold layer structures.

Example files:

```text
create_bronze_tables.sql
create_silver_tables.sql
create_gold_fact_policy.sql
create_gold_dim_customer.sql
```

#### `sql/etl-control/`

Stores SQL scripts for ETL configuration, pipeline control, audit logging, error logging, retry handling, and execution tracking.

Example files:

```text
create_etl_config_table.sql
create_pipeline_controller_table.sql
create_etl_audit_log_table.sql
create_error_log_table.sql
```

#### `sql/validation/`

Stores SQL scripts used for data validation, reconciliation, duplicate checks, null checks, and KPI validation.

Example files:

```text
validate_row_count_reconciliation.sql
validate_duplicate_customer.sql
validate_policy_payment_consistency.sql
```

---

### 3.4 `src/`

The `src/` folder stores reusable source code for ingestion, transformation, data quality, and utilities. Code in this folder should be version-controlled and reviewed before being used in Fabric pipelines or notebooks.

#### `src/ingestion/`

Stores reusable ingestion logic for loading source data into Bronze or raw zones.

Example files:

```text
ingest_customer.py
ingest_quotation.py
ingest_policy_json.py
```

#### `src/transformation/`

Stores reusable transformation logic for Bronze to Silver and Silver to Gold processing.

Example files:

```text
transform_customer.py
transform_policy.py
build_gold_fact_payment.py
```

#### `src/quality/`

Stores reusable data quality rules, validation functions, and reconciliation logic.

Example files:

```text
quality_rules.py
row_count_reconciliation.py
business_rule_validation.py
```

#### `src/utilities/`

Stores helper functions shared across ingestion, transformation, and validation logic.

Example files:

```text
logging_utils.py
config_loader.py
date_utils.py
```

---

### 3.5 `config/`

The `config/` folder stores configuration files used by pipelines, notebooks, scripts, and deployment processes.

Example files:

```text
dev_config.yml
pipeline_config.yml
source_system_config.json
lakehouse_paths.yml
```

Rules:

- Do not store passwords, secrets, tokens, or private credentials in this folder.
- Use placeholder values for sensitive settings.
- Keep environment-specific configuration clearly named.

---

### 3.6 `tests/`

The `tests/` folder stores test cases and validation assets used to verify data quality, reconciliation, and pipeline behavior.

#### `tests/data-quality/`

Stores data quality test cases.

Example files:

```text
test_customer_null_check.sql
test_policy_status_values.sql
test_payment_amount_validation.sql
```

#### `tests/reconciliation/`

Stores reconciliation tests between source, Bronze, Silver, Gold, and reporting layers.

Example files:

```text
test_source_to_bronze_row_count.sql
test_silver_to_gold_reconciliation.sql
```

#### `tests/pipeline-tests/`

Stores pipeline test cases, sample test inputs, and expected output notes.

Example files:

```text
test_incremental_load_policy.md
test_pipeline_retry_behavior.md
```

---

### 3.7 `archive/`

The `archive/` folder stores outdated files that are no longer actively used but may need to be kept for historical reference.

Rules:

- Only move files to `archive/` when they are replaced by a newer version.
- Add a short note when archiving important documents.
- Do not use archived files as the current source of truth.

Example files:

```text
old_sequence_diagram_version1.png
old_dimension_design_draft.docx
```

---

## 4. File Placement Rules

Use the following rules when adding new files:

| File Type                           | Target Folder                     |
| ----------------------------------- | --------------------------------- |
| Business process documents          | `docs/business-process/`          |
| Business diagrams                   | `docs/business-process/diagrams/` |
| Architecture documents              | `docs/architecture/`              |
| Data modeling documents             | `docs/data-modeling/`             |
| Source-to-target mapping documents  | `docs/source-to-target-mapping/`  |
| Task descriptions and AC documents  | `docs/project-management/`        |
| Final reviewed deliverables         | `docs/final-deliverables/`        |
| Fabric pipeline assets              | `fabric/pipelines/`               |
| Fabric notebooks                    | `fabric/notebooks/`               |
| Lakehouse design notes              | `fabric/lakehouse/`               |
| Semantic model design notes         | `fabric/semantic-model/`          |
| Deployment notes                    | `fabric/deployment/`              |
| Source database SQL scripts         | `sql/source/`                     |
| Lakehouse or analytical SQL scripts | `sql/lakehouse/`                  |
| ETL control SQL scripts             | `sql/etl-control/`                |
| Validation SQL scripts              | `sql/validation/`                 |
| Ingestion code                      | `src/ingestion/`                  |
| Transformation code                 | `src/transformation/`             |
| Data quality code                   | `src/quality/`                    |
| Shared helper code                  | `src/utilities/`                  |
| Config files                        | `config/`                         |
| Test files                          | `tests/`                          |
| Outdated files                      | `archive/`                        |

---

## 5. Naming Convention

### 5.1 General File Naming

Use lowercase words separated by hyphens for new files.

Recommended format:

```text
short-description-version-or-purpose.extension
```

Examples:

```text
business-process-overview.md
source-to-target-mapping-customer.md
create-gold-fact-policy.sql
bronze-to-silver-customer.py
data-quality-validation.ipynb
```

### 5.2 Versioned Deliverables

For reviewed deliverables or diagrams that need version tracking, use a clear version suffix.

Examples:

```text
sequence-diagram-insurance-processes-v3.png
final-output-v2.docx
dimension-design-v1.docx
```

### 5.3 Avoid

Do not use:

```text
New Document.docx
final_final.docx
abc.sql
test.py
my_file_latest_updated_v2_final.docx
```

---

## 6. Git Branch and Pull Request Workflow

All changes should be made through a separate branch and reviewed through a Pull Request.

Recommended branch format:

```text
type/task-id-short-description
```

Example:

```text
doc/task-112-project-structure-and-folder-organization
feature/task-034-incremental-load-logic
fix/task-070-row-count-reconciliation
```

Before creating a Pull Request:

- Confirm files are placed in the correct folder.
- Confirm file names follow the agreed naming convention.
- Confirm unrelated files are not included in the same PR.
- Confirm generated or outdated files are not accidentally committed.
- Confirm the PR description explains what was changed and why.
- Confirm links to relevant documents, diagrams, or task IDs are included.

---

## 7. Microsoft Fabric Alignment

Microsoft Fabric is used for pipeline execution, Lakehouse processing, semantic modeling, and analytics delivery. GitHub is used to track related assets and supporting source files.

The repository should support Fabric work in the following way:

- Pipeline-related files should be stored under `fabric/pipelines/`.
- Notebook files or notebook source exports should be stored under `fabric/notebooks/`.
- Lakehouse table design and path conventions should be stored under `fabric/lakehouse/`.
- Semantic model documentation should be stored under `fabric/semantic-model/`.
- Deployment and environment notes should be stored under `fabric/deployment/`.
- Reusable Python or SQL logic used by Fabric should be tracked under `src/` or `sql/`.

This separation helps the team understand which files are documentation, which files are executable logic, and which files are Fabric-specific assets.

---

## 8. Pull Request Review Checklist

Reviewers should check the following items before approving a PR:

- The file is stored in the correct folder.
- The file name is clear and follows the naming convention.
- The change is related to the task or user story.
- The PR does not mix unrelated work.
- The document or code can be understood by other team members.
- Any related diagram, SQL file, source code, or deliverable is linked where needed.
- Outdated files are moved to `archive/` if they are no longer current.
- Fabric-related work is placed under the correct Fabric, SQL, or source code folder.

---

## 9. Ownership Notes

Each team member is responsible for keeping their task files organized according to this structure. If a new folder is needed, the team should discuss and agree before adding it.

Do not create personal folders such as:

```text
phu/
member-a/
temp/
new-folder/
```

Instead, place files based on their purpose and layer in the project.

---

## 10. Source of Truth

The current version in the `main` or agreed integration branch should be treated as the source of truth.

For final review or submission, use files under:

```text
docs/final-deliverables/
```

For active working documents, use the relevant folder under:

```text
docs/
fabric/
sql/
src/
config/
tests/
```

Archived files should not be used as the latest version unless explicitly restored by the team.
