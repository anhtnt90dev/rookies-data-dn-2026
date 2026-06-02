# Rookie Data DN 2026

## Project Overview

This repository contains project documentation, source code, SQL scripts, test assets, and Microsoft Fabric-related artifacts for the Insurance Analytics MockProject.

GitHub is the primary source control system for all project deliverables. Team members must work through feature branches and Pull Requests to ensure traceability, collaboration, and reviewability.

---

## Repository Structure

```text
rookies-data-dn-2026/
│
├── README.md
├── .gitignore
│
├── config/
│
├── docs/
│   ├── architecture/
│   ├── business-process/
│   ├── data-modeling/
│   ├── final-deliverables/
│   ├── git/
│   │   └── processing-guide/
│   │       ├── conflict-resolution.md
│   │       ├── conventions.md
│   │       ├── promotion-workflow.md
│   │       ├── pull-request-process.md
│   │       └── README.md
│   ├── project-management/
│   └── source-to-target-mapping/
│
├── fabric/
│
├── sql/
│
└── tests/
```

---

## Folder Usage

### `config/`

Stores configuration templates and environment-independent configuration files.

Examples:

- Pipeline configuration templates
- Source system configuration templates
- Parameter files

Do not store credentials, secrets, passwords, or access tokens.

---

### `docs/`

Stores all project documentation and design artifacts.

#### `docs/architecture/`

Architecture-related documentation:

- Solution architecture
- Data platform architecture
- Fabric workspace design
- OneLake structure
- Layer responsibility documents
- Security and access design

#### `docs/business-process/`

Business understanding and process documentation:

- Business process descriptions
- BPMN diagrams
- Sequence diagrams
- Business rules
- Status mapping documents

#### `docs/data-modeling/`

Data modeling artifacts:

- Star schema design
- Dimension design
- Fact design
- Grain definition
- ERD diagrams

#### `docs/source-to-target-mapping/`

Mapping documentation between source systems and analytical models.

Examples:

- Customer mapping
- Quotation mapping
- Policy mapping
- Payment mapping

#### `docs/project-management/`

Project planning and management artifacts.

Examples:

- Sprint documents
- Task descriptions
- Working agreements
- Team guidelines
- Planning notes

#### `docs/final-deliverables/`

Final reviewed outputs prepared for submission or stakeholder review.

Examples:

- Final presentations
- Final design documents
- Approved diagrams
- Sprint deliverables

---

### `docs/git/processing-guide/`

Stores team Git workflow documentation.

Contents include:

| File                      | Purpose                          |
| ------------------------- | -------------------------------- |
| `conventions.md`          | Branching and commit conventions |
| `pull-request-process.md` | Pull Request workflow            |
| `promotion-workflow.md`   | Branch promotion strategy        |
| `conflict-resolution.md`  | Merge conflict handling guide    |
| `README.md`               | Overview of Git working process  |

This folder serves as the single source of truth for repository collaboration practices.

---

### `fabric/`

Reserved for Microsoft Fabric Git Integration artifacts.

Fabric-generated assets should remain aligned with the structure created by Fabric Git Integration.

Examples:

- Pipelines
- Notebooks
- Lakehouse metadata
- Semantic models
- Fabric deployment artifacts

Avoid creating custom structures that conflict with Fabric-managed assets.

---

### `sql/`

Stores SQL scripts used throughout the project.

Examples:

- Source database scripts
- Analytical table definitions
- Validation scripts
- ETL control scripts
- Test scripts

---

### `tests/`

Stores testing and validation artifacts.

Examples:

- Data quality tests
- Reconciliation tests
- Validation queries
- Pipeline verification scripts

---

## Naming Convention

General rules:

- Use lowercase names.
- Use hyphens (`-`) instead of spaces.
- Keep names concise and meaningful.
- Include task IDs when appropriate.

Examples:

```text
task-112-description-ac.md
sequence-diagram-insurance-process-v3.png
project-structure-and-folder-organization-guideline.docx
```

---

## Git Workflow

1. Create a branch from the appropriate base branch.
2. Commit related changes only.
3. Create a Pull Request.
4. Request peer review.
5. Resolve comments and conflicts if required.
6. Merge after approval.

Refer to:

```text
docs/git/processing-guide/
```

for detailed Git workflow documentation.

---

## Empty Folder Tracking

Git does not track empty folders.

If an empty folder must be preserved in the repository structure, add a:

```text
.gitkeep
```

file inside the folder.

---

## Project Guideline

Repository structure and folder organization guidance is maintained under:

```text
docs/project-management/
```

and should be updated whenever the agreed project structure changes.
