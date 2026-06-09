# Rookie Data DN 2026 - Insurance Analytics

This repository serves as the single source of truth for the **Insurance Analytics MockProject** (Rookie Data DN 2026 / CarPro Insurance Analytics). It contains all project documentation, SQL scripts, test assets, and Microsoft Fabric-related configurations and notebooks.

---

## 1. Project Overview

The objective of this project is to implement a medallion data architecture (Bronze → Silver → Gold) to ingest, clean, model, and serve insurance analytics data. The data platform leverages **Microsoft Fabric** as the primary integration engine and **GitHub** for source control management and collaboration.

---

## 2. Directory Structure

Below is the current physical folder structure of the repository:

```text
rookies-data-dn-2026/
│
├── .gitignore
├── README.md
│
├── architecture/                     # Architecture diagrams and exports
│   ├── README.md
│   ├── diagrams/
│   └── exports/
│
├── config/                           # Environment configuration templates
│   └── .gitkeep
│
├── docs/                             # Project documentation and designs
│   ├── architecture/                 # System architecture design documents
│   ├── business-process/             # Business workflow analysis
│   │   ├── diagram/                  # BPMN & sequence diagram source files
│   │   │   └── business_measure_kpi/ # Measure and KPI documents for source databases
│   │   ├── diagrams/
│   │   └── docs/                     # Detailed business process specs
│   ├── data-modeling/                # Star schema designs & table definitions
│   │   ├── dimensional-design/       # Conformed dimension scope and SCD designs
│   │   └── facts/                    # Fact table column-level requirements
│   ├── final-deliverables/           # Reviewed deliverables for submissions
│   ├── kpi-measure/                  # Business KPI mathematical mappings
│   ├── project-management/           # Sprint boards and team agreements
│   ├── source-to-target-mapping/     # Source column mappings to Bronze/Silver/Gold
│   └── standards/                    # Global team development guidelines
│       ├── git/                      # Git workflow documentation
│       │   ├── README.md             # Git processing guide index
│       │   └── processing-guide/     # Detailed Git workflow sub-docs
│       ├── naming_convention.md      # Python & SQL naming standard
│       └── workspace_fabric_structure.md # Fabric Lakehouse design layout
│
├── fabric/                           # Microsoft Fabric Git integration artifacts
│   ├── Bronze/                       # Bronze ingestion notebooks
│   ├── Config/                       # Pipeline control tables config
│   ├── Gold/                         # Gold dimensional building notebooks
│   ├── Lakehouse/                    # Fabric Lakehouse schema definitions
│   ├── Monitoring/                   # Validation & audit notebooks
│   ├── Pipelines/                    # Data pipeline orchestrator definitions
│   ├── Silver/                       # Silver transformation notebooks
│   └── Source/                       # Ingestion metadata definitions
│
├── sql/                              # Raw SQL scripts and DDLs
│   ├── etl-control/                  # Control flow table and audit scripts
│   ├── lakehouse/                    # Table/view definitions for Lakehouses
│   ├── source/                       # Source system simulator DDLs
│   └── validation/                   # Manual test queries
│
└── tests/                            # Quality assurance and testing suite
    ├── data-quality/                 # PySpark row-level validations
    ├── pipeline-tests/               # Integration pipeline validations
    └── reconciliation/               # Source-to-target reconciliation checks
```

---

## 3. Git & Development Standards

To keep collaboration clean and trackable, all team members must follow our Git branching, naming, and commit conventions.

Detailed Git guidelines are located in the [Git Naming Convention & Structure Guide](docs/standards/git/processing-guide/02-naming-convention-and-structure.md).

### Quick Links to Git processing guides:
- [Repository Structure & Naming Guide](docs/standards/git/processing-guide/02-naming-convention-and-structure.md) – Directory usage rules and general file naming.
- [Branching & Commit Conventions](docs/standards/git/processing-guide/01-branching-and-commit-conventions.md) – Kebab-case branch templates and conventional commit structures.
- [Pull Request Process](docs/standards/git/processing-guide/03-pull-request-process.md) – PR template, reviewer checklists, and merge requirements.
- [Promotion Workflow](docs/standards/git/processing-guide/00-promotion-workflow.md) – Environment progression (Feature → dev → release → main).
- [Conflict Resolution Guide](docs/standards/git/processing-guide/04-conflict-resolution.md) – Resolving conflicts in source code and Jupyter Notebooks.

### Code Naming Conventions:
- For Python variables, classes, SQL tables, and database schemas, follow the [Python & SQL Naming Convention Guide](docs/standards/naming_convention.md).
- For Fabric-specific item structures, consult the [Fabric Workspace Structure Guide](docs/standards/workspace_fabric_structure.md).

---

## 4. Development Workflow Checklist

1. **Start Task**: Create a feature branch off `dev` following [Branching Conventions](docs/standards/git/processing-guide/01-branching-and-commit-conventions.md).
2. **Implement Changes**: Write code using [Code Naming Standards](docs/standards/naming_convention.md).
3. **Commit Code**: Commit changes frequently with [Conventional Commits](docs/standards/git/processing-guide/01-branching-and-commit-conventions.md).
4. **Push & Create PR**: Push feature branch and create a PR using the [PR Template](docs/standards/git/processing-guide/03-pull-request-process.md).
5. **Review & Merge**: Review with checklist, resolve any [Conflicts](docs/standards/git/processing-guide/04-conflict-resolution.md), merge to `dev`, and delete the branch.
