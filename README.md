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
├── architecture/                     # System architecture design & diagrams
│   ├── README.md                     # Architecture guide index
│   ├── data-quality/                 # Data quality and validation specs
│   ├── diagrams/                     # Mermaid diagram source files
│   ├── exports/                      # Exported diagram images (PNG)
│   ├── team-1/                       # Logging, workflow, and Gold ingestion designs
│   └── team-2/                       # Fabric workspace accessibility design
│
├── config/                           # Environment configuration templates
│   └── .gitkeep
│
├── docs/                             # Project documentation and designs
│   ├── business-process/             # Business workflow analysis
│   ├── data-modeling/                # Star schema designs & table definitions
│   ├── final-deliverables/           # Reviewed deliverables for submissions
│   ├── project-management/           # Sprint boards and team agreements
│   ├── source-to-target-mapping/     # Source column mappings to Bronze/Silver/Gold
│   └── standards/                    # Global team development guidelines
│
├── fabric/                           # Microsoft Fabric Git integration artifacts (managed by Fabric workspace)
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

Detailed Git guidelines are located in the [Git Naming Convention & Structure Guide](docs/standards/git-rules/processing-guide/02-naming-convention-and-structure.md).

### Quick Links to Git processing guides:
- [Repository Structure & Naming Guide](docs/standards/git-rules/processing-guide/02-naming-convention-and-structure.md) – Directory usage rules and general file naming.
- [Branching & Commit Conventions](docs/standards/git-rules/processing-guide/01-branching-and-commit-conventions.md) – Kebab-case branch templates and conventional commit structures.
- [Pull Request Process](docs/standards/git-rules/processing-guide/03-pull-request-process.md) – PR template, reviewer checklists, and merge requirements.
- [Promotion Workflow](docs/standards/git-rules/processing-guide/00-promotion-workflow.md) – Environment progression (Feature → dev → release → main).
- [Conflict Resolution Guide](docs/standards/git-rules/processing-guide/04-conflict-resolution.md) – Resolving conflicts in source code and Jupyter Notebooks.

### Code Naming Conventions:
- For Python variables, classes, SQL tables, and database schemas, follow the [Python & SQL Naming Convention Guide](docs/standards/fabric-rules/naming-convention.md).
- For Fabric-specific item structures, consult the [Fabric Workspace Structure Guide](docs/standards/fabric-rules/workspace-fabric-structure.md).

---

## 4. Development Workflow Checklist

1. **Start Task**: Create a feature branch off `dev` following [Branching Conventions](docs/standards/git-rules/processing-guide/01-branching-and-commit-conventions.md).
2. **Implement Changes**: Write code using [Code Naming Standards](docs/standards/fabric-rules/naming-convention.md).
3. **Commit Code**: Commit changes frequently with [Conventional Commits](docs/standards/git-rules/processing-guide/01-branching-and-commit-conventions.md).
4. **Push & Create PR**: Push feature branch and create a PR using the [PR Template](docs/standards/git-rules/processing-guide/03-pull-request-process.md).
5. **Review & Merge**: Review with checklist, resolve any [Conflicts](docs/standards/git-rules/processing-guide/04-conflict-resolution.md), merge to `dev`, and delete the branch.

