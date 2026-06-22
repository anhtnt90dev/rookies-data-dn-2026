# Git Repository Structure & Naming Conventions

This document outlines the standard folder organization, directory usage rules, and file naming conventions for the Git repository. Adhering to these standards ensures consistency across environments and team collaboration.

---

## 1. Git Repository Directory structure

```text
rookies-data-dn-2026/
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
├── config/
│   └── .gitkeep
│
├── docs/
│   ├── business-process/
│   ├── data-modeling/
│   ├── final-deliverables/
│   ├── project-management/
│   ├── source-to-target-mapping/
│   └── standards/
│
├── fabric/                           # Microsoft Fabric Git integration artifacts (managed by Fabric workspace)
│
├── sql/
│   ├── etl-control/
│   ├── lakehouse/
│   ├── source/
│   └── validation/
│
└── tests/
    ├── data-quality/
    ├── pipeline-tests/
    └── reconciliation/
```

---

## 2. Directory Roles & Usage Rules

### `config/`
Stores configuration templates and environment-independent setup files.
- **Examples**: pipeline parameter files, schema definitions, template structures.
- > [!CAUTION]
  > Never commit credentials, passwords, access keys, or personal access tokens (PATs) here.

### `architecture/`
Stores system architecture blueprints, technical designs, diagrams, and logs strategies.
- `architecture/data-quality/`: Data quality and validation strategies.
- `architecture/diagrams/`: Mermaid source files.
- `architecture/exports/`: Exported diagram images (PNG).
- `architecture/team-1/` & `architecture/team-2/`: Team-specific architectural designs.

### `docs/`
Serves as the central repository for all project documentation.

| Sub-directory | Contents & Purpose |
| :--- | :--- |
| `docs/business-process/` | BPMN diagrams, sequence diagrams, business rules, and status mapping documents. |
| `docs/data-modeling/` | Star schema designs, dimension definitions, fact table schemas, and ERDs. |
| `docs/final-deliverables/` | Stakeholder-reviewed, final versioned artifacts and presentations. |
| `docs/project-management/` | Sprint planning documents, retrospective notes, and team guidelines. |
| `docs/source-to-target-mapping/`| Column-level mapping files from landing zones to Bronze, Silver, and Gold. |
| `docs/standards/` | Global project standards, workspace layouts, naming systems, and Git guides. |

### `fabric/`
Reserved exclusively for Microsoft Fabric Git Integration.
- **Usage**: Automatically populated by the Fabric Workspace Git Integration client.
- > [!WARNING]
  > Do not manually restructure files or subdirectories under `fabric/` as it will break the Fabric Git Integration.

### `sql/`
Stores database and data warehouse DDL and validation scripts.
- `sql/source/`: DDL scripts representing source tables.
- `sql/lakehouse/`: Table and view definition scripts for Silver/Gold layers.
- `sql/etl-control/`: Control table scripts and stored procedures.
- `sql/validation/`: Manual testing and query validation scripts.

### `tests/`
Stores automated testing suites and reconciliation logic.
- `tests/data-quality/`: PySpark or SQL validation checks for nulls, range bounds, and data formats.
- `tests/pipeline-tests/`: Verification scripts for integration pipelines.
- `tests/reconciliation/`: Source-to-target row count and financial validation routines.

---

## 3. General Naming Conventions

For all files, folders, and documentation created directly in Git:

### Formatting Rules
- **Lowercase only**: No uppercase characters in directory or file names.
- **Hyphen separated**: Use hyphens (`-`) instead of spaces or underscores (e.g. `cancellation-report.md`, not `cancellation_report.md` or `cancellation report.md`).
- **No redundant terms**: Avoid repeating context in names (e.g., inside the `git/` directory, files should avoid repeating the word `git-` unless necessary).

### General Examples
- **Documentation**: `surrogate-key-handling-approach.md`
- **Diagrams**: `high-level-architecture-v1.2.png`

---

## 4. Code & Workspace Specific Naming Standards

For naming standards inside SQL warehouses, PySpark code, or Fabric Workspace layout, refer to the specialized standards documents:

- **Python & SQL Object Naming**: Refer to [naming-convention.md](../../fabric-rules/naming-convention.md) (covers classes, variables, database schemas, tables, columns, constraints).
- **Microsoft Fabric Item & Tables Layout**: Refer to [workspace-fabric-structure.md](../../fabric-rules/workspace-fabric-structure.md) (covers Lakehouse folders, Delta table naming, and system logging tables).

---

## 5. Empty Folder Tracking
Git does not track empty folders. To ensure the folder structure is preserved across all clones, place a `.gitkeep` file inside empty directories:
```text
.gitkeep
```
