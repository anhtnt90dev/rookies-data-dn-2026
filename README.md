# Rookie Data DN 2026

## Project Overview

This repository organizes documentation, source code, SQL scripts, configuration templates, test assets, and deliverables for the Rookie Data DN 2026 analytics project.

Pipelines and runtime execution are implemented in Microsoft Fabric. Use GitHub for source control and code review: create branches for changes and open Pull Requests for review before merging.

## Repository Structure

```text
rookies-data-dn-2026/
├─ README.md
├─ .gitignore
├─ config/
├─ docs/
│  ├─ architecture/
│  ├─ business-process/
│  │  └─ diagrams/
│  ├─ data-modeling/
│  ├─ source-to-target-mapping/
│  ├─ project-management/
│  └─ final-deliverables/
├─ fabric/
├─ sql/
│  ├─ source/
│  ├─ lakehouse/
│  ├─ etl-control/
│  └─ validation/
├─ tests/
│  ├─ data-quality/
│  ├─ reconciliation/
│  └─ pipeline-tests/
└─ .gitkeep (used in empty folders)
```

## Directory summary

- **docs/** — Project documentation and reviewable deliverables.
  - `docs/business-process/`: Business process notes and diagrams.
  - `docs/architecture/`: Solution and data architecture diagrams and design notes.
  - `docs/data-modeling/`: Data models, ERDs, dimension/fact definitions.
  - `docs/source-to-target-mapping/`: Source-to-target mapping specifications.
  - `docs/project-management/`: Conventions, plans, and team guidelines.
  - `docs/final-deliverables/`: Finalized artifacts for stakeholder review.

- **fabric/** — Artifacts synchronized from Microsoft Fabric (pipelines, notebooks, semantic models). Do not manually create Fabric-managed subfolders.

- **sql/** — SQL scripts for source setup, lakehouse definitions, ETL control, and validation.
  - `sql/source/`: Source schema and seed data scripts.
  - `sql/lakehouse/`: Lakehouse table and view definitions.
  - `sql/etl-control/`: ETL control and audit tables, procedures.
  - `sql/validation/`: Reconciliation and data validation scripts.

- **config/** — Non-sensitive configuration templates. Do not commit secrets or credentials. Use example files like `pipeline_config.example.json`.

- **tests/** — Test assets and scripts.
  - `tests/data-quality/`: Data quality rules and test cases.
  - `tests/reconciliation/`: Source-to-target reconciliation tests.
  - `tests/pipeline-tests/`: Pipeline execution and end-to-end tests.

## Empty folder handling

Git does not track empty folders. Keep `.gitkeep` in empty directories to preserve structure; remove it when the folder contains real files.

## Naming conventions

- Use lowercase names.
- Prefer hyphens (`-`) instead of spaces.
- Keep names short and descriptive; include `task-XXX` or version suffixes when helpful.

Examples: `task-112-description-ac.md`, `source-to-target-mapping-customer.md`.

## Git & Pull Request guidelines

Before opening a PR:

1. Place files in the appropriate directory.
2. Avoid duplicate or overlapping folders.
3. Do not commit local cache files, temp files, or secrets.
4. Update this `README.md` if the folder structure changes.
5. Describe changed folders and important artifacts in the PR description.

Suggested PR template:

```md
## Summary

- Short description of the change.

## Changes

- List of added/updated files or folders.

## Review notes

- What reviewers should focus on.
```

## Microsoft Fabric + GitHub

Microsoft Fabric runs and schedules pipelines; GitHub is used for source control and review. Keep the `fabric/` folder reserved for artifacts created by Fabric Git Integration to avoid conflicts.

---

If you'd like, I can:

- add a bilingual (EN/VI) README version,
- commit this change to the current branch and open a Pull Request.
