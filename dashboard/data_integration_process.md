# Git Conventions (Branch + Commit)
---

This document outlines the Git branching strategy and commit message standards for the project. Adhering to these conventions ensures a clean repository history, easy tracking of user stories, and streamlined collaboration.

> [!IMPORTANT]
> **Zero Tolerance Policy**: Direct pushes to protected branches (`dev` and `main`) are strictly prohibited. All changes must be proposed via a Pull Request.

---

## 1) Branch Naming Convention

We use a modified Git Flow strategy. Branch names must be mapped to an **Azure DevOps User Story** (or **Bug** for hotfixes/bugfixes) to maintain traceability.

### Branch Types

| Branch Type | Base Branch | Target Branch | Description |
| :--- | :--- | :--- | :--- |
| `main` | - | - | Production-ready stable branch. |
| `dev` | - | - | Integration branch for day-to-day development. |
| `feature/*` | `dev` | `dev` | Active development for new user stories. |
| `release/*` | `dev` | `main` & `dev` | Release candidates (UAT, stabilization). |
| `hotfix/*` | `main` | `main` & `dev` | Urgent production bug fixes. |
| `doc/*` | `dev` / `main` | `dev` / `main` | Documentations, architecture diagrams, and guides. |
| `refactor/*` | `dev` | `dev` | Restructuring existing code/notebooks without changing behavior or adding features. |
| `chore/*` | `dev` | `dev` | Routine maintenance tasks (e.g., folder reorganizations, CI/CD updates, dependency upgrades). |

### Branch Naming Format

Branch names must follow this structure:

```bash
# For items with a ticket (User Story, Task, or Bug)
[type]/[workitem-prefix]-[WorkItemID]-short-description-with-hyphens

# For minor chores/maintenance without a ticket (ad-hoc cleanup, typos)
[type]/no-ref-short-description-with-hyphens
```

*   **`[type]`**: The type of branch (e.g., `feature`, `doc`, `hotfix`, `refactor`, `chore`).
*   **`[workitem-prefix]`**: The type of ticket in Azure DevOps:
    *   `us` — User Story (e.g., `us-1024`)
    *   `task` — Task (e.g., `task-5123`)
    *   `bug` — Bug (e.g., `bug-3051`)
*   **`[WorkItemID]`**: The numeric ID from Azure DevOps.
*   **`no-ref`**: Use this literal prefix if the chore/refactor is extremely minor and does not have an assigned ticket.
*   **`[short-description]`**: A brief description separated by hyphens (kebab-case).

### Branch Naming Rules

- **Lowercase only**: All letters must be lowercase (no uppercase letters).
- **Hyphens only**: Use hyphens (`-`) to separate words. Do not use spaces, underscores (`_`), or special characters.
- **Traceability**: Whenever possible, link your branch to a valid User Story, Task, or Bug ID. Use `no-ref` only for trivial, non-business changes.
- **Single scope**: One branch per ticket/task. Do not bundle unrelated changes.

### Modern Examples

#### Data Engineering & Pipelines
*   `feature/us-1024-ingest-sales-bronze`
*   `feature/task-5123-optimize-silver-join`
*   `feature/us-1028-pipeline-daily-full-load`

#### Lakehouse & Data Warehouse Schema
*   `feature/us-2045-lakehouse-schema-customer-dim`
*   `feature/task-5124-add-warehouse-index`

#### Fixes & Releases
*   `hotfix/bug-3051-fix-null-orderdate-pipeline`
*   `release/v1.3.1`

#### Documentation
*   `doc/us-4012-git-workflow-guide`
*   `doc/no-ref-fix-typo-branching-and-commit-conventions-md`

#### Project Structure & Maintenance (Chores / Refactoring)
*   `chore/us-5011-reorganize-kpi-docs-folders`
*   `chore/no-ref-cleanup-temp-files`
*   `refactor/task-5128-restructure-bronze-notebooks`

---

## 2) Commit Message Convention

We follow a structured commit style similar to Conventional Commits, which links commits back to their Azure DevOps work items.

### Commit Message Format

```text
<type>(<scope>): <short summary> (#<work-item-id>)

[Optional body: detailed explanation of WHY the change was made]

[Optional footer: breaking changes or ticket references]
```

*   **`<type>`**: Describes the intent of the commit (see table below).
*   **`<scope>`** *(Optional)*: The specific component or layer affected (e.g., `bronze`, `silver`, `gold`, `dwh`, `pipeline`, `config`).
*   **`<short summary>`**: A concise description in the present tense (e.g., "add customer table", not "added customer table").
*   **`(#<work-item-id>)`**: The Azure DevOps User Story or Bug ID prefixed with `#` (e.g., `#1024`).

### Commit Types

| Type | Description | Example |
| :--- | :--- | :--- |
| `feat` | New notebook, pipeline, dataflow, or serving table | `feat(bronze): add sales ingestion notebook (#1024)` |
| `fix` | Bug fix in code or pipeline | `fix(silver): handle null order_date values (#3051)` |
| `refactor` | Restructuring code without changing behavior | `refactor(gold): optimize dim_customer join logic (#1025)` |
| `perf` | Performance optimization | `perf(pipeline): optimize partition keys (#1028)` |
| `schema` | Schema, DDL, or table definition changes | `schema(dwh): add tax_amount column to fact_transactions (#2046)` |
| `config` | Parameter, config, or pipeline metadata changes | `config: parameterize storage account names (#1028)` |
| `docs` | Documentation edits, README updates | `docs(git): update conventions for user stories (#4012)` |
| `test` | Quality checks, unit tests, data assertions | `test: add null value check for customer_id (#2045)` |
| `chore` | Routine tasks, maintenance, dependency updates | `chore: clean up deprecated staging files (#1024)` |

### Good Commit Examples

```bash
feat(bronze): add sales ingestion notebook from Azure SQL (#1024)
fix(silver): handle null values in order_date column (#3051)
schema(dwh): add tax_amount to fact_transactions (#2046)
config(pipeline): parameterize storage account name in daily load (#1028)
```

> [!TIP]
> Keep the commit subject line under 72 characters. If you need to write a more detailed explanation, leave a blank line and write a paragraph explaining **why** the change was made, not **how** (the code shows how).
