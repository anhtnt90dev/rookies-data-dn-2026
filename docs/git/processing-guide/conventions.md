# Git Conventions (Branch + Commit)
---

## 1) Branch naming convention

### Branch types (Git Flow)

- `main` — production
- `develop` — integration branch for dev
- `feature/*` — normal day-to-day work (branch from `develop`, PR back to `develop`)
- `release/*` — UAT / stabilisation (branch from `develop`, PR to `main` + back-merge to `develop`)
- `hotfix/*` — urgent production fixes (branch from `main`, PR to `main` + back-merge to `develop`)

### Naming format

`type/short-description-with-hyphens`

**Rules (always):**
- Lowercase + hyphens only (no spaces, no underscores, no capitals)
- Keep it short but meaningful (about 3–6 words)
- One branch per task/ticket (don’t bundle unrelated work)

### Examples

```bash
# Data Engineering
feature/ingest-sales-bronze
feature/transform-orders-silver
feature/pipeline-daily-full-load

# Lakehouse / Warehouse
feature/lakehouse-schema-customer-dim
feature/warehouse-fact-transactions

# Fixes / releases
hotfix/fix-null-orderdate-pipeline
release/v1.3.1
```

---

## 2) Commit message convention

### Format

```
<type>: <short summary>

[Optional: why we changed it (not how)]

[Optional: ticket reference]
```

### Types

- `feat` — new notebook/pipeline/dataflow/table/feature
- `fix` — bug fix
- `refactor` — restructure without changing behavior
- `perf` — performance improvement
- `schema` — schema/DDL change
- `config` — config/parameters/settings
- `docs` — documentation
- `test` — validation/data-quality/tests
- `chore` — maintenance

### Good examples

```bash
feat: add bronze ingestion notebook for sales orders from Azure SQL
fix: handle null values in order_date column during silver transform
schema: add tax_amount column to warehouse fact_transactions table
config: parameterise storage account name in daily pipeline
```
