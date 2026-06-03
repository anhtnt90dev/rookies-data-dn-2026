# Bronze → Silver Pipeline — Implementation Guide

# Branch: feature/task-123-implement-dynmaic-loading-table-silver

> **Medallion Architecture | Microsoft Fabric | PySpark + Delta Format**


---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Component Map](#2-component-map)
3. [Step-by-Step Implementation](#3-step-by-step-implementation)
   - [Step 1 — Read Config Table](#step-1--read-config-table)
   - [Step 2 — Load JSON Mapping File](#step-2--load-json-mapping-file)
   - [Step 3 — Read Bronze Source Table](#step-3--read-bronze-source-table)
   - [Step 4 — Apply Column Transformations](#step-4--apply-column-transformations)
   - [Step 5 — Data Quality Validation](#step-5--data-quality-validation)
   - [Step 6 — MERGE INTO Silver (Delta Upsert)](#step-6--merge-into-silver-delta-upsert)
   - [Step 7 — Write Audit Log](#step-7--write-audit-log)
4. [Team Task Assignment](#4-team-task-assignment)
5. [Interface Contract (Handoff Points)](#5-interface-contract-handoff-points)
6. [Notebook Structure](#6-notebook-structure)
7. [Fabric Pipeline Orchestration](#7-fabric-pipeline-orchestration)
8. [Error Handling Strategy](#8-error-handling-strategy)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       FABRIC PIPELINE (Scheduled)               │
│                                                                  │
│  ┌──────────────┐    ┌────────────────────────────────────────┐  │
│  │ Config Table │───▶│         MAIN ORCHESTRATOR              │  │
│  │  (metadata)  │    │   foreach (source → target) row        │  │
│  └──────────────┘    └───────────┬────────────────────────────┘  │
│                                  │ iterate per table             │
│  ┌──────────────┐                ▼                               │
│  │  JSON Mapping│    ┌────────────────────────────────────────┐  │
│  │  Files       │───▶│         NOTEBOOK: ingest_bronze_silver │  │
│  │  (per table) │    │                                        │  │
│  └──────────────┘    │  1. Read Bronze (Delta)                │  │
│                      │  2. Apply column mapping               │  │
│  ┌──────────────┐    │  3. Handle nulls / defaults            │  │
│  │ Bronze Layer │───▶│  4. Run Data Quality validation                  │  │
│  │ (Delta)      │    │  5. MERGE INTO Silver (Delta)          │  │
│  └──────────────┘    │  6. Call audit_log()                   │  │
│                      └────────────────────────────────────────┘  │
│                                  │                               │
│                                  ▼                               │
│                      ┌────────────────────────────────────────┐  │
│                      │         Silver Layer (Delta)            │  │
│                      └────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Map

| Component | Type | Status | Owner |
|---|---|---|---|
| Config table reader | Notebook utility | 🔨 To build | VinhPM |
| JSON mapping file loader | Utility function | ✅ Already done | — |
| Config reader utility | Utility function | ✅ Already done | — |
| DB connection / session manager | Utility function | ✅ Already done | — |
| Bronze reader (Delta) | Notebook | 🔨 To build | VinhPM |
| Column transformation engine | Notebook | 🔨 To build | VinhPM |
| Null / default handler   | Notebook | 🔨 To build | Trang |
| DQ validation engine | Notebook | 🔨 To build | Trang |
| Silver MERGE INTO (Delta upsert) | Notebook | 🔨 To build | VinhPM |
| Audit log caller | Wrapper call | 🔨 To build | Trang |
| Fabric Pipeline wiring | Pipeline activity | 🔨 To build | Together |

---

## 3. Step-by-Step Implementation

---

### Step 1 — Read Config Table

**Owner:** VinhPM
**Type:** Notebook cell / utility function

#### Purpose
Query the central config table to get the full list of ingestion jobs. Each row defines one Bronze → Silver pipeline run.

#### Input

| Source | Detail |
|---|---|
| Config table (SQL/Delta) | `cfg.source_tables` or equivalent table name |
| Columns needed | `source_table`, `target_table`, `mapping_json_path_bronze_silver`, `is_active`, `merge_keys` (this maybe difference name but same meaning) |



#### Implementation Notes
- Use existing **config reader utility** (already implemented)
- If config table is empty or returns 0 rows → raise a clear exception, do not silently skip
- Log number of active pipelines found before starting the loop

---

### Step 2 — Load JSON Mapping File

**Owner:** Trang
**Type:** Notebook utility call

#### Purpose
For each row from the config table, load the corresponding JSON mapping file that defines how columns are mapped from Bronze to Silver.

#### Input

| Source | Detail |
|---|---|
| `mapping_json_path` | File path string from config row (e.g. `/mappings/bronze_silver/agent.json`) |
| JSON loader utility | Already implemented — call it directly |

**Example mapping JSON file:**

```json
{
  "source_table": "bronze.agent",
  "target_table": "silver.agent",
  "columns": [
    {
      "target": "agent_key",
      "expression": null
    },
    {
      "target": "agent_id",
      "expression": "agent_id"
    },
    {
      "target": "agent_name",
      "expression": "agent_name"
    },
    {
      "target": "full_name",
      "expression": "concat(first_name, ' ', last_name)"
    }
  ]
}
```

#### Output

```python
# Parsed mapping object — this is the HANDOFF CONTRACT 
mapping: dict = {
    "source_table": "bronze.agent",
    "target_table": "silver.agent",
    "columns": [
        {"target": "agent_key",  "expression": None},
        {"target": "agent_id",   "expression": "agent_id"},
        {"target": "agent_name", "expression": "agent_name"},
        {"target": "full_name",  "expression": "concat(first_name, ' ', last_name)"}
    ]
}
```

#### Implementation Notes
- Use existing **JSON mapping file loader** utility
- Validate loaded JSON has required keys: `source_table`, `target_table`, `columns`
- If `expression` is `null` → that column will be handled in Step 4 (null/default handling)
- Raise `ValueError` if mapping file not found or JSON is malformed

---

### Step 3 — Read Bronze Source Table

**Owner:** VinhPM
**Type:** Notebook cell (PySpark)

#### Purpose
Read the Bronze Delta table into a Spark DataFrame using the `source_table` from the config row.

#### Input

| Source | Detail |
|---|---|
| `mapping["source_table"]` | String, e.g. `"bronze.agent"` |
| Spark session | From existing DB connection/session manager |

#### Output

| Output | Type | Detail |
|---|---|---|
| `df_bronze` | `pyspark.sql.DataFrame` | All raw columns from Bronze table, no transformation yet |
| Row count (logged) | `int` | Log before returning for audit |

#### Implementation Notes
- Do **not** filter or transform here — read raw as-is
- Log: `f"Read {df_bronze.count()} rows from {source_table}"`
- If table does not exist → catch `AnalysisException`, log error, raise to pipeline

---

### Step 4 — Apply Column Transformations

**Owner:** VinhPM
**Type:** Notebook cell (PySpark)

#### Purpose
Build the Silver DataFrame by selecting and transforming columns from Bronze based on the mapping JSON. Handle:
1. Direct column renames (`expression == source_column_name`)
2. SQL expressions (`expression == "concat(a, ' ', b)"`)
3. Generate by system or default value handling   (`expression == null`)

#### Input

| Source | Type | Detail |
|---|---|---|
| `df_bronze` | `DataFrame` | Output of Step 3 |
| `mapping["columns"]` | `List[dict]` | Output of Step 2 — `[{target, expression}]` |
| `null_defaults` | `dict` | Config-level defaults per data type (e.g. `{"string": "", "int": 0}`) |


#### Output

| Output | Type | Detail |
|---|---|---|
| `df_mapped` | `DataFrame` | Only Silver target columns, renamed and transformed |

#### Implementation Notes
- Column order in output follows the order defined in mapping JSON
- If an `expression` references a column that doesn't exist in Bronze → catch and raise `ColumnNotFoundError` with clear message
- Log which columns were mapped, which used defaults

---

### Step 5 — Data Quality Validation

**Owner:** Trang
**Type:** Notebook cell (PySpark)

#### Purpose
Run DQ rules against the mapped DataFrame. Flag or quarantine records that fail validation before writing to Silver.

#### Input

| Source | Type | Detail |
|---|---|---|
| `df_mapped` | `DataFrame` | Output of Step 4 — transformed DataFrame |
| DQ rules config | `List[dict]` | Rules defined per column: type checks, null checks, range checks |

**Example DQ rules structure:**

```python
dq_rules = [
    {"column": "agent_id",   "rule": "not_null"},
    {"column": "agent_name", "rule": "not_null"},
    {"column": "agent_id",   "rule": "unique"},
    {"column": "status",     "rule": "in_set", "values": ["active", "inactive"]}
]
```


#### Output

| Output | Type | Detail |
|---|---|---|
| `df_valid` | `DataFrame` | Clean rows — ready for MERGE INTO Silver |
| `df_rejected` | `DataFrame` | Failed rows with `__dq_failure_reason` column |
| `dq_failure_count` | `int` | Count of rejected rows — passed to audit log |

#### Implementation Notes
- `df_rejected` should be written to a **quarantine table** (e.g. `silver_quarantine.agent`) for investigation
- If `df_valid` is empty after DQ → skip MERGE step, log warning, mark run as `WARNING` not `FAILED`
- DQ rules can be extended; the engine should be generic (not hardcoded per table)

---

### Step 6 — MERGE INTO Silver (Delta Upsert)

**Owner:** VinhPM
**Type:** Notebook cell (PySpark / SQL)

#### Purpose
Write validated records to the Silver Delta table using a dynamic `MERGE INTO` statement. Update existing records on match, insert new ones.

#### Input

| Source | Type | Detail |
|---|---|---|
| `df_valid` | `DataFrame` | Output of Step 5 — clean rows only |
| `mapping["target_table"]` | `str` | e.g. `"silver.agent"` |
| `config["merge_keys"]` | `List[str]` | Join keys for MERGE, e.g. `["agent_id"]` |




#### Output

| Output | Type | Detail |
|---|---|---|
| Silver Delta table updated | Side effect | Rows upserted into `target_table` |
| `merge_stats` | `dict` | `{target_table, rows_attempted}` — passed to audit log |

#### Implementation Notes
- Silver table must already exist (DDL is out of scope for this notebook — managed separately)
- If Silver table does not exist → catch exception, log, do NOT create it automatically
- `merge_keys` must not be empty — validate before building SQL
- Consider adding `_ingestion_timestamp` column to track when each record was last upserted

---

### Step 7 — Write Audit Log

**Owner:** Trang (wrap the existing function)
**Type:** Notebook cell — call existing `audit_log()` common function

#### Purpose
Record the result of each table ingestion run into the central audit log for observability, debugging, and SLA tracking.

#### Input

Collect the following metadata during steps 1–6 and pass to `audit_log()`:

| Parameter | Type | Source |
|---|---|---|
| `pipeline_name` | `str` | `"bronze_to_silver"` |
| `source_table` | `str` | From config row (Step 1) |
| `target_table` | `str` | From mapping (Step 2) |
| `run_timestamp` | `datetime` | Captured at start of notebook run |
| `rows_read` | `int` | From Step 3 |
| `rows_valid` | `int` | `df_valid.count()` from Step 5 |
| `rows_rejected` | `int` | `df_rejected.count()` from Step 5 |
| `rows_merged` | `int` | From merge stats in Step 6 |
| `status` | `str` | `"SUCCESS"` / `"WARNING"` / `"FAILED"` |
| `error_message` | `str` | Exception message if any, else `None` |



#### Output

| Output | Type | Detail |
|---|---|---|
| Audit log record written | Side effect | 1 row per table per run in audit log table |

#### Implementation Notes
- Call `audit_log()` inside a `finally` block so it always runs — even on failure
- On failure: set `status = "FAILED"`, populate `error_message` with exception string
- On DQ-only warning (all rows rejected, no rows merged): set `status = "WARNING"`

---

## 4. Team Task Assignment

### VinhPM — PySpark  (Steps 3, 4, 6)

| Task | Step | Notes |
|---|---|---|
| Notebook: read Bronze Delta table | Step 3 | Use session manager utility |
| Dynamic column selector from mapping | Step 4 | Handle `null`, direct ref, and SQL expression |
| Null / default handling logic | Step 4 | Generic across all tables |
| Dynamic `MERGE INTO` Silver Delta | Step 6 | Build SQL from `merge_keys` and column list |
| Error handling + retry wrapper | All | Wrap per-table loop in try/except |
| End-to-end integration test | — | Test with 1 real table pair |

### Trang —  Quality, Audit (Steps 1, 2, 5, 7)

| Task | Step | Notes |
|---|---|---|
| Load and validate mapping JSON per row | Step 2 | Use JSON loader utility |
| DQ rule engine (null, type, range, unique) | Step 5 | Generic, table-agnostic |
| DQ quarantine write (rejected rows) | Step 5 | Write to `silver_quarantine.*` |
| Audit log wrapper + metadata collection | Step 7 | Collect stats from Steps 3–6 |
| Unit tests for config reader + DQ rules | — | Use sample data |

---

## 5. Interface Contract (Handoff Points)

> ⚠️ **Agree on these before starting development.** These are the data contracts between Trang's output (Steps 1–2) and VinhPM's input (Steps 3–4).

### Contract A — Mapping object structure

```python
# Trang produces this → VinhPM consumes this
mapping: dict = {
    "source_table": str,        # e.g. "bronze.agent"
    "target_table": str,        # e.g. "silver.agent"
    "columns": [
        {
            "target": str,          # Silver column name
            "expression": str | None  # Bronze col name, SQL expression, or None
        }
    ]
}
```

### Contract B — Config row structure

```python
# Trang produces this → used by both
config_row: dict = {
    "source_table":       str,        # e.g. "bronze.agent"
    "target_table":       str,        # e.g. "silver.agent"
    "mapping_json_path":  str,        # e.g. "/mappings/bronze_silver/agent.json"
    "merge_keys":         List[str],  # e.g. ["agent_id"]
    "is_active":          bool
}
```

### Contract C — DQ output to merge step

```python
# Trang produces this → VinhPM consumes for MERGE
df_valid:    DataFrame  # Only clean rows, same schema as df_mapped
df_rejected: DataFrame  # Failed rows with __dq_failure_reason column added
dq_failure_count: int   # Count of rejected rows
```

---

## 6. Notebook Structure

**Recommended: single notebook `nb_ingest_bronze_silver` with clear cell sections.**

```
nb_ingest_bronze_silver
│
├── [Cell 1]  PARAMETERS  (Fabric notebook parameters)
│             - config_table_name: str
│             - mapping_base_path: str
│             - run_env: str  (dev / prod)
│
├── [Cell 2]  IMPORTS & SETUP
│             - SparkSession from session manager
│             - Import utility functions
│
├── [Cell 3]  STEP 1 — Read config table
│             - config_list = read_config_table(config_table_name)
│
├── [Cell 4]  MAIN LOOP — foreach config row
│   │
│   ├── [Cell 4a]  STEP 2  — Load mapping JSON
│   ├── [Cell 4b]  STEP 3  — Read Bronze table
│   ├── [Cell 4c]  STEP 4  — Apply transformations
│   ├── [Cell 4d]  STEP 5  — DQ validation
│   ├── [Cell 4e]  STEP 6  — MERGE INTO Silver
│   └── [Cell 4f]  STEP 7  — Write audit log (in finally block)
│
└── [Cell 5]  SUMMARY LOG
              - Print total tables processed, success/warning/failed counts
```

---

## 7. Fabric Pipeline Orchestration

```
Fabric Pipeline: pl_bronze_to_silver
│
├── Activity 1: Notebook  →  nb_ingest_bronze_silver
│   Parameters:
│     - config_table_name = "pipeline.ingest_config"
│     - mapping_base_path = "/mappings/bronze_silver/"
│     - run_env = @pipeline().parameters.run_env
│
└── On Failure:
    └── Activity 2: Stored Procedure → sp_notify_pipeline_failure
        Parameters: pipeline_name, run_id, error_message
```

**Pipeline parameters to expose:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `run_env` | string | `prod` | Environment tag for audit log |
| `force_full_reload` | bool | `false` | If true, skip MERGE and overwrite |

---

## 8. Error Handling Strategy

| Failure Point | Behavior | Status |
|---|---|---|
| Config table empty | Raise exception, stop pipeline | `FAILED` |
| Mapping JSON not found | Skip this table, continue loop | `FAILED` (per table) |
| Bronze table not found | Skip this table, continue loop | `FAILED` (per table) |
| Column in expression not in Bronze | Skip this table, log details | `FAILED` (per table) |
| All rows rejected by DQ | Skip MERGE, write to quarantine | `WARNING` |
| Some rows rejected by DQ | Merge valid rows, write quarantine | `SUCCESS` with note |
| MERGE INTO fails | Log full error, continue loop | `FAILED` (per table) |
| Audit log call fails | Log to notebook output, do not re-raise | — |

> **Key principle:** A failure on one table should **not** stop other tables from processing. Use a per-table try/except block and always call `audit_log()` in `finally`.

---

*End of document*