# CarPro Naming Convention Guide

**Project:** CarPro Insurance Analytics
**Purpose:** Provide one shared naming standard for Python code and SQL objects so the team can develop consistently.

---

## 1. Core Rules

These rules apply to both Python and SQL unless a platform requires a different format.

| Rule               | Standard                                                                                                                   |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Language           | Use English names only.                                                                                                    |
| Meaning            | Names must explain business meaning or technical purpose.                                                                  |
| Style              | Prefer readable names over short unclear names.                                                                            |
| Abbreviation       | Avoid abbreviations unless they are common in the project, such as `id`, `url`, `api`, `sql`, `json`, `etl`, `kpi`, `rls`. |
| Consistency        | Use the same word for the same concept everywhere. Do not mix `client`, `customer`, and `user` for the same entity.        |
| Separator          | Use underscores for most technical assets: `snake_case`.                                                                   |
| Spaces             | Do not use spaces in Python identifiers or SQL identifiers.                                                                |
| Special characters | Avoid special characters except `_`.                                                                                       |
| Case sensitivity   | Treat names as if they are case-sensitive even when the platform is not. Do not create names that differ only by case.     |
| Reserved words     | Do not use reserved words such as `order`, `user`, `table`, `date`, `group`, `select`, `from`, `where`.                    |
| Searchability      | Use names that are easy to search in code and SQL scripts.                                                                 |

---

## 2. Python Naming Convention

Python code should follow PEP 8 style, adapted for the CarPro data engineering project.

### 2.1 Python Naming Summary

| Python Item             | Style                                          | Example                                        |
| ----------------------- | ---------------------------------------------- | ---------------------------------------------- |
| Package                 | short lowercase, avoid underscores if possible | `carpro`, `insurance`                          |
| Module / `.py` file     | `snake_case.py`                                | `policy_transform.py`                          |
| Class                   | `PascalCase` / `CapWords`                      | `PolicyTransformer`                            |
| Exception class         | `PascalCase` + `Error`                         | `InvalidPolicyStatusError`                     |
| Function                | `snake_case`                                   | `standardize_policy_status()`                  |
| Method                  | `snake_case`                                   | `validate_schema()`                            |
| Variable                | `snake_case`                                   | `policy_df`                                    |
| Constant                | `UPPER_SNAKE_CASE`                             | `DEFAULT_TIMEZONE`                             |
| Enum class              | `PascalCase`                                   | `PolicyStatus`                                 |
| Enum value              | `UPPER_SNAKE_CASE`                             | `ACTIVE`                                       |
| Test file               | `test_<module>.py`                             | `test_policy_transform.py`                     |
| Test function           | `test_<expected_behavior>`                     | `test_policy_status_is_active_after_payment()` |
| Private/internal helper | `_leading_underscore`                          | `_normalize_status_code()`                     |
| Dunder method           | Use only Python-defined names                  | `__init__`, `__repr__`                         |
| Type variable           | short `PascalCase` or single capital           | `T`, `EntityT`                                 |

### 2.2 Python Package and Module Names

Use short lowercase module names. Use underscores only when they improve readability.

Good:

```text
policy_transform.py
payment_validation.py
watermark_manager.py
fabric_lakehouse.py
```

Bad:

```text
PolicyTransform.py
paymentValidation.py
payment-validation.py
utils2.py
misc.py
```

### 2.3 Python Class Names

Use `PascalCase`. Class names should usually be nouns or noun phrases.

Good:

```python
class PolicyTransformer:
    pass

class PaymentValidator:
    pass

class WatermarkManager:
    pass

class InvalidQuotationStatusError(Exception):
    pass
```

Bad:

```python
class policy_transformer:
    pass

class DoPaymentValidation:
    pass

class Error1(Exception):
    pass
```

### 2.4 Python Function and Method Names

Use `snake_case`. Function names should usually start with a verb because functions do something.

Recommended verbs for this project:

| Purpose          | Verb Examples                                              |
| ---------------- | ---------------------------------------------------------- |
| Ingestion        | `load`, `read`, `extract`, `ingest`                        |
| Transformation   | `transform`, `standardize`, `normalize`, `clean`, `enrich` |
| Validation       | `validate`, `check`, `assert`                              |
| Writing          | `write`, `save`, `merge`, `upsert`                         |
| Audit            | `log`, `record`, `capture`                                 |
| Configuration    | `get`, `set`, `update`, `resolve`                          |
| Building objects | `build`, `create`, `generate`                              |

Good:

```python
def load_policy_from_landing(path: str):
    pass


def standardize_policy_status(policy_df):
    pass


def validate_payment_schema(payment_df):
    pass


def write_audit_record(audit_df):
    pass
```

Bad:

```python
def process(data):
    pass


def policy(data):
    pass


def do_stuff(x):
    pass
```

### 2.5 Python Variable Names

Use `snake_case`. Variables should describe the content, not only the type.

Good:

```python
policy_df = read_delta_table("bronze_policy")
valid_payment_df = filter_valid_payments(payment_df)
source_row_count = policy_df.count()
latest_watermark_value = get_latest_watermark("policy")
```

Bad:

```python
df = read_delta_table("bronze_policy")
x = df.count()
val = get_latest_watermark("policy")
```

Short names are allowed only for very small local scopes where the meaning is obvious:

```python
for row in rows:
    print(row)

for i, column_name in enumerate(column_names):
    print(i, column_name)
```

### 2.6 DataFrame Naming

Because this is a data engineering project, DataFrame names should identify the entity and stage.

| Data Object          | Pattern                   | Example                |
| -------------------- | ------------------------- | ---------------------- |
| Raw source DataFrame | `<entity>_raw_df`         | `policy_raw_df`        |
| Bronze DataFrame     | `<entity>_bronze_df`      | `policy_bronze_df`     |
| Silver DataFrame     | `<entity>_silver_df`      | `policy_silver_df`     |
| Gold DataFrame       | `<entity>_gold_df`        | `policy_gold_df`       |
| Valid records        | `valid_<entity>_df`       | `valid_payment_df`     |
| Invalid records      | `invalid_<entity>_df`     | `invalid_payment_df`   |
| Deduplicated records | `deduped_<entity>_df`     | `deduped_policy_df`    |
| Aggregated records   | `<entity>_<grain>_agg_df` | `payment_daily_agg_df` |

Acceptable:

```python
policy_df = spark.table("bronze_policy")
```

Better when multiple versions exist in the same function:

```python
policy_bronze_df = spark.table("bronze_policy")
policy_silver_df = standardize_policy_status(policy_bronze_df)
invalid_policy_df = find_invalid_policy_records(policy_silver_df)
```

### 2.7 Boolean Names

Boolean variables and functions should read like true/false statements.

| Pattern           | Example                              |
| ----------------- | ------------------------------------ |
| `is_<state>`      | `is_active`, `is_cancelled`          |
| `has_<thing>`     | `has_payment`, `has_valid_policy_id` |
| `can_<action>`    | `can_be_cancelled`                   |
| `should_<action>` | `should_reprocess_batch`             |
| `needs_<thing>`   | `needs_retry`                        |

Good:

```python
is_valid_status = policy_status in VALID_POLICY_STATUSES
has_successful_payment = payment_status == "PAID"
should_update_watermark = pipeline_status == "SUCCESS"
```

Bad:

```python
flag = True
status_check = True
payment = True
```

### 2.8 Constants

Use `UPPER_SNAKE_CASE`. Constants should usually be declared near the top of the module.

```python
VALID_POLICY_STATUSES = {"ISSUED", "ACTIVE", "EXPIRED", "CANCELLED"}
DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
MAX_RETRY_COUNT = 3
BRONZE_POLICY_TABLE = "bronze_policy"
```

### 2.9 Function Arguments

Use clear `snake_case` names. Use `self` for instance methods and `cls` for class methods.

Good:

```python
def update_watermark(entity_name: str, latest_watermark_value: str) -> None:
    pass


class PolicyTransformer:
    def transform(self, policy_df):
        pass

    @classmethod
    def from_config(cls, config_path: str):
        pass
```

Bad:

```python
def update(wm, x):
    pass


class PolicyTransformer:
    def transform(this, data):
        pass
```

### 2.10 Private and Internal Names

Use one leading underscore for internal helpers that should not be used outside the module/class.

```python
def _normalize_status_text(status_text: str) -> str:
    return status_text.strip().upper()
```

Use double leading underscores only when you truly need Python name mangling inside classes. Do not invent new double-underscore magic names.

### 2.11 Exception Names

Exception names should be classes, so use `PascalCase`. Add `Error` suffix when the exception represents an error.

```python
class InvalidSourceSchemaError(Exception):
    pass

class WatermarkUpdateError(Exception):
    pass
```

### 2.12 Enum Names

Use `PascalCase` for the enum class and `UPPER_SNAKE_CASE` for members.

```python
from enum import Enum


class PolicyStatus(Enum):
    ISSUED = "ISSUED"
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
```

### 2.13 Notebook Names

Fabric notebook names should follow the platform convention:

```text
nb_<layer>_<entity>_<purpose>_<env>
```

Examples:

```text
nb_bronze_policy_ingest_dev
nb_silver_policy_clean_dev
nb_silver_payment_validate_dev
nb_gold_policy_fact_build_dev
nb_audit_pipeline_log_dev
```

### 2.14 Python Script File Names

| Script Type    | Pattern                  | Example                    |
| -------------- | ------------------------ | -------------------------- |
| Ingestion      | `<entity>_ingestion.py`  | `policy_ingestion.py`      |
| Transformation | `<entity>_transform.py`  | `policy_transform.py`      |
| Validation     | `<entity>_validation.py` | `payment_validation.py`    |
| Utility        | `<subject>_utils.py`     | `delta_table_utils.py`     |
| Config         | `<subject>_config.py`    | `pipeline_config.py`       |
| Test           | `test_<module>.py`       | `test_policy_transform.py` |

Avoid generic names:

```text
helpers.py
common.py
utils.py
new.py
final.py
test1.py
```

If a utility file is necessary, make it specific:

```text
delta_table_utils.py
watermark_utils.py
schema_validation_utils.py
```

### 2.15 Test Naming

Test names should describe the expected behavior.

Good:

```python
def test_policy_status_is_active_when_payment_is_paid():
    pass


def test_invalid_payment_status_is_written_to_quarantine():
    pass


def test_watermark_is_updated_after_successful_load():
    pass
```

Bad:

```python
def test_policy():
    pass


def test_1():
    pass


def test_success():
    pass
```

### 2.16 Python Import Alias Naming

Use common aliases only when they are widely understood.

Good:

```python
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
```

Bad:

```python
import pandas as p
from pyspark.sql import functions as funcs
```

### 2.17 Python Naming Checklist

Before committing Python code, check:

- [ ] Module file names use `snake_case.py`.
- [ ] Class names use `PascalCase`.
- [ ] Exception classes end with `Error` when appropriate.
- [ ] Functions and methods use `snake_case` and start with a clear verb.
- [ ] Variables explain business meaning.
- [ ] Booleans start with `is_`, `has_`, `can_`, `should_`, or `needs_`.
- [ ] Constants use `UPPER_SNAKE_CASE`.
- [ ] Internal helpers use one leading underscore only.
- [ ] No unclear names like `data`, `tmp`, `obj`, `val`, `x`, except in tiny local scopes.
- [ ] No new double-underscore names unless Python defines them.

---

## 3. SQL Naming Convention

This project should use simple, readable, SQL-friendly names. The default SQL object style is `lower_snake_case`.

### 3.1 SQL Naming Summary

| SQL Object             | Pattern                            | Example                                             |
| ---------------------- | ---------------------------------- | --------------------------------------------------- |
| Schema                 | `lower_snake_case`                 | `gold`, `audit`, `cfg`, `etl`                       |
| Table                  | `lower_snake_case`                 | `fact_policy`                                  |
| View                   | `vw_<purpose>`                     | `vw_policy_performance`                             |
| Stored procedure       | `usp_<verb>_<object>`              | `usp_load_silver_policy`                            |
| Function               | `fn_<verb>_<object>`               | `fn_calculate_policy_age`                           |
| Table-valued function  | `tvf_<verb>_<object>`              | `tvf_get_policy_payments`                           |
| Trigger                | `trg_<table>_<event>`              | `trg_policy_after_update`                           |
| Primary key constraint | `pk_<table>`                       | `pk_dim_customer`                              |
| Foreign key constraint | `fk_<child_table>__<parent_table>` | `fk_fact_policy_dim_customer`            |
| Unique constraint      | `uq_<table>__<columns>`            | `uq_dim_customer__customer_id`                 |
| Check constraint       | `ck_<table>__<rule>`               | `ck_fact_payment__payment_amount_non_negative` |
| Default constraint     | `df_<table>__<column>`             | `df_audit_pipeline_execution__created_at`           |
| Index                  | `ix_<table>__<columns>`            | `ix_fact_policy__policy_date_key`              |
| Unique index           | `ux_<table>__<columns>`            | `ux_silver_policy__policy_id`                       |
| Temporary table        | `#tmp_<purpose>`                   | `#tmp_policy_dedup`                                 |
| SQL variable           | `@snake_case`                      | `@batch_id`                                         |
| SQL parameter          | `@snake_case`                      | `@entity_name`                                      |

### 3.2 SQL Identifier Rules

Use regular SQL identifiers whenever possible.

Rules:

- Use lowercase letters, digits, and underscores.
- Start names with a letter.
- Avoid names starting with `_`, `@`, or `#` unless the platform gives that symbol special meaning, such as SQL variables or temporary tables.
- Avoid spaces.
- Avoid special characters.
- Avoid reserved words.
- Avoid using brackets or quotes just to make a bad name work.
- Keep names under 128 characters for SQL Server / Fabric SQL compatibility.

Good:

```sql
select policy_id, customer_id, policy_status
from fact_policy;
```

Bad:

```sql
select [Policy ID], [Customer ID], [Status]
from [Gold Fact Policy];
```

### 3.3 Schema Names

If working in a SQL Warehouse or relational database, use schemas to separate responsibility.

| Schema   | Purpose                                              |
| -------- | ---------------------------------------------------- |
| `stg`    | Temporary staging tables used during loading.        |
| `bronze` | Raw or source-aligned tables.                        |
| `silver` | Cleaned and standardized tables.                     |
| `gold`   | Business-ready dimensional model.                    |
| `audit`  | Execution logs, row counts, errors, quality results. |
| `cfg`    | Config, watermark, source metadata, load control.    |
| `ref`    | Reference lists and controlled mapping tables.       |
| `etl`    | Stored procedures/functions used for ETL logic.      |
| `rpt`    | Report-facing views if needed.                       |

### 3.4 Lakehouse vs Warehouse Table Naming

The current Lakehouse design uses layer prefixes because all tables are listed together:

```text
bronze_policy
silver_policy
fact_policy
```

If a future SQL Warehouse uses schemas, prefer schema separation and avoid repeating the layer in the table name:

```sql
bronze.policy
silver.policy
gold.fact_policy
audit.pipeline_execution
cfg.watermark
```

Do not mix both styles in the same SQL database unless there is a clear reason.

Bad future Warehouse example:

```sql
gold.dim_customer_table
silver.silver_policy
```

Good future Warehouse example:

```sql
gold.fact_policy
silver.policy
```

### 3.5 Table Names

Use singular business entity names for entity tables.

Good:

```text
silver_customer
silver_vehicle
silver_provider
silver_policy
```

Avoid plural table names:

```text
silver_customers
silver_vehicles
silver_policies
```

For Gold dimensional modeling, use fact/dimension prefixes:

```text
dim_customer
dim_vehicle
dim_provider
dim_date
fact_quotation
fact_policy
fact_payment
fact_cancellation
```

### 3.6 Column Names

Use `lower_snake_case` for all SQL columns.

| Column Type             | Pattern                      | Example                     |
| ----------------------- | ---------------------------- | --------------------------- |
| Primary key             | `<entity>_id`                | `policy_id`                 |
| Foreign key             | same as referenced key       | `customer_id`               |
| Surrogate dimension key | `<entity>_key`               | `customer_key`              |
| Date key                | `<event>_date_key`           | `policy_start_date_key`     |
| Status                  | `<entity>_status`            | `policy_status`             |
| Timestamp               | `<event>_at`                 | `issued_at`                 |
| Date only               | `<event>_date`               | `policy_start_date`         |
| Amount                  | `<business>_amount`          | `premium_amount`            |
| Count                   | `<business>_count`           | `quotation_count`           |
| Percentage              | `<business>_pct`             | `cancellation_rate_pct`     |
| Boolean                 | `is_<state>` / `has_<thing>` | `is_current`, `has_payment` |
| Code                    | `<business>_code`            | `provider_code`             |
| Description             | `<business>_description`     | `coverage_description`      |
| Name                    | `<business>_name`            | `provider_name`             |

Good:

```sql
policy_id
customer_id
vehicle_id
provider_id
policy_status
premium_amount
policy_start_date
policy_end_date
issued_at
is_current
```

Bad:

```sql
PolicyID
CustID
Vehicle
Status
Amt
StartDate
createdDatetime
flag
```

### 3.7 ID and Key Naming

Use these rules consistently:

| Concept                      | Column Name                       | Meaning                                                   |
| ---------------------------- | --------------------------------- | --------------------------------------------------------- |
| Source business ID           | `<entity>_id`                     | ID from source system or business process.                |
| Data warehouse surrogate key | `<entity>_key`                    | Generated key used in star schema.                        |
| Foreign key to dimension     | `<entity>_key`                    | Foreign key in fact table to dimension table.             |
| Natural/business key         | `<entity>_business_key` if needed | Stable business identifier when different from source ID. |

Example Gold dimension:

```sql
create table dim_customer (
    customer_key bigint,
    customer_id string,
    customer_name string,
    phone_number string,
    email_address string,
    _is_current boolean,
    _effective_from timestamp,
    _effective_to timestamp
);
```

Example Gold fact:

```sql
create table fact_policy (
    policy_id string,
    customer_key bigint,
    vehicle_key bigint,
    provider_key bigint,
    package_key bigint,
    policy_start_date_key int,
    policy_end_date_key int,
    premium_amount decimal(18, 2),
    policy_status string
);
```

### 3.8 Date and Time Column Names

Use consistent suffixes.

| Suffix      | Use For                               | Example                 |
| ----------- | ------------------------------------- | ----------------------- |
| `_date`     | Date without time                     | `policy_start_date`     |
| `_at`       | Timestamp / datetime event            | `created_at`, `paid_at` |
| `_time`     | Time only                             | `payment_time`          |
| `_date_key` | Foreign key to date dimension         | `payment_date_key`      |
| `_year`     | Year number                           | `manufacture_year`      |
| `_month`    | Month number/name depending on column | `payment_month`         |

Avoid unclear names:

```sql
date
time
datetime
created
updated
```

Use:

```sql
created_at
updated_at
policy_start_date
policy_end_date
payment_date_key
```

### 3.9 Money and Numeric Column Names

Use clear units and business meaning.

Good:

```sql
premium_amount
paid_amount
refund_amount
discount_amount
commission_amount
cancellation_fee_amount
coverage_limit_amount
quotation_count
policy_count
payment_count
```

If currency is relevant, add currency code:

```sql
premium_amount
premium_currency_code
```

Avoid:

```sql
amount
amt
value
price
money
```

### 3.10 Status and Mapping Tables

Use `ref_` for official reference values and `map_` for source-to-standard mappings.

| Table                  | Purpose                                              |
| ---------------------- | ---------------------------------------------------- |
| `ref_policy_status`    | Final standard policy statuses.                      |
| `map_policy_status`    | Maps source status values to standard statuses.      |
| `ref_payment_status`   | Final standard payment statuses.                     |
| `map_payment_status`   | Maps source payment statuses to standard statuses.   |
| `ref_quotation_status` | Final standard quotation statuses.                   |
| `map_quotation_status` | Maps source quotation statuses to standard statuses. |

Example columns:

```sql
source_system
source_status_code
source_status_description
standard_status_code
standard_status_description
is_active
created_at
updated_at
```

### 3.11 View Names

Use `vw_<business_purpose>`.

Good:

```sql
vw_policy_performance
vw_agent_quotation_summary
vw_provider_payment_summary
vw_cancellation_trend
```

Bad:

```sql
view1
policy_view
vw_data
```

Views should not hide unclear transformation logic. If a view becomes complex, move logic into Silver/Gold transformation and keep the view simple.

### 3.12 Stored Procedure Names

Use this pattern:

```text
usp_<verb>_<object>
```

Recommended verbs:

| Verb       | Use For                               | Example                      |
| ---------- | ------------------------------------- | ---------------------------- |
| `load`     | Load from one layer/source to another | `usp_load_bronze_policy`     |
| `merge`    | Merge/upsert data                     | `usp_merge_silver_policy`    |
| `build`    | Build derived tables                  | `usp_build_fact_policy` |
| `refresh`  | Refresh report-facing object          | `usp_refresh_policy_summary` |
| `validate` | Run validation rules                  | `usp_validate_payment`       |
| `log`      | Write audit records                   | `usp_log_pipeline_execution` |
| `update`   | Update control metadata               | `usp_update_watermark`       |

Rules:

- Do not use `sp_` for user stored procedures.
- Put ETL stored procedures in `etl` schema when possible.
- Procedure name should describe the final action.
- Avoid vague names like `usp_process_data`.

Good:

```sql
etl.usp_load_bronze_policy
etl.usp_merge_silver_payment
etl.usp_build_fact_policy
etl.usp_update_watermark
```

Bad:

```sql
sp_policy
usp_process
load_data
procedure1
```

### 3.13 SQL Function Names

Use `fn_` for scalar functions and `tvf_` for table-valued functions.

```sql
fn_calculate_policy_duration_days
tvf_get_customer_policies
fn_normalize_status_code
```

Avoid putting heavy business transformation in scalar functions if it hurts performance. Prefer set-based SQL logic or Spark transformations for large data.

### 3.14 Constraint Names

Always name important constraints explicitly. Do not rely on system-generated names.

| Constraint  | Pattern                            | Example                                             |
| ----------- | ---------------------------------- | --------------------------------------------------- |
| Primary key | `pk_<table>`                       | `pk_dim_customer`                              |
| Foreign key | `fk_<child_table>__<parent_table>` | `fk_fact_policy__dim_customer`            |
| Unique      | `uq_<table>__<columns>`            | `uq_dim_customer__customer_id`                 |
| Check       | `ck_<table>__<rule>`               | `ck_fact_payment__payment_amount_non_negative` |
| Default     | `df_<table>__<column>`             | `df_audit_pipeline_execution__created_at`           |

Use double underscore `__` to separate the table name from the target columns or related table.

### 3.15 Index Names

Use index names that show table and column purpose.

| Index Type     | Pattern                                         | Example                                                   |
| -------------- | ----------------------------------------------- | --------------------------------------------------------- |
| Normal index   | `ix_<table>__<column_list>`                     | `ix_fact_policy__customer_key_policy_start_date_key` |
| Unique index   | `ux_<table>__<column_list>`                     | `ux_silver_policy__policy_id`                             |
| Filtered index | `ix_<table>__<column_list>__filter_<condition>` | `ix_policy__policy_status__filter_active`                 |

### 3.16 CTE and Alias Names

Use meaningful CTE names. Avoid single-letter aliases except in tiny queries.

Good:

```sql
with policy_base as (
    select *
    from silver_policy
),
paid_payment as (
    select *
    from silver_payment
    where payment_status = 'PAID'
)
select
    policy_base.policy_id,
    paid_payment.payment_amount
from policy_base
left join paid_payment
    on policy_base.policy_id = paid_payment.policy_id;
```

Acceptable for very small queries:

```sql
select p.policy_id, c.customer_name
from fact_policy p
join dim_customer c
    on p.customer_key = c.customer_key;
```

Bad:

```sql
with a as (...), b as (...)
select * from a join b on a.id = b.id;
```

### 3.17 SQL Script File Names

Use ordered and descriptive file names.

| Script Type       | Pattern                         | Example                                    |
| ----------------- | ------------------------------- | ------------------------------------------ |
| DDL create schema | `001_create_schemas.sql`        | `001_create_schemas.sql`                   |
| DDL create tables | `010_create_<layer>_tables.sql` | `010_create_gold_tables.sql`               |
| Reference data    | `020_seed_<subject>.sql`        | `020_seed_ref_status.sql`                  |
| Stored procedure  | `030_create_usp_<name>.sql`     | `030_create_usp_load_silver_policy.sql`    |
| Migration         | `YYYYMMDD_HHMM__<change>.sql`   | `20260529_1030__add_vehicle_dimension.sql` |
| Test query        | `test_<subject>.sql`            | `test_fact_policy_counts.sql`         |

### 3.18 SQL Naming Checklist

Before committing SQL code, check:

- [ ] All identifiers use lowercase snake_case.
- [ ] No spaces or special characters in object names.
- [ ] No reserved words used as object or column names.
- [ ] Table names are singular.
- [ ] Fact and dimension tables use `fact_` / `dim_` in Lakehouse.
- [ ] Primary keys use `<entity>_id` or `<entity>_key` consistently.
- [ ] Foreign keys use the same name as the referenced key.
- [ ] Date/time suffixes are consistent: `_date`, `_at`, `_date_key`.
- [ ] Boolean columns start with `is_`, `has_`, `can_`, or `should_`.
- [ ] Constraints and indexes have explicit names.
- [ ] Stored procedures do not use `sp_` prefix.
- [ ] Query aliases are meaningful.

---

## 4. Final Team Decision Summary

Use this as the short version for daily development.

| Area                     | Decision                                                  |
| ------------------------ | --------------------------------------------------------- |
| Python module            | `snake_case.py`.                                          |
| Python class             | `PascalCase`.                                             |
| Python function/variable | `snake_case`.                                             |
| Python constant          | `UPPER_SNAKE_CASE`.                                       |
| SQL object               | `lower_snake_case`.                                       |
| SQL table                | singular business name.                                   |
| SQL column               | `lower_snake_case`, meaningful suffixes.                  |
| SQL stored procedure     | `usp_<verb>_<object>`, never `sp_`.                       |
| SQL view                 | `vw_<purpose>`.                                           |
| SQL constraint/index     | explicit names: `pk_`, `fk_`, `uq_`, `ck_`, `df_`, `ix_`. |

---

## 5. Source Basis

This guide is synthesized from:

- Python PEP 8 naming conventions and Python style guidance.
- Clean code principles: meaningful names, consistency, small focused functions, avoiding unclear abbreviations, and avoiding unnecessary comments when names can explain intent.
- SQL practice references covering meaningful aliases, SQL metadata/identifier behavior, database modeling, and stored SQL object usage.
- Microsoft SQL Server / Fabric SQL identifier rules for valid database object names.



