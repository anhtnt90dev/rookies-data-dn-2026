# PySpark & Python Notebook Anti-Patterns: Deep Reference

Detailed explanations and before/after code examples for the most common
anti-patterns in PySpark data engineering notebooks. Load this when you need
deeper guidance or a concrete fix for a specific pattern.

---

## Table of Contents

**PySpark Performance**
1. [Missing broadcast() on Small-Table Joins](#1-missing-broadcast)
2. [Python UDF Instead of Native Spark Functions](#2-python-udf)
3. [collect() on Large Unfiltered DataFrames](#3-collect-large)
4. [Repeated Actions Without Caching](#4-repeated-actions)
5. [repartition() When coalesce() Suffices](#5-repartition-vs-coalesce)
6. [Small Files Problem](#6-small-files)

**Data Correctness**
7. [inferSchema=True in Production Reads](#7-infer-schema)
8. [Writing Without Explicit mode=](#8-write-mode)
9. [Column Collisions After Joins](#9-column-collision)
10. [NULL Filtering Gotcha (col != value)](#10-null-filter)

**Data Engineering Practices**
11. [No Row Count Validation Between Stages](#11-row-count)
12. [Hardcoded Paths and Table Names](#12-hardcoded-paths)
13. [Hardcoded Credentials](#13-credentials)
14. [Non-Idempotent Writes](#14-idempotent)
15. [Missing Watermark / Lineage Columns](#15-watermark)

**Notebook Structure**
16. [DataFrames Named df, df2, df3](#16-naming)
17. [SparkSession Recreated Mid-Notebook](#17-sparksession)
18. [Scattered Imports and Configs](#18-scattered-config)

---

## 1. Missing broadcast() on Small-Table Joins

**Problem**: Spark's default join strategy shuffles both sides across the
network. When one side is small (typically < 100MB), broadcasting it to all
executors avoids the shuffle entirely — often 10–100× faster.

```python
# ❌ SLOW — both sides shuffled (sort-merge join)
result = orders.join(country_codes, on="country_id", how="left")

# ✅ FAST — small table broadcast to all executors
from pyspark.sql import functions as F
result = orders.join(F.broadcast(country_codes), on="country_id", how="left")
```

**When to apply**: One side of the join is a lookup/dimension table that fits
comfortably in executor memory. Check with `spark.conf.get("spark.sql.autoBroadcastJoinThreshold")`
(default 10MB). Raise the threshold or use explicit hints for tables up to ~200MB.

```python
# Check size heuristic
print(f"country_codes row count: {country_codes.count()}")
# If < ~1M rows and narrow schema → good broadcast candidate
```

---

## 2. Python UDF Instead of Native Spark Functions

**Problem**: Python UDFs serialize each row from the JVM to the Python process,
process it, and serialize back. This bypasses the Catalyst optimizer and
Tungsten execution engine, making it 10–100× slower than equivalent native functions.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ❌ SLOW — Python UDF, row-by-row Python serialization
@udf(returnType=StringType())
def clean_phone(phone):
    return phone.strip().replace("-", "").replace(" ", "") if phone else None

df = df.withColumn("phone_clean", clean_phone(F.col("phone")))

# ✅ FAST — native Spark SQL functions, stays in JVM
df = df.withColumn(
    "phone_clean",
    F.regexp_replace(F.trim(F.col("phone")), r"[-\s]", "")
)
```

When Python logic is genuinely too complex for SQL functions, prefer **Pandas UDFs**:

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# ✅ BETTER — Pandas UDF uses Apache Arrow, vectorized transfer
@pandas_udf(StringType())
def clean_phone_vectorized(phones: pd.Series) -> pd.Series:
    return phones.str.strip().str.replace(r"[-\s]", "", regex=True)

df = df.withColumn("phone_clean", clean_phone_vectorized(F.col("phone")))
```

---

## 3. collect() on Large Unfiltered DataFrames

**Problem**: `.collect()` brings all rows to the driver node. On a large dataset
this causes driver OOM and defeats the purpose of distributed processing.

```python
# ❌ DANGEROUS — millions of rows → driver OOM
all_rows = df.collect()
for row in all_rows:
    process(row)

# ✅ CORRECT patterns:

# Pattern A — sample before collecting (exploration)
sample_rows = df.limit(1000).collect()

# Pattern B — aggregate first, then collect (metrics)
metrics = df.groupBy("status").count().collect()

# Pattern C — write to storage instead of collecting (ETL output)
df.write.parquet("s3://bucket/output/")

# Pattern D — toPandas() on small aggregated result
summary_pd = df.groupBy("region").agg(F.sum("revenue")).toPandas()
```

---

## 4. Repeated Actions Without Caching

**Problem**: Spark is lazy — each action (count, show, write) replays the full
transformation lineage from the source. If you use a DataFrame in two places,
Spark reads and transforms the source data twice.

```python
# ❌ SLOW — reads and transforms source data TWICE
cleaned = (
    raw.filter(F.col("status") == "active")
       .withColumn("amount_usd", F.col("amount") / 100)
)
print(f"Row count: {cleaned.count()}")   # Job 1: full pipeline
cleaned.write.parquet("s3://output/")    # Job 2: full pipeline again

# ✅ FAST — compute once, reuse
cleaned.cache()
print(f"Row count: {cleaned.count()}")   # Job 1: compute + cache
cleaned.write.parquet("s3://output/")    # Job 2: reads from cache
cleaned.unpersist()                       # Free memory when done
```

**When to cache**: DataFrame is used in 2+ actions, or used in both a write
and a validation check. Don't cache DataFrames used only once.

**Storage levels**:
```python
from pyspark import StorageLevel

# Default — cache in memory (spills if too large)
df.cache()

# Explicit — memory + disk spill, serialized (safer for large DFs)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

---

## 5. repartition() When coalesce() Suffices

**Problem**: `repartition(n)` always triggers a full shuffle across the network
to evenly distribute data. `coalesce(n)` combines existing partitions on the
same executor when *reducing* partition count — no shuffle needed.

```python
# ❌ UNNECESSARY SHUFFLE — repartitioning to fewer partitions
df.repartition(10).write.parquet("s3://output/")

# ✅ NO SHUFFLE — coalesce merges partitions locally
df.coalesce(10).write.parquet("s3://output/")
```

**When to use repartition vs coalesce**:

| Use case | Recommended |
|----------|------------|
| Reducing partition count | `coalesce(n)` |
| Increasing partition count | `repartition(n)` |
| Repartitioning by column(s) for join alignment | `repartition(n, col)` |
| Fixing severe data skew | `repartition(n)` (full shuffle to rebalance) |

---

## 6. Small Files Problem

**Problem**: Writing a DataFrame with hundreds of partitions creates hundreds
of small files. Object stores (S3, GCS, ADLS) have per-file metadata overhead —
thousands of tiny files make downstream reads very slow.

```python
# ❌ BAD — 200 tiny files written to S3
df.write.parquet("s3://bucket/output/")  # default partition count = spark.default.parallelism

# ✅ GOOD — coalesce to a reasonable file count
target_size_mb = 128
estimated_mb = df.count() * avg_row_bytes / 1e6  # rough estimate
n_files = max(1, int(estimated_mb / target_size_mb))
df.coalesce(n_files).write.parquet("s3://bucket/output/")
```

For Delta Lake, enable auto-optimization:
```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

**Rule of thumb**: Aim for output files of 128MB–1GB each. For daily partitions
on moderate data, 1–10 files per partition is usually fine.

---

## 7. inferSchema=True in Production Reads

**Problem**: Schema inference reads the entire file (or a sample) to determine
types — slow and fragile. On messy CSVs, a single dirty row can cause an entire
column to be inferred as StringType. Schemas change silently as source data evolves.

```python
# ❌ FRAGILE — inferred schema, silent type changes
df = spark.read.option("inferSchema", "true").csv("s3://raw/orders/")

# ✅ ROBUST — explicit schema, fail fast on type mismatch
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

orders_schema = StructType([
    StructField("order_id",    LongType(),      nullable=False),
    StructField("customer_id", LongType(),      nullable=True),
    StructField("amount",      DoubleType(),    nullable=True),
    StructField("status",      StringType(),    nullable=True),
    StructField("created_at",  TimestampType(), nullable=True),
])

df = spark.read.schema(orders_schema).csv(
    "s3://raw/orders/",
    header=True,
    mode="PERMISSIVE",          # keep malformed rows
    columnNameOfCorruptRecord="_corrupt_record"
)

# Inspect bad rows
bad_rows = df.filter(F.col("_corrupt_record").isNotNull())
print(f"Malformed rows: {bad_rows.count()}")
```

---

## 8. Writing Without Explicit mode=

**Problem**: The default write mode is `ErrorIfExists` — the job fails if the
output path already exists. In pipeline reruns (which happen constantly), this
causes silent failures or requires manual cleanup.

```python
# ❌ FRAGILE — fails on rerun if path exists
df.write.parquet("s3://bucket/output/orders/")

# ✅ EXPLICIT — choose the intended behavior
# Overwrite: full replace, idempotent
df.write.mode("overwrite").parquet("s3://bucket/output/orders/")

# Append: add rows, use only for truly append-only data
df.write.mode("append").parquet("s3://bucket/output/orders/")

# For partitioned writes — overwrite only the affected partition
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/output/orders/")
```

---

## 9. Column Collisions After Joins

**Problem**: When two DataFrames share a column name, joining them creates
duplicate columns that cause `AnalysisException` when referenced later.

```python
# ❌ BUG — both sides have 'created_at' and 'status'
result = orders.join(customers, orders.customer_id == customers.customer_id)
result.select("status")  # AnalysisException: ambiguous column!

# ✅ FIX option A — rename before joining
customers_renamed = customers.select(
    F.col("customer_id"),
    F.col("name").alias("customer_name"),
    F.col("status").alias("customer_status")
)
result = orders.join(customers_renamed, on="customer_id", how="left")

# ✅ FIX option B — use string key (auto-deduplicates join key only)
result = orders.join(customers, on="customer_id", how="left")
# Note: non-key duplicate columns still collide!

# ✅ FIX option C — drop duplicates after join
result = orders.join(customers, on=["customer_id"], how="left")
result = result.drop(customers["created_at"])  # drop one side explicitly
```

---

## 10. NULL Filtering Gotcha

**Problem**: In Spark SQL (and SQL generally), `NULL != value` evaluates to
`NULL`, not `True`. A filter `col != "cancelled"` silently **excludes** rows
where `col` is NULL.

```python
# ❌ SILENT BUG — excludes NULLs from result
active = df.filter(F.col("status") != "cancelled")
# Rows where status IS NULL are also dropped!

# ✅ CORRECT — explicitly handle NULLs
active = df.filter(
    (F.col("status") != "cancelled") | F.col("status").isNull()
)

# ✅ ALTERNATIVE — use isin with null handling
active = df.filter(~F.col("status").isin(["cancelled"]) | F.col("status").isNull())
```

---

## 11. No Row Count Validation Between Stages

**Problem**: Pipelines that silently drop rows are hard to debug. A missing
join condition or wrong filter can reduce 10M rows to 100K with no warning.

```python
# ❌ NO VISIBILITY — don't know if rows were lost
cleaned = raw.filter(F.col("status").isNotNull())
enriched = cleaned.join(lookup, on="product_id", how="inner")
enriched.write.parquet("s3://output/")

# ✅ VALIDATED — checkpoint counts at each stage
raw_count = raw.count()
print(f"[1] Raw rows:     {raw_count:,}")

cleaned = raw.filter(F.col("status").isNotNull())
cleaned_count = cleaned.count()
print(f"[2] After filter: {cleaned_count:,} ({cleaned_count/raw_count:.1%} retained)")

enriched = cleaned.join(lookup, on="product_id", how="inner")
enriched_count = enriched.count()
print(f"[3] After join:   {enriched_count:,} ({enriched_count/cleaned_count:.1%} retained)")

# Optional: assert minimum retention rate
assert enriched_count >= raw_count * 0.95, \
    f"Row count dropped below 95%: {enriched_count} / {raw_count}"

enriched.write.mode("overwrite").parquet("s3://output/")
print(f"[4] Written: {enriched_count:,} rows")
```

---

## 12. Hardcoded Paths and Table Names

**Problem**: Hardcoded paths make notebooks impossible to reuse across environments
(dev/staging/prod) and break when folder structures change.

```python
# ❌ RIGID — hardcoded everywhere
df = spark.read.parquet("/mnt/raw/orders/2024-01-15/")
df.write.parquet("/mnt/processed/orders/2024-01-15/")

# ✅ PARAMETERIZED — extract config to top cell
# ── Configuration cell (Cell 1) ──────────────────────
ENV         = "prod"                          # or use dbutils.widgets / argparse
BASE_PATH   = f"s3://my-datalake-{ENV}"
SOURCE_DATE = "2024-01-15"                    # or datetime.today().strftime("%Y-%m-%d")

RAW_PATH    = f"{BASE_PATH}/raw/orders/{SOURCE_DATE}/"
OUTPUT_PATH = f"{BASE_PATH}/processed/orders/{SOURCE_DATE}/"
TABLE_NAME  = f"catalog.{ENV}.orders"

# ── Usage cells ──────────────────────────────────────
df = spark.read.parquet(RAW_PATH)
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

For Databricks, use widgets:
```python
dbutils.widgets.text("source_date", "2024-01-15", "Source Date")
SOURCE_DATE = dbutils.widgets.get("source_date")
```

---

## 13. Hardcoded Credentials

**Problem**: Database passwords, storage keys, API tokens in notebook cells
are exposed to anyone with notebook access and captured in version control history.

```python
# ❌ DANGEROUS
jdbc_url = "jdbc:postgresql://prod-db:5432/orders"
df = spark.read.jdbc(url=jdbc_url, table="orders",
                     properties={"user": "admin", "password": "s3cr3t!"})

# ✅ DATABRICKS SECRETS
jdbc_password = dbutils.secrets.get(scope="prod-db", key="jdbc-password")
df = spark.read.jdbc(
    url=jdbc_url,
    table="orders",
    properties={"user": "svc_spark", "password": jdbc_password}
)

# ✅ ENVIRONMENT VARIABLES (non-Databricks)
import os
jdbc_password = os.environ["JDBC_PASSWORD"]

# ✅ AWS SECRETS MANAGER pattern
import boto3, json
secret = json.loads(
    boto3.client("secretsmanager").get_secret_value(SecretId="prod/db/orders")["SecretString"]
)
jdbc_password = secret["password"]
```

---

## 14. Non-Idempotent Writes

**Problem**: If a pipeline job fails halfway and reruns, non-idempotent writes
will duplicate data. This is one of the most common data quality issues in
production pipelines.

```python
# ❌ NON-IDEMPOTENT — reruns append duplicate rows
df.write.mode("append").parquet("s3://output/orders/date=2024-01-15/")

# ✅ IDEMPOTENT option A — overwrite the full partition
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet("s3://output/orders/")

# ✅ IDEMPOTENT option B — Delta Lake MERGE (upsert)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://output/orders/")
delta_table.alias("target").merge(
    df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## 15. Missing Watermark / Lineage Columns

**Problem**: Without metadata columns, it's impossible to debug data issues,
trace a row back to its source file, or understand when data was loaded.

```python
# ❌ NO LINEAGE — row appears from nowhere
df_clean = raw.filter(F.col("status").isNotNull())
df_clean.write.parquet("s3://output/orders/")

# ✅ WITH LINEAGE COLUMNS
from pyspark.sql import functions as F
from datetime import datetime

df_clean = (
    raw
    .filter(F.col("status").isNotNull())
    .withColumn("_ingested_at",   F.current_timestamp())
    .withColumn("_source_file",   F.input_file_name())
    .withColumn("_pipeline_run",  F.lit(RUN_ID))        # set RUN_ID at top of notebook
    .withColumn("_batch_date",    F.lit(SOURCE_DATE))
)

df_clean.write.mode("overwrite").parquet("s3://output/orders/")
```

---

## 16. DataFrames Named df, df2, df3

**Problem**: Generic names make notebooks impossible to read after more than
a few cells, and make debugging joins/filters very hard.

```python
# ❌ UNREADABLE
df = spark.read.parquet("s3://raw/orders/")
df2 = df.filter(F.col("status") == "active")
df3 = df2.join(df4, on="customer_id")

# ✅ SELF-DOCUMENTING
orders_raw       = spark.read.parquet("s3://raw/orders/")
orders_active    = orders_raw.filter(F.col("status") == "active")
orders_enriched  = orders_active.join(F.broadcast(customers), on="customer_id", how="left")
orders_final     = orders_enriched.select("order_id", "customer_name", "amount", "date")
```

Convention: `{entity}_{stage}` — e.g., `events_raw`, `events_parsed`,
`events_deduped`, `events_enriched`, `events_final`.

---

## 17. SparkSession Recreated Mid-Notebook

**Problem**: Creating a new `SparkSession` mid-notebook with `getOrCreate()`
is fine, but explicitly stopping and restarting Spark (`spark.stop()`) tears
down the entire Spark context, losing cached data and potentially killing
running jobs.

```python
# ❌ BAD — stops Spark context, breaks subsequent cells
spark.stop()
spark = SparkSession.builder.appName("new_job").getOrCreate()

# ✅ CORRECT — create once at the top, reuse everywhere
# Cell 1
spark = (
    SparkSession.builder
    .appName("orders-ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")       # enable AQE
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # reduce noise
```

---

## 18. Scattered Imports and Configs

**Problem**: Imports and config values scattered across cells create hidden
dependencies and make the notebook impossible to rerun from a clean state.

```python
# ❌ SCATTERED — configs and imports buried in cells 1, 5, 12, 18...

# Cell 5 (buried)
from pyspark.sql import functions as F    # ❌ hidden import

# Cell 12 (buried)
OUTPUT_PATH = "/mnt/output/orders/"       # ❌ hidden constant

# ✅ CONSOLIDATED — everything visible at the top

# ── Cell 1: Imports ──────────────────────────────────
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, BooleanType
)

# ── Cell 2: Configuration ────────────────────────────
ENV           = os.getenv("ENV", "dev")
RUN_ID        = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
SOURCE_DATE   = os.getenv("SOURCE_DATE", datetime.today().strftime("%Y-%m-%d"))
BASE_PATH     = f"s3://my-datalake-{ENV}"
RAW_PATH      = f"{BASE_PATH}/raw/orders/date={SOURCE_DATE}/"
OUTPUT_PATH   = f"{BASE_PATH}/processed/orders/"
CATALOG_TABLE = f"catalog.{ENV}.orders_processed"

print(f"Run ID:      {RUN_ID}")
print(f"Source date: {SOURCE_DATE}")
print(f"Output:      {OUTPUT_PATH}")