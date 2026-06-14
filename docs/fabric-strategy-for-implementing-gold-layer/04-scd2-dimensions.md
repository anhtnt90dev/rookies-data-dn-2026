# 04 - SCD Type 2 Dimensions Ingestion Strategy (`nb_gold_load_scd2_dimensions_dev`)

This document defines the ingestion strategy and historical versioning rules for Slowly Changing Dimension (SCD) Type 2 tables in the Gold Layer. Context for columns and rules is aligned with the project specification in [silver-to-gold-mapping.md](../source-to-target-mapping/silver-to-gold-mapping.md) and the JSON files in [silver-to-gold](../source-to-target-mapping/jsons/silver-to-gold).

---

## 1. Objectives

*   Orchestrate SCD Type 2 dimensions (`dim_customer`, `dim_agent`, `dim_provider`, `dim_vehicle`).
*   Track historical changes in critical fields using surrogate keys, version start/end timestamps, and active flags.
*   Seed each dimension with an Unknown Member (`-1`) row.

---

## 2. Targeted SCD Type 2 Tables & Columns

Below is the exact column list and change-tracking columns mapped for each SCD Type 2 table:

| Target Table | Source Silver Table | Business Key | Tracked Columns (SCD2 Hash Columns) | Other Target Columns |
| :--- | :--- | :--- | :--- | :--- |
| `dim_customer` | `silver.customer` | `customer_id` | `full_name`, `gender`, `dob`, `phone_number`, `email`, `city`, `district` | `customer_key` (PK), `effective_from`, `effective_to`, `is_current`, `created_at`, `updated_at` |
| `dim_agent` | `silver.agent` | `agent_id` | `agent_name`, `region`, `branch`, `manager_name` | `agent_key` (PK), `effective_from`, `effective_to`, `is_current`, `created_at`, `updated_at` |
| `dim_provider` | `silver.provider` | `provider_code` | `provider_name`, `provider_group`, `active_flag` | `provider_key` (PK), `effective_from`, `effective_to`, `is_current`, `created_at`, `updated_at` |
| `dim_vehicle` | `silver.vehicle` | `vehicle_id` | `customer_id`, `plate_number`, `vehicle_brand`, `vehicle_model`, `manufacture_year`, `vehicle_value` | `vehicle_key` (PK), `effective_from`, `effective_to`, `is_current`, `created_at`, `updated_at` |

> [!NOTE]
> * `active_flag` (INT) in `dim_provider` is calculated by casting the Silver `is_active` (BOOLEAN) to `INT` (as specified in `dim_provider.json`).
> * `effective_from` (TIMESTAMP) is populated by `COALESCE(updated_at, created_at)` from the Silver row.
> * `effective_to` (TIMESTAMP) defaults to `9999-12-31 23:59:59` for the current active version of a row.
> * `is_current` (BOOLEAN) is set to `true` for the active version, and `false` for retired versions.

---

## 3. Ingestion Logic Flow (SCD2 Merge)

SCD Type 2 processing requires handling two primary conditions:
1.  **New Business Key**: Insert the key with `is_current = true`, `effective_from = COALESCE(source.updated_at, source.created_at)`, `effective_to = '9999-12-31 23:59:59'`.
2.  **SCD2 Tracked Change**: Expire the currently active target row (set `is_current = false`, `effective_to = COALESCE(source.updated_at, source.created_at)`) and insert a new row with updated attributes, `is_current = true`, and `effective_from = COALESCE(source.updated_at, source.created_at)`.

```mermaid
graph TD
    Start([Start SCD2 Ingestion]) --> CheckUnknown{Unknown Row -1 exists?}
    CheckUnknown -- No --> InsertUnknown[Insert Unknown Row -1]
    CheckUnknown -- Yes --> ReadSource[Read Clean Silver Tables]
    InsertUnknown --> ReadSource
    
    ReadSource --> MatchKey{Business Key Match in Target?}
    
    MatchKey -->|No Match / New Key| InsertNew[Insert New Version: set is_current=true, effective_from=COALESCE_updated_at_created_at]
    MatchKey -->|Match Found| HashDiff{Compare MD5 Hash of Tracked Columns}
    
    HashDiff -->|Tracked Cols Changed| ExpireOld[Expire Existing Version: set is_current=false, effective_to=COALESCE_updated_at_created_at]
    HashDiff -->|No Changes| SkipIngest[Bypass Ingestion / Do Nothing]
    
    ExpireOld --> InsertNew
    
    InsertNew & SkipIngest --> End([SCD2 Completed])
```

---

## 4. PySpark Ingestion Design Specification

To implement this logic efficiently in Spark, we calculate the MD5 hash of the tracked columns on-the-fly in PySpark dataframes rather than storing it in the Delta tables. The load is completed in two main phases:

### Phase 1: Deduplication & Hash Generation (On-the-Fly)
- Incoming source data is deduplicated by business key, keeping only the latest record based on `event_time` (using a window function partition).
- An MD5 hash of the tracked attributes is generated on-the-fly for the incoming source records.
- A similar hash is calculated for the active target records in the Delta table (`is_current = true`).

### Phase 2: Identify Versioning Actions & Execute
The incoming and target dataframes are joined on the business key:
1.  **Records to Expire**: Target records whose business key matches an incoming record, but whose tracked attributes' hash does not match (`src.row_hash != tgt.row_hash`).
2.  **Records to Insert**:
    - **New Keys**: Genuinely new business keys that do not exist in the target.
    - **New Versions**: The updated version of records that were expired in Step 1.

### Implementation PySpark Code Example (`dim_customer`)

```python
# 1. Generate row_hash on-the-fly for incoming source records
incoming_with_hash = source_df.withColumn(
    "row_hash",
    F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in customer_cols]))
)

# 2. Get active target records and calculate row_hash on-the-fly
target_active = spark.table("gold.dim_customer") \
    .where((F.col("is_current") == True) & (F.col("customer_key") != -1)) \
    .withColumn(
        "row_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in customer_cols]))
    )

# 3. Join on customer_id to identify changes
joined = incoming_with_hash.alias("src").join(
    target_active.alias("tgt"),
    on=F.col("src.customer_id") == F.col("tgt.customer_id"),
    how="left"
)

records_to_expire = joined.filter(F.col("tgt.customer_key").isNotNull() & (F.col("src.row_hash") != F.col("tgt.row_hash")))
new_records = joined.filter(F.col("tgt.customer_key").isNull())
new_versions = records_to_expire

# Step A: Expire old active records via Delta Table Merge Update
if records_to_expire.count() > 0:
    expire_df = records_to_expire.select(
        F.col("src.customer_id").alias("customer_id"),
        F.col("src.event_time").alias("expire_time")
    )
    delta_table = DeltaTable.forName(spark, "gold.dim_customer")
    delta_table.alias("target").merge(
        expire_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    ).whenMatchedUpdate(
        set={
            "is_current": "false",
            "effective_to": "source.expire_time",
            "updated_at": "current_timestamp()"
        }
    ).execute()

# Step B: Insert new versions and new business keys with generated keys
insert_source_df = new_records.select(
    F.col("src.customer_id").alias("customer_id"),
    *[F.col("src." + c).alias(c) for c in customer_cols],
    F.col("src.event_time").alias("effective_from")
).union(
    new_versions.select(
        F.col("src.customer_id").alias("customer_id"),
        *[F.col("src." + c).alias(c) for c in customer_cols],
        F.col("src.event_time").alias("effective_from")
    )
)

if insert_source_df.count() > 0:
    # Retrieve current maximum surrogate key to calculate next keys
    max_key = spark.table("gold.dim_customer").where(F.col("customer_key") != -1).agg(F.max("customer_key")).collect()[0][0]
    max_key = int(max_key) if max_key is not None else 0

    window_insert = Window.orderBy("customer_id")
    insert_final_df = insert_source_df.withColumn(
        "customer_key",
        F.lit(max_key) + F.row_number().over(window_insert).cast("bigint")
    ).withColumn("effective_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))) \
     .withColumn("is_current", F.lit(True)) \
     .withColumn("created_at", F.current_timestamp()) \
     .withColumn("updated_at", F.current_timestamp())

    # Append new records
    insert_final_df.select(
        "customer_key", "customer_id", *customer_cols,
        "effective_from", "effective_to", "is_current",
        "created_at", "updated_at"
    ).write.format("delta").mode("append").saveAsTable("gold.dim_customer")
```

---

## 5. Idempotency & Rerun Behavior (No-Change Scenario)

The PySpark-based SCD Type 2 logic guarantees strict idempotency on pipeline recovery or batch reruns:
*   **No Changes**: If the business key already has an active record in Gold (`is_current = true`) and the incoming fields match the existing record (matching on MD5 hash):
    *   `records_to_expire` will yield a count of 0. No merge statement will be executed.
    *   `new_records` will find that `tgt.customer_key` is not null, yielding 0 rows.
    *   Result: 0 updates and 0 appends occur. The system state remains unchanged.
*   **Rerun of Changed Batch**: If the batch is rerun, the join resolves the active state. Since target records are already updated and expired in the initial run, the new run resolves them as already matching, causing no duplicate version records.

---

## 6. Unknown Member Specification (`dim_customer` example)

The Unknown row must be present with the following attributes:
*   `customer_key` = `-1`
*   `customer_id` = `'Unknown'`
*   `full_name` = `'Unknown'`
*   `gender` = `'Unknown'`
*   `dob` = `NULL`
*   `phone_number` = `'Unknown'`
*   `email` = `'Unknown'`
*   `city` = `'Unknown'`
*   `district` = `'Unknown'`
*   `is_current` = `true`
*   `effective_from` = `'1900-01-01 00:00:00'`
*   `effective_to` = `'9999-12-31 23:59:59'`
