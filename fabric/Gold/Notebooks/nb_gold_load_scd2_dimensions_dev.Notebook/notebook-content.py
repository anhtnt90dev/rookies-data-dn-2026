# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "126c09a8-79bf-4e16-9e56-5e7c93311e29",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "6358469d-5cd2-48a3-8d0f-c9583b40d1fa",
# META       "known_lakehouses": [
# META         {
# META           "id": "126c09a8-79bf-4e16-9e56-5e7c93311e29"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

session_id = ""
batch_id = 0
run_mode = "NEW"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

p_session_id = "9693abc1-154f-4a40-a9a6-4f73860c656a"
p_batch_id = "20260613113233"
p_run_mode = "NEW"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, BooleanType, TimestampType, LongType
from delta.tables import DeltaTable

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()

# Ensure Unknown Row (-1) helper for SCD2
def ensure_unknown_row_scd2(table_name: str, pk_col: str, bk_col: str, tracked_cols: list[str]):
    exists = spark.table(table_name).where(F.col(pk_col) == -1).count() > 0
    if not exists:
        schema = spark.table(table_name).schema
        row_dict = {}
        for field in schema.fields:
            col_name = field.name
            if col_name == pk_col:
                row_dict[col_name] = -1
            elif col_name == bk_col:
                row_dict[col_name] = "Unknown"
            elif col_name == "is_current":
                row_dict[col_name] = True
            elif col_name == "effective_from":
                row_dict[col_name] = datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            elif col_name == "effective_to":
                row_dict[col_name] = datetime.strptime("9999-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
            elif col_name in ("created_at", "updated_at"):
                row_dict[col_name] = datetime.now()
            elif col_name in tracked_cols:
                if col_name == "active_flag":
                    row_dict[col_name] = -1
                elif str(field.dataType).startswith("StringType"):
                    row_dict[col_name] = "Unknown"
                else:
                    row_dict[col_name] = None
            else:
                row_dict[col_name] = None
        
        unknown_df = spark.createDataFrame([row_dict], schema)
        unknown_df.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"[INFO] Seeded Unknown row (-1) into SCD2 table {table_name}")

# SCD Type 2 Loader function
def load_scd2_dimension(
    target_table_name: str,
    source_df: DataFrame,
    business_key_col: str,
    surrogate_key_col: str,
    tracked_cols: list[str],
    dim_fact_table_id: int
):
    # Check if table should process in this run
    if not should_process_table_layer(batch_id, dim_fact_table_id, "GOLD"):
        print(f"[BYPASS] Table {target_table_name} was already successfully processed in this batch. Skipping.")
        return

    table_session_id = start_table_layer(
        session_id=session_id,
        source_table_id=dim_fact_table_id,
        source_table_name=target_table_name.split(".")[-1],
        layer="GOLD",
        batch_id=batch_id,
        load_type="INCREMENTAL"
    )

    try:
        # 1. Ensure Unknown row (-1)
        ensure_unknown_row_scd2(target_table_name, surrogate_key_col, business_key_col, tracked_cols)

        # 2. Add event_time and row_hash columns to incoming source data
        # Deduplicate to keep only the latest version of each business key in the incoming batch
        window_source = Window.partitionBy(business_key_col).orderBy(F.col("event_time").desc())
        
        incoming_with_hash = source_df.withColumn(
            "row_hash",
            F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]))
        ).withColumn("rn", F.row_number().over(window_source))\
         .filter(F.col("rn") == 1)\
         .drop("rn")

        # 3. Read active target records and calculate row_hash on-the-fly
        target_active = spark.table(target_table_name) \
            .where((F.col("is_current") == True) & (F.col(surrogate_key_col) != -1)) \
            .withColumn(
                "row_hash",
                F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]))
            )

        # 4. Join incoming and target active records on business_key
        joined = incoming_with_hash.alias("src").join(
            target_active.alias("tgt"),
            on=F.col("src." + business_key_col) == F.col("tgt." + business_key_col),
            how="left"
        )

        # Identify updates (records that exist in target but have different hash)
        records_to_expire = joined.filter(F.col("tgt." + surrogate_key_col).isNotNull() & (F.col("src.row_hash") != F.col("tgt.row_hash")))
        
        # Identify inserts:
        # A. Genuinely new business keys (not in target)
        new_records = joined.filter(F.col("tgt." + surrogate_key_col).isNull())
        # B. Changed business keys (new active version of existing records)
        new_versions = records_to_expire

        expire_count = records_to_expire.count()
        new_key_count = new_records.count()
        total_inserted = 0

        if expire_count > 0 or new_key_count > 0:
            # Step A: Expire old active records (Delta Merge Update)
            if expire_count > 0:
                expire_df = records_to_expire.select(
                    F.col("src." + business_key_col).alias(business_key_col),
                    F.col("src.event_time").alias("expire_time")
                )
                delta_table = DeltaTable.forName(spark, target_table_name)
                delta_table.alias("target").merge(
                    expire_df.alias("source"),
                    f"target.{business_key_col} = source.{business_key_col} AND target.is_current = true"
                ).whenMatchedUpdate(
                    set={
                        "is_current": "false",
                        "effective_to": "source.expire_time",
                        "updated_at": "current_timestamp()"
                    }
                ).execute()

            # Step B: Insert new versions and new business keys (omitting row_hash)
            insert_source_df = new_records.select(
                F.col("src." + business_key_col).alias(business_key_col),
                *[F.col("src." + c).alias(c) for c in tracked_cols],
                F.col("src.event_time").alias("effective_from")
            ).union(
                new_versions.select(
                    F.col("src." + business_key_col).alias(business_key_col),
                    *[F.col("src." + c).alias(c) for c in tracked_cols],
                    F.col("src.event_time").alias("effective_from")
                )
            )

            # Generate surrogate keys dynamically based on max key + row_number
            max_key = spark.table(target_table_name).where(F.col(surrogate_key_col) != -1).agg(F.max(surrogate_key_col)).collect()[0][0]
            max_key = int(max_key) if max_key is not None else 0

            window_insert = Window.orderBy(business_key_col)
            insert_final_df = insert_source_df.withColumn(
                surrogate_key_col,
                F.lit(max_key) + F.row_number().over(window_insert).cast("bigint")
            ).withColumn("effective_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))) \
             .withColumn("is_current", F.lit(True)) \
             .withColumn("created_at", F.current_timestamp()) \
             .withColumn("updated_at", F.current_timestamp())

            total_inserted = insert_final_df.count()

            # Append new records
            insert_final_df.select(
                surrogate_key_col, business_key_col, *tracked_cols,
                "effective_from", "effective_to", "is_current",
                "created_at", "updated_at"
            ).write.format("delta").mode("append").saveAsTable(target_table_name)

        # Get final counts
        total_count = spark.table(target_table_name).count()

        finish_table_layer(
            table_session_id=table_session_id,
            layer="GOLD",
            status="SUCCESS",
            is_final_table_step=True,
            source_row_count=source_df.count(),
            target_row_count=total_count,
            inserted_row=total_inserted,
            updated_row=expire_count,
            deleted_row=0,
            rejected_row=0
        )
        print(f"[INFO] Completed SCD2 load for {target_table_name}. Expired: {expire_count}, Inserted: {total_inserted}")

    except Exception as err:
        print(f"[ERROR] Failed to load SCD2 {target_table_name}: {err}")
        finish_table_layer(
            table_session_id=table_session_id,
            layer="GOLD",
            status="FAILED",
            error_code="SCD2_LOAD_FAILED",
            error_message=str(err)[:1000]
        )
        raise err

# ---------------------------------------------------------------------------
# Dimension Loads Sequenced Execution
# ---------------------------------------------------------------------------

# 1. dim_customer (Source: silver.customer, ID: 2)
print("[LOAD] Processing dim_customer...")
customer_src_df = spark.table("silver.customer").select(
    "customer_id",
    "full_name",
    "gender",
    "dob",
    "phone_number",
    "email",
    "city",
    "district",
    F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time")
)
customer_cols = ["full_name", "gender", "dob", "phone_number", "email", "city", "district"]
load_scd2_dimension("gold.dim_customer", customer_src_df, "customer_id", "customer_key", customer_cols, 2)

# 2. dim_agent (Source: silver.agent, ID: 3)
print("[LOAD] Processing dim_agent...")
agent_src_df = spark.table("silver.agent").select(
    "agent_id",
    "agent_name",
    "region",
    "branch",
    "manager_name",
    F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time")
)
agent_cols = ["agent_name", "region", "branch", "manager_name"]
load_scd2_dimension("gold.dim_agent", agent_src_df, "agent_id", "agent_key", agent_cols, 3)

# 3. dim_provider (Source: silver.provider, ID: 4)
print("[LOAD] Processing dim_provider...")
provider_src_df = spark.table("silver.provider").select(
    "provider_code",
    "provider_name",
    "provider_group",
    F.coalesce(F.col("is_active").cast("integer"), F.lit(1)).alias("active_flag"),
    F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time")
)
provider_cols = ["provider_name", "provider_group", "active_flag"]
load_scd2_dimension("gold.dim_provider", provider_src_df, "provider_code", "provider_key", provider_cols, 4)

# 4. dim_vehicle (Source: silver.vehicle, ID: 14)
print("[LOAD] Processing dim_vehicle...")
vehicle_src_df = spark.table("silver.vehicle").select(
    "vehicle_id",
    "customer_id",
    "plate_number",
    "vehicle_brand",
    "vehicle_model",
    "manufacture_year",
    "vehicle_value",
    F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time")
)
vehicle_cols = ["customer_id", "plate_number", "vehicle_brand", "vehicle_model", "manufacture_year", "vehicle_value"]
load_scd2_dimension("gold.dim_vehicle", vehicle_src_df, "vehicle_id", "vehicle_key", vehicle_cols, 14)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
