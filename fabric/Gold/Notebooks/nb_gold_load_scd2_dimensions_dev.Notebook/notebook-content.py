# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f6154ec7-4dbf-44f7-a335-159149f2ae56",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "c86fdecc-7ed1-42f4-9ec0-4b0274a76958",
# META       "known_lakehouses": [
# META         {
# META           "id": "f6154ec7-4dbf-44f7-a335-159149f2ae56"
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

# CELL ********************

%run nb_gold_audit_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

session_id = ""
batch_id = ""
run_mode = ""
p_table_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Cast parameters
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()
p_table_id = int(p_table_id) if p_table_id else None

# Load table ID mapping from cfg.dim_fact_table dynamically
dim_fact_lookup = {
    row["table_name"]: int(row["id"])
    for row in spark.table("cfg.dim_fact_table").select("id", "table_name").collect()
}

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

        # Apply incremental batch filtering if applicable
        if run_mode != "FULL" and batch_id and "_batch_id" in source_df.columns:
            source_df = source_df.where(F.col("_batch_id") == F.lit(str(batch_id)))
            print(f"[INFO] Incremental filter applied: _batch_id == {batch_id}")

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

        # Add action type mapping to avoid multiple filter actions
        joined_action = joined.withColumn(
            "action_type",
            F.when(F.col("tgt." + surrogate_key_col).isNull(), "INSERT_NEW")
             .when(F.col("src.row_hash") != F.col("tgt.row_hash"), "UPDATE_EXPIRE")
             .otherwise("NO_CHANGE")
        )
        
        # Cache joined DataFrame since we'll split and count
        joined_action = joined_action.cache()

        # Aggregate counts in a single action to avoid executing the join multiple times
        status_counts = joined_action.groupBy("action_type").count().collect()
        expire_count = 0
        new_key_count = 0
        for row in status_counts:
            if row["action_type"] == "INSERT_NEW":
                new_key_count = row["count"]
            elif row["action_type"] == "UPDATE_EXPIRE":
                expire_count = row["count"]

        total_inserted = 0

        if expire_count > 0 or new_key_count > 0:
            # Step A: Expire old active records (Delta Merge Update)
            if expire_count > 0:
                expire_df = joined_action.filter(F.col("action_type") == "UPDATE_EXPIRE").select(
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

            # Step B: Insert new versions and new business keys
            new_records = joined_action.filter(F.col("action_type") == "INSERT_NEW")
            new_versions = joined_action.filter(F.col("action_type") == "UPDATE_EXPIRE")
            
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

            # Generate surrogate keys dynamically based on max key + row_number using Partition Offset method
            max_key = spark.table(target_table_name).where(F.col(surrogate_key_col) != -1).agg(F.max(surrogate_key_col)).collect()[0][0]
            max_key = int(max_key) if max_key is not None else 0

            insert_source_with_pid = insert_source_df.withColumn("_pid", F.spark_partition_id())
            partition_counts = insert_source_with_pid.groupBy("_pid").count().collect()
            partition_counts.sort(key=lambda x: x["_pid"])
            
            if partition_counts:
                offsets = {}
                running_sum = max_key
                for row in partition_counts:
                    offsets[row["_pid"]] = running_sum
                    running_sum += row["count"]
                
                offsets_df = spark.createDataFrame([(k, v) for k, v in offsets.items()], ["_pid", "_offset"])
                window_insert = Window.partitionBy("_pid").orderBy(business_key_col)
                insert_final_df = insert_source_with_pid.join(F.broadcast(offsets_df), "_pid") \
                    .withColumn(surrogate_key_col, F.col("_offset") + F.row_number().over(window_insert).cast("bigint")) \
                    .withColumn("effective_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))) \
                    .withColumn("is_current", F.lit(True)) \
                    .withColumn("created_at", F.current_timestamp()) \
                    .withColumn("updated_at", F.current_timestamp()) \
                    .drop("_pid", "_offset")
            else:
                insert_final_df = insert_source_df.withColumn(surrogate_key_col, F.lit(None).cast("bigint")) \
                    .withColumn("effective_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))) \
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

        # Unpersist cache
        joined_action.unpersist()

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
        try:
            joined_action.unpersist()
        except:
            pass
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

# 1. dim_customer (Source: silver.customer)
dim_customer_id = dim_fact_lookup["dim_customer"]
if p_table_id is None or p_table_id == dim_customer_id:
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
        F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time"),
        "_batch_id"
    )
    customer_cols = ["full_name", "gender", "dob", "phone_number", "email", "city", "district"]
    load_scd2_dimension("gold.dim_customer", customer_src_df, "customer_id", "customer_key", customer_cols, dim_customer_id)

# 2. dim_agent (Source: silver.agent)
dim_agent_id = dim_fact_lookup["dim_agent"]
if p_table_id is None or p_table_id == dim_agent_id:
    print("[LOAD] Processing dim_agent...")
    agent_src_df = spark.table("silver.agent").select(
        "agent_id",
        "agent_name",
        "region",
        "branch",
        "manager_name",
        F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time"),
        "_batch_id"
    )
    agent_cols = ["agent_name", "region", "branch", "manager_name"]
    load_scd2_dimension("gold.dim_agent", agent_src_df, "agent_id", "agent_key", agent_cols, dim_agent_id)

# 3. dim_provider (Source: silver.provider)
dim_provider_id = dim_fact_lookup["dim_provider"]
if p_table_id is None or p_table_id == dim_provider_id:
    print("[LOAD] Processing dim_provider...")
    provider_src_df = spark.table("silver.provider").select(
        "provider_code",
        "provider_name",
        "provider_group",
        F.coalesce(F.col("is_active").cast("integer"), F.lit(1)).alias("active_flag"),
        F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time"),
        "_batch_id"
    )
    provider_cols = ["provider_name", "provider_group", "active_flag"]
    load_scd2_dimension("gold.dim_provider", provider_src_df, "provider_code", "provider_key", provider_cols, dim_provider_id)

# 4. dim_vehicle (Source: silver.vehicle)
dim_vehicle_id = dim_fact_lookup["dim_vehicle"]
if p_table_id is None or p_table_id == dim_vehicle_id:
    print("[LOAD] Processing dim_vehicle...")
    vehicle_src_df = spark.table("silver.vehicle").select(
        "vehicle_id",
        "customer_id",
        "plate_number",
        "vehicle_brand",
        "vehicle_model",
        "manufacture_year",
        "vehicle_value",
        F.coalesce(F.col("updated_at"), F.col("created_at")).alias("event_time"),
        "_batch_id"
    )
    vehicle_cols = ["customer_id", "plate_number", "vehicle_brand", "vehicle_model", "manufacture_year", "vehicle_value"]
    load_scd2_dimension("gold.dim_vehicle", vehicle_src_df, "vehicle_id", "vehicle_key", vehicle_cols, dim_vehicle_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
