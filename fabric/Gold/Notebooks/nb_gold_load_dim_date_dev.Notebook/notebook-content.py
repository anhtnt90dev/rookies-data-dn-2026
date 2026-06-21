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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, BooleanType
from delta.tables import DeltaTable

# Ensure parameters are correctly cast
batch_id = int(batch_id)
session_id = str(session_id)
run_mode = str(run_mode).upper()
DIM_FACT_TABLE_ID = int(p_table_id)

# Resolve table name dynamically from cfg.dim_fact_table
DIM_DATE_TABLE = "gold.dim_date"
try:
    row = spark.table("cfg.dim_fact_table").filter(F.col("id") == F.lit(DIM_FACT_TABLE_ID)).select("table_name").collect()
    if row:
        DIM_DATE_TABLE = f"gold.{row[0]['table_name']}"
except Exception as e:
    print(f"[INFO] Failed to resolve table name from cfg.dim_fact_table: {e}. Falling back to default: {DIM_DATE_TABLE}")

# Check if table already populated
is_populated = False
try:
    existing_count = spark.table(DIM_DATE_TABLE).count()
    if existing_count >= 4018: # 11 years calendar + 1 Unknown row
        is_populated = True
        print(f"[BYPASS] {DIM_DATE_TABLE} is already populated with {existing_count} records. Skipping load.")
except Exception as e:
    print(f"[INFO] Table check encountered exception (may not exist yet): {e}")

if not is_populated:
    # Start Table Ingestion Session
    table_session_id = start_table_layer(
        session_id=session_id,
        source_table_id=DIM_FACT_TABLE_ID,
        source_table_name=DIM_DATE_TABLE.split(".")[-1],
        layer="GOLD",
        batch_id=batch_id,
        load_type="FULL"
    )

    try:
        # 1. Generate date range DataFrame (2020-01-01 to 2030-12-31)
        date_range_df = spark.sql("""
            SELECT sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day) as date_array
        """).withColumn("full_date", F.explode("date_array")).drop("date_array")

        # 2. Derive Calendar Fields
        derived_dates_df = date_range_df.select(
            F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
            F.col("full_date"),
            F.dayofmonth(F.col("full_date")).alias("day_number"),
            F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
            F.weekofyear(F.col("full_date")).alias("week_number"),
            F.month(F.col("full_date")).alias("month_number"),
            F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
            F.quarter(F.col("full_date")).alias("quarter_number"),
            F.year(F.col("full_date")).alias("year_number"),
            F.date_format(F.col("full_date"), "yyyy-MM").alias("year_month"),
            F.dayofweek(F.col("full_date")).isin(1, 7).alias("is_weekend")
        )

        # 3. Create Unknown Row Dataframe (-1)
        unknown_schema = StructType([
            StructField("date_key", IntegerType(), False),
            StructField("full_date", DateType(), True),
            StructField("day_number", IntegerType(), True),
            StructField("day_name", StringType(), True),
            StructField("week_number", IntegerType(), True),
            StructField("month_number", IntegerType(), True),
            StructField("month_name", StringType(), True),
            StructField("quarter_number", IntegerType(), True),
            StructField("year_number", IntegerType(), True),
            StructField("year_month", StringType(), True),
            StructField("is_weekend", BooleanType(), False)
        ])

        unknown_row = [(
            -1,
            None,
            None,
            "Unknown",
            None,
            None,
            "Unknown",
            None,
            None,
            "Unknown",
            False
        )]
        unknown_df = spark.createDataFrame(unknown_row, unknown_schema)

        # 4. Union generated calendar and Unknown row
        final_dates_df = derived_dates_df.unionByName(unknown_df)

        # 5. Merge into Target Delta Table
        delta_table = DeltaTable.forName(spark, DIM_DATE_TABLE)
        delta_table.alias("target").merge(
            final_dates_df.alias("source"),
            "target.date_key = source.date_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # Get final counts
        total_count = spark.table(DIM_DATE_TABLE).count()
        print(f"[SUCCESS] Loaded {DIM_DATE_TABLE} successfully. Total count: {total_count}")

        # Finish Session
        finish_table_layer(
            table_session_id=table_session_id,
            layer="GOLD",
            status="SUCCESS",
            is_final_table_step=True,
            source_row_count=final_dates_df.count(),
            target_row_count=total_count,
            inserted_row=final_dates_df.count()
        )

    except Exception as err:
        tbl_short_name = DIM_DATE_TABLE.split(".")[-1]
        print(f"[ERROR] Failed to load {DIM_DATE_TABLE}: {err}")
        finish_table_layer(
            table_session_id=table_session_id,
            layer="GOLD",
            status="FAILED",
            error_code=f"{tbl_short_name.upper()}_LOAD_FAILED",
            error_message=str(err)[:1000]
        )
        raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
