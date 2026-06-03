# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14b073f3-0eb9-4315-8d49-155c39392779",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "21e1cea5-9786-4ce5-aa47-1d8255b69b82",
# META       "known_lakehouses": [
# META         {
# META           "id": "14b073f3-0eb9-4315-8d49-155c39392779"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

def create_schema() -> None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS cfg")

create_schema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_config_tables() -> None:

    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfg.source_table (
        id BIGINT,
        source_system STRING,
        source_type STRING,
        source_name STRING,
        source_location STRING,
        source_format STRING,
        delimiter STRING,
        load_type STRING,
        primary_key STRING,
        silver_transform_name STRING,
        watermark_column STRING,
        bronze_table_name STRING,
        bronze_path STRING,
        load_sequence INT,
        is_active BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfg.dim_fact_table (
        id BIGINT,
        table_name STRING,
        table_type STRING,
        gold_transform_name STRING,
        load_sequence INT,
        upsert_key STRING,
        is_active BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfg.watermark (
        source_table_id BIGINT,
        watermark_value TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfg.source_dim_fact (
        dim_fact_table_id BIGINT,
        source_table_id BIGINT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfg.next_run_mode (
        next_run_mode STRING,
        batch_id BIGINT,
        session_id BIGINT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    """)

create_config_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType, BooleanType
)

WS = "CarPro_Local" # Update name based on your workspace
BASE_PATH = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/lh_insurance_dev.Lakehouse" 


def insert_source_table_data() -> None:
    data = [
        (1, "crm_system", "database", "customers", f"{BASE_PATH}/Tables/dbo/customers", "table", None, "INCREMENTAL", "customer_id", None, "updated_date", "bronze.customer", f"{BASE_PATH}/Tables/bronze/customer", 1, True),
        (2, "crm_system", "database", "agents", f"{BASE_PATH}/Tables/dbo/agents", "table", None, "INCREMENTAL", "agent_id", None, "updated_date", "bronze.agent", f"{BASE_PATH}/Tables/bronze/agent", 2, True),
        (3, "crm_system", "database", "insurance_providers", f"{BASE_PATH}/Tables/dbo/insurance_providers", "table", None, "INCREMENTAL", "provider_code", None, "updated_date", "bronze.insurance_provider", f"{BASE_PATH}/Tables/bronze/insurance_provider", 3, True),
        (4, "crm_system", "database", "vehicle", f"{BASE_PATH}/Tables/dbo/vehicle", "table", None, "INCREMENTAL", "vehicle_id", None, "updated_date", "bronze.vehicle", f"{BASE_PATH}/Tables/bronze/vehicle", 4, True),
        (5, "crm_system", "database", "quotation", f"{BASE_PATH}/Tables/dbo/quotation", "table", None, "INCREMENTAL", "quotation_id", None, "updated_date", "bronze.quotation", f"{BASE_PATH}/Tables/bronze/quotation", 5, True),
        (6, "crm_system", "database", "quotation_item", f"{BASE_PATH}/Tables/dbo/quotation_item", "table", None, "INCREMENTAL", "quotation_item_id", None, "updated_date", "bronze.quotation_item", f"{BASE_PATH}/Tables/bronze/quotation_item", 6, True),

        (7, "policy_system", "file", "policy", f"{BASE_PATH}/Files/landing/policy_system/policy", "json", None, "INCREMENTAL", "policy_id", None, "last_updated", "bronze.policy", f"{BASE_PATH}/Tables/bronze/policy", 7, True),
        (8, "policy_system", "file", "cancellation", f"{BASE_PATH}/Files/landing/policy_system/cancellation", "json", None, "INCREMENTAL", "cancellation_id", None, "last_updated", "bronze.cancellation", f"{BASE_PATH}/Tables/bronze/cancellation", 8, True),
        (9, "payment_system", "file", "payment", f"{BASE_PATH}/Files/landing/payment_system/payment", "json", None, "INCREMENTAL", "payment_id", None, "last_updated", "bronze.payment", f"{BASE_PATH}/Tables/bronze/payment", 9, True),
    ]

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("source_system", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("source_location", StringType(), True),
        StructField("source_format", StringType(), True),
        StructField("delimiter", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("primary_key", StringType(), True),
        StructField("silver_transform_name", StringType(), True),
        StructField("watermark_column", StringType(), True),
        StructField("bronze_table_name", StringType(), True),
        StructField("bronze_path", StringType(), True),
        StructField("load_sequence", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
    ])

    df = (
        spark.createDataFrame(data, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("overwrite").saveAsTable("cfg.source_table")


def insert_watermark_data() -> None:
    data = [(i, None) for i in range(1, 10)]

    schema = StructType([
        StructField("source_table_id", LongType(), False),
        StructField("watermark_value", StringType(), True),
    ])

    df = (
        spark.createDataFrame(data, schema)
        .withColumn("watermark_value", F.col("watermark_value").cast("timestamp"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("overwrite").saveAsTable("cfg.watermark")


def insert_next_run_mode_data() -> None:
    data = [("NEW", None, None)]

    schema = StructType([
        StructField("next_run_mode", StringType(), False),
        StructField("batch_id", LongType(), True),
        StructField("session_id", LongType(), True),
    ])

    df = (
        spark.createDataFrame(data, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("overwrite").saveAsTable("cfg.next_run_mode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
TRUNCATE TABLE cfg.source_table
""")
spark.sql("""
TRUNCATE TABLE cfg.next_run_mode
""")
spark.sql("""
TRUNCATE TABLE cfg.watermark
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

insert_source_table_data()
insert_watermark_data()
insert_next_run_mode_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
