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
        source_to_bronze_mapping_path STRING,
        bronze_to_silver_mapping_path STRING,
        silver_transform_name STRING,
        watermark_column STRING,
        bronze_table_name STRING,
        silver_table_name STRING,
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
        session_id STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfg.retry_policy (
        id BIGINT,
        policy_name STRING,
        max_retry_count INT,
        retry_delay_seconds INT,
        backoff_strategy STRING,
        retryable_error_types STRING,
        non_retryable_error_types STRING,
        is_active BOOLEAN,
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


def insert_source_table_data() -> None:
    MAPPING_PATH = "Files/config/mapping"

    data = [
        (1, "crm_system", "database", "customers", "dbo.customers", "table", None, "INCREMENTAL", "customer_id",
         None, "updated_date", "bronze.customer", "silver.customer",
         f"{MAPPING_PATH}/source-to-bronze/customer.json", f"{MAPPING_PATH}/bronze-to-silver/customer.json", 1, True),

        (2, "crm_system", "database", "agents", "dbo.agents", "table", None, "INCREMENTAL", "agent_id",
         None, "updated_date", "bronze.agent", "silver.agent",
         f"{MAPPING_PATH}/source-to-bronze/agent.json", f"{MAPPING_PATH}/bronze-to-silver/agent.json", 2, True),

        (3, "crm_system", "database", "insurance_providers", "dbo.insurance_providers", "table", None, "INCREMENTAL", "provider_code",
         None, "updated_date", "bronze.insurance_provider", "silver.insurance_provider",
         f"{MAPPING_PATH}/source-to-bronze/insurance_provider.json", f"{MAPPING_PATH}/bronze-to-silver/insurance_provider.json", 3, True),

        (4, "crm_system", "database", "vehicle", "dbo.vehicle", "table", None, "INCREMENTAL", "vehicle_id",
         None, "updated_date", "bronze.vehicle", "silver.vehicle",
         f"{MAPPING_PATH}/source-to-bronze/vehicle.json", f"{MAPPING_PATH}/bronze-to-silver/vehicle.json", 4, True),

        (5, "crm_system", "database", "quotation", "dbo.quotation", "table", None, "INCREMENTAL", "quotation_id",
         None, "updated_date", "bronze.quotation", "silver.quotation",
         f"{MAPPING_PATH}/source-to-bronze/quotation.json", f"{MAPPING_PATH}/bronze-to-silver/quotation.json", 5, True),

        (6, "crm_system", "database", "quotation_item", "dbo.quotation_item", "table", None, "INCREMENTAL", "quotation_item_id",
         None, "updated_date", "bronze.quotation_item", "silver.quotation_item",
         f"{MAPPING_PATH}/source-to-bronze/quotation_item.json", f"{MAPPING_PATH}/bronze-to-silver/quotation_item.json", 6, True),

        (7, "policy_system", "file", "policy", "Files/landing/policy_system/policy", "json", None, "INCREMENTAL", "policy_id",
         None, "last_updated", "bronze.policy", "silver.policy",
         f"{MAPPING_PATH}/source-to-bronze/policy.json", f"{MAPPING_PATH}/bronze-to-silver/policy.json", 7, True),

        (8, "policy_system", "file", "cancellation", "Files/landing/policy_system/cancellation", "json", None, "INCREMENTAL", "cancellation_id",
         None, "last_updated", "bronze.cancellation", "silver.cancellation",
         f"{MAPPING_PATH}/source-to-bronze/cancellation.json", f"{MAPPING_PATH}/bronze-to-silver/cancellation.json", 8, True),

        (9, "payment_system", "file", "payment", "Files/landing/payment_system/payment", "json", None, "INCREMENTAL", "payment_id",
         None, "last_updated", "bronze.payment", "silver.payment",
         f"{MAPPING_PATH}/source-to-bronze/payment.json", f"{MAPPING_PATH}/bronze-to-silver/payment.json", 9, True),
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
        StructField("silver_table_name", StringType(), True),
        StructField("source_to_bronze_mapping_path", StringType(), True),
        StructField("bronze_to_silver_mapping_path", StringType(), True),
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
        StructField("session_id", StringType(), True),
    ])

    next_run_df = (
        spark.createDataFrame(data, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    next_run_df.write.format("delta").mode("overwrite").saveAsTable("cfg.next_run_mode")


def insert_retry_policy_data() -> None:
    data = [
        (
            1,
            "default_transient_system_retry",
            2,
            60,
            "FIXED_DELAY",
            "SYSTEM",
            "DATA,RULE,CONFIG,UNKNOWN",
            True,
        )
    ]

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("policy_name", StringType(), False),
        StructField("max_retry_count", IntegerType(), False),
        StructField("retry_delay_seconds", IntegerType(), False),
        StructField("backoff_strategy", StringType(), False),
        StructField("retryable_error_types", StringType(), False),
        StructField("non_retryable_error_types", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ])

    retry_policy_df = (
        spark.createDataFrame(data, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    retry_policy_df.write.format("delta").mode("overwrite").saveAsTable("cfg.retry_policy")


def insert_dim_fact_table_data() -> None:
    data = [
        (1, "dim_date", "DIM", None, 1, "date_key", True),
        (2, "dim_customer", "DIM", None, 2, "customer_id", True),
        (3, "dim_agent", "DIM", None, 3, "agent_id", True),
        (4, "dim_provider", "DIM", None, 4, "provider_code", True),
        (5, "dim_package", "DIM", None, 5, "package_code", True),
        (6, "dim_coverage", "DIM", None, 6, "coverage_type", True),
        (7, "dim_quotation", "DIM", None, 7, "quotation_id", True),
        (8, "dim_policy", "DIM", None, 8, "policy_id", True),
        (9, "dim_quotation_status", "DIM", None, 9, "quotation_status_code", True),
        (10, "dim_policy_status", "DIM", None, 10, "policy_status_code", True),
        (11, "dim_payment_status", "DIM", None, 11, "payment_status_code", True),
        (12, "dim_payment_method", "DIM", None, 12, "payment_method_code", True),
        (13, "dim_cancellation_reason", "DIM", None, 13, "cancellation_reason", True),
        (14, "dim_vehicle", "DIM", None, 14, "vehicle_id", True),
        (15, "fact_quotation", "FACT", None, 15, "quotation_id", True),
        (16, "fact_quotation_item", "FACT", None, 16, "quotation_item_id", True),
        (17, "fact_policy", "FACT", None, 17, "policy_id", True),
        (18, "fact_payment", "FACT", None, 18, "payment_id", True),
        (19, "fact_cancellation", "FACT", None, 19, "cancellation_id", True),
    ]

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("table_name", StringType(), True),
        StructField("table_type", StringType(), True),
        StructField("gold_transform_name", StringType(), True),
        StructField("load_sequence", IntegerType(), True),
        StructField("upsert_key", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ])

    df = (
        spark.createDataFrame(data, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("overwrite").saveAsTable("cfg.dim_fact_table")

def insert_source_dim_fact_data() -> None:
    data = [
        # dim_date is a generated calendar dimension, so it does not map to a source table here.

        # Dimensions
        (2, 1),    # dim_customer <- customers
        (3, 2),    # dim_agent <- agents
        (4, 3),    # dim_provider <- insurance_providers
        (5, 5),    # dim_package <- quotation
        (6, 6),    # dim_coverage <- quotation_item
        (7, 5),    # dim_quotation <- quotation
        (8, 7),    # dim_policy <- policy
        (9, 5),    # dim_quotation_status <- quotation
        (10, 7),   # dim_policy_status <- policy
        (11, 9),   # dim_payment_status <- payment
        (12, 9),   # dim_payment_method <- payment
        (13, 8),   # dim_cancellation_reason <- cancellation
        (14, 4),   # dim_vehicle <- vehicle

        # fact_quotation <- quotation + related dimensions
        (15, 5),   # quotation
        (15, 1),   # customers
        (15, 2),   # agents
        (15, 3),   # insurance_providers
        (15, 4),   # vehicle

        # fact_quotation_item <- quotation_item + quotation context
        (16, 6),   # quotation_item
        (16, 5),   # quotation
        (16, 1),   # customers
        (16, 2),   # agents
        (16, 3),   # insurance_providers
        (16, 4),   # vehicle

        # fact_policy <- policy + quotation context
        (17, 7),   # policy
        (17, 5),   # quotation
        (17, 1),   # customers
        (17, 2),   # agents
        (17, 3),   # insurance_providers
        (17, 4),   # vehicle

        # fact_payment <- payment + policy context
        (18, 9),   # payment
        (18, 7),   # policy
        (18, 1),   # customers
        (18, 3),   # insurance_providers
        (18, 4),   # vehicle

        # fact_cancellation <- cancellation + policy context
        (19, 8),   # cancellation
        (19, 7),   # policy
        (19, 1),   # customers
        (19, 3),   # insurance_providers
        (19, 4),   # vehicle
    ]

    schema = StructType([
        StructField("dim_fact_table_id", LongType(), False),
        StructField("source_table_id", LongType(), False),
    ])

    df = (
        spark.createDataFrame(data, schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("overwrite").saveAsTable("cfg.source_dim_fact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""TRUNCATE TABLE cfg.source_table""")
spark.sql("""TRUNCATE TABLE cfg.next_run_mode""")
spark.sql("""TRUNCATE TABLE cfg.watermark""")
spark.sql("""TRUNCATE TABLE cfg.retry_policy""")
spark.sql("""TRUNCATE TABLE cfg.source_dim_fact""")
spark.sql("""TRUNCATE TABLE cfg.dim_fact_table""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

insert_source_table_data()
insert_watermark_data()
insert_next_run_mode_data()
insert_retry_policy_data()
insert_dim_fact_table_data()
insert_source_dim_fact_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
