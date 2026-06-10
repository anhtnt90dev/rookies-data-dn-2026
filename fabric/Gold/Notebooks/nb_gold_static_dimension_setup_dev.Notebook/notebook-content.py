# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "561e17c5-1ddb-4ff5-8cf0-4979fe0f6a9c",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "7cc49813-e700-4770-b90b-9613e48bb7df",
# META       "known_lakehouses": [
# META         {
# META           "id": "561e17c5-1ddb-4ff5-8cf0-4979fe0f6a9c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 1: EXPECTED SCHEMAS DEFINITION
# -------------------------------------------------------------------------
from pyspark.sql.functions import col, explode, sequence, to_date, year, month, dayofmonth, date_format, weekofyear, quarter, dayofweek, expr
from pyspark.sql.types import DateType

EXPECTED_SCHEMAS = {
    "gold.dim_date": {
        "date_key": "integer",
        "full_date": "date",
        "day_number": "integer",
        "day_name": "string",
        "week_number": "integer",
        "month_number": "integer",
        "month_name": "string",
        "quarter_number": "integer",
        "year_number": "integer",
        "year_month": "string",
        "is_weekend": "boolean"
    },
    "gold.dim_customer": {
        "customer_key": "long",
        "customer_id": "string",
        "full_name": "string",
        "gender": "string",
        "dob": "date",
        "phone_number": "string",
        "email": "string",
        "city": "string",
        "district": "string",
        "effective_from": "timestamp",
        "effective_to": "timestamp",
        "is_current": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_agent": {
        "agent_key": "long",
        "agent_id": "string",
        "agent_name": "string",
        "region": "string",
        "branch": "string",
        "manager_name": "string",
        "effective_from": "timestamp",
        "effective_to": "timestamp",
        "is_current": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_provider": {
        "provider_key": "long",
        "provider_code": "string",
        "provider_name": "string",
        "provider_group": "string",
        "active_flag": "integer",
        "effective_from": "timestamp",
        "effective_to": "timestamp",
        "is_current": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_package": {
        "package_key": "long",
        "package_code": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_coverage": {
        "coverage_key": "long",
        "coverage_type": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_quotation": {
        "quotation_key": "long",
        "quotation_id": "string",
        "quotation_expiry_date": "date",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_policy": {
        "policy_key": "long",
        "policy_id": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_quotation_status": {
        "quotation_status_key": "long",
        "quotation_status_code": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_policy_status": {
        "policy_status_key": "long",
        "policy_status_code": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_payment_status": {
        "payment_status_key": "long",
        "payment_status_code": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_payment_method": {
        "payment_method_key": "long",
        "payment_method_code": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_cancellation_reason": {
        "cancellation_reason_key": "long",
        "cancellation_reason": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.dim_vehicle": {
        "vehicle_key": "long",
        "vehicle_id": "string",
        "customer_id": "string",
        "plate_number": "string",
        "vehicle_brand": "string",
        "vehicle_model": "string",
        "manufacture_year": "integer",
        "vehicle_value": "decimal(18,2)",
        "effective_from": "timestamp",
        "effective_to": "timestamp",
        "is_current": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    },
    "gold.fact_quotation": {
        "quotation_id": "string",
        "customer_id": "string",
        "agent_id": "string",
        "provider_code": "string",
        "quotation_key": "long",
        "customer_key": "long",
        "agent_key": "long",
        "provider_key": "long",
        "package_key": "long",
        "quotation_status_key": "long",
        "quotation_date_key": "integer",
        "quotation_expiry_date_key": "integer",
        "vehicle_key": "long",
        "premium_amount": "decimal(18,2)",
        "converted_flag": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_batch_id": "string",
        "_source_system": "string",
        "pipeline_run_id": "string",
        "is_deleted": "boolean",
        "deleted_at": "timestamp",
        "delete_batch_id": "string"
    },
    "gold.fact_quotation_item": {
        "quotation_item_id": "string",
        "quotation_id": "string",
        "quotation_key": "long",
        "quotation_date_key": "integer",
        "customer_key": "long",
        "agent_key": "long",
        "provider_key": "long",
        "package_key": "long",
        "quotation_status_key": "long",
        "coverage_key": "long",
        "vehicle_key": "long",
        "coverage_amount": "decimal(18,2)",
        "deductible_amount": "decimal(18,2)",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_batch_id": "string",
        "_source_system": "string",
        "pipeline_run_id": "string",
        "is_deleted": "boolean",
        "deleted_at": "timestamp",
        "delete_batch_id": "string"
    },
    "gold.fact_policy": {
        "policy_id": "string",
        "policy_number": "string",
        "quotation_id": "string",
        "customer_id": "string",
        "provider_code": "string",
        "policy_key": "long",
        "quotation_key": "long",
        "customer_key": "long",
        "provider_key": "long",
        "agent_key": "long",
        "package_key": "long",
        "policy_status_key": "long",
        "issued_date_key": "integer",
        "policy_start_date_key": "integer",
        "policy_end_date_key": "integer",
        "vehicle_key": "long",
        "premium_amount": "decimal(18,2)",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_batch_id": "string",
        "_source_system": "string",
        "pipeline_run_id": "string",
        "is_deleted": "boolean",
        "deleted_at": "timestamp",
        "delete_batch_id": "string"
    },
    "gold.fact_payment": {
        "payment_id": "string",
        "policy_id": "string",
        "transaction_reference": "string",
        "policy_key": "long",
        "payment_status_key": "long",
        "payment_method_key": "long",
        "payment_date_key": "integer",
        "issued_date_key": "integer",
        "customer_key": "long",
        "provider_key": "long",
        "vehicle_key": "long",
        "payment_amount": "decimal(18,2)",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_batch_id": "string",
        "_source_system": "string",
        "pipeline_run_id": "string",
        "is_deleted": "boolean",
        "deleted_at": "timestamp",
        "delete_batch_id": "string"
    },
    "gold.fact_cancellation": {
        "cancellation_id": "string",
        "policy_id": "string",
        "policy_key": "long",
        "cancellation_reason_key": "long",
        "cancellation_date_key": "integer",
        "customer_key": "long",
        "provider_key": "long",
        "vehicle_key": "long",
        "refund_amount": "decimal(18,2)",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_batch_id": "string",
        "_source_system": "string",
        "pipeline_run_id": "string",
        "is_deleted": "boolean",
        "deleted_at": "timestamp",
        "delete_batch_id": "string"
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 2: SCHEMA VALIDATION HELPER FUNCTION
# -------------------------------------------------------------------------
def validate_gold_schemas() -> None:
    """
    Validates that existing Gold tables exist in Spark catalog
    and that all column names and data types match specifications.
    """
    print("=========================================================================")
    print("1. STARTING SCHEMA VALIDATION FOR GOLD TABLES")
    print("=========================================================================")
    
    validation_passed = True
    
    for table_name, expected_cols in EXPECTED_SCHEMAS.items():
        if not spark.catalog.tableExists(table_name):
            print(f"[ERROR] Table '{table_name}' does not exist in catalog!")
            validation_passed = False
            continue
            
        print(f"[INFO] Validating table '{table_name}'...")
        schema = spark.table(table_name).schema
        mismatches = []
        
        for col_name, expected_type in expected_cols.items():
            try:
                field = schema[col_name]
                actual_type = field.dataType.simpleString()
                
                # Normalize types to avoid name variations like bigint/long
                norm_expected = expected_type.lower().replace("long", "bigint").replace(" ", "")
                norm_actual = actual_type.lower().replace(" ", "")
                
                if norm_expected != norm_actual:
                    mismatches.append(f"Column '{col_name}' type mismatch: expected {expected_type}, found {actual_type}")
            except KeyError:
                mismatches.append(f"Missing column '{col_name}'")
                
        if mismatches:
            print(f"[WARN] Table '{table_name}' schema mismatch:")
            for mismatch in mismatches:
                print(f"  - {mismatch}")
            validation_passed = False
        else:
            print(f"[OK] Table '{table_name}' schema matches successfully.")
            
    print("-------------------------------------------------------------------------")
    if validation_passed:
        print("[SUCCESS] All existing Gold schemas validated successfully.")
    else:
        print("[WARNING] Schema validation finished with warnings or errors. Check log above.")
    print("=========================================================================\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 3: dim_date GENERATION
# -------------------------------------------------------------------------
def generate_dim_date(start_date: str = "2020-01-01", end_date: str = "2030-12-31") -> None:
    """
    Generates and populates gold.dim_date calendar dimension for the specified date range.
    """
    print("=========================================================================")
    print(f"2. GENERATING CALENDAR FOR dim_date: {start_date} TO {end_date}")
    print("=========================================================================")
    
    # Create single row boundary dataframe
    df_range = spark.createDataFrame([(start_date, end_date)], ["start", "end"])
    
    # Generate full date sequence
    df_dates = df_range.select(
        explode(sequence(to_date(col("start")), to_date(col("end")), expr("interval 1 day"))).alias("full_date")
    )
    
    # Compute calendar attributes
    df_dim_date = df_dates.select(
        date_format(col("full_date"), "yyyyMMdd").cast("integer").alias("date_key"),
        col("full_date"),
        dayofmonth(col("full_date")).alias("day_number"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        weekofyear(col("full_date")).alias("week_number"),
        month(col("full_date")).alias("month_number"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        quarter(col("full_date")).alias("quarter_number"),
        year(col("full_date")).alias("year_number"),
        date_format(col("full_date"), "yyyy-MM").alias("year_month"),
        dayofweek(col("full_date")).isin(1, 7).alias("is_weekend")
    )
    
    # Write to Delta table
    df_dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")
    
    actual_count = spark.table("gold.dim_date").count()
    print(f"[SUCCESS] gold.dim_date generated successfully with {actual_count} rows.")
    print("=========================================================================\n")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 4: UNKNOWN MEMBER SQL MERGE QUERIES
# -------------------------------------------------------------------------
DIM_UNKNOWN_QUERIES = {
    "gold.dim_customer": """
        MERGE INTO gold.dim_customer AS target
        USING (SELECT -1 AS customer_key) AS source
        ON target.customer_key = source.customer_key
        WHEN NOT MATCHED THEN
          INSERT (customer_key, customer_id, full_name, gender, dob, phone_number, email, city, district, effective_from, effective_to, is_current, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', CAST(NULL AS DATE), 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', CAST('1900-01-01 00:00:00' AS TIMESTAMP), CAST('9999-12-31 23:59:59' AS TIMESTAMP), true, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_agent": """
        MERGE INTO gold.dim_agent AS target
        USING (SELECT -1 AS agent_key) AS source
        ON target.agent_key = source.agent_key
        WHEN NOT MATCHED THEN
          INSERT (agent_key, agent_id, agent_name, region, branch, manager_name, effective_from, effective_to, is_current, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', CAST('1900-01-01 00:00:00' AS TIMESTAMP), CAST('9999-12-31 23:59:59' AS TIMESTAMP), true, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_provider": """
        MERGE INTO gold.dim_provider AS target
        USING (SELECT -1 AS provider_key) AS source
        ON target.provider_key = source.provider_key
        WHEN NOT MATCHED THEN
          INSERT (provider_key, provider_code, provider_name, provider_group, active_flag, effective_from, effective_to, is_current, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', -1, CAST('1900-01-01 00:00:00' AS TIMESTAMP), CAST('9999-12-31 23:59:59' AS TIMESTAMP), true, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_package": """
        MERGE INTO gold.dim_package AS target
        USING (SELECT -1 AS package_key) AS source
        ON target.package_key = source.package_key
        WHEN NOT MATCHED THEN
          INSERT (package_key, package_code, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_coverage": """
        MERGE INTO gold.dim_coverage AS target
        USING (SELECT -1 AS coverage_key) AS source
        ON target.coverage_key = source.coverage_key
        WHEN NOT MATCHED THEN
          INSERT (coverage_key, coverage_type, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_quotation": """
        MERGE INTO gold.dim_quotation AS target
        USING (SELECT -1 AS quotation_key) AS source
        ON target.quotation_key = source.quotation_key
        WHEN NOT MATCHED THEN
          INSERT (quotation_key, quotation_id, quotation_expiry_date, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CAST(NULL AS DATE), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_policy": """
        MERGE INTO gold.dim_policy AS target
        USING (SELECT -1 AS policy_key) AS source
        ON target.policy_key = source.policy_key
        WHEN NOT MATCHED THEN
          INSERT (policy_key, policy_id, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_quotation_status": """
        MERGE INTO gold.dim_quotation_status AS target
        USING (SELECT -1 AS quotation_status_key) AS source
        ON target.quotation_status_key = source.quotation_status_key
        WHEN NOT MATCHED THEN
          INSERT (quotation_status_key, quotation_status_code, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_policy_status": """
        MERGE INTO gold.dim_policy_status AS target
        USING (SELECT -1 AS policy_status_key) AS source
        ON target.policy_status_key = source.policy_status_key
        WHEN NOT MATCHED THEN
          INSERT (policy_status_key, policy_status_code, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_payment_status": """
        MERGE INTO gold.dim_payment_status AS target
        USING (SELECT -1 AS payment_status_key) AS source
        ON target.payment_status_key = source.payment_status_key
        WHEN NOT MATCHED THEN
          INSERT (payment_status_key, payment_status_code, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_payment_method": """
        MERGE INTO gold.dim_payment_method AS target
        USING (SELECT -1 AS payment_method_key) AS source
        ON target.payment_method_key = source.payment_method_key
        WHEN NOT MATCHED THEN
          INSERT (payment_method_key, payment_method_code, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_cancellation_reason": """
        MERGE INTO gold.dim_cancellation_reason AS target
        USING (SELECT -1 AS cancellation_reason_key) AS source
        ON target.cancellation_reason_key = source.cancellation_reason_key
        WHEN NOT MATCHED THEN
          INSERT (cancellation_reason_key, cancellation_reason, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """,
    "gold.dim_vehicle": """
        MERGE INTO gold.dim_vehicle AS target
        USING (SELECT -1 AS vehicle_key) AS source
        ON target.vehicle_key = source.vehicle_key
        WHEN NOT MATCHED THEN
          INSERT (vehicle_key, vehicle_id, customer_id, plate_number, vehicle_brand, vehicle_model, manufacture_year, vehicle_value, effective_from, effective_to, is_current, created_at, updated_at)
          VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', -1, CAST(0.00 AS DECIMAL(18,2)), CAST('1900-01-01 00:00:00' AS TIMESTAMP), CAST('9999-12-31 23:59:59' AS TIMESTAMP), true, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
}

def insert_unknown_members() -> None:
    """
    Idempotently inserts a -1 Unknown member record in each dimension table
    if it does not already exist.
    """
    print("=========================================================================")
    print("3. INSERTING -1 UNKNOWN MEMBERS")
    print("=========================================================================")
    
    for table_name, merge_sql in DIM_UNKNOWN_QUERIES.items():
        if not spark.catalog.tableExists(table_name):
            print(f"[SKIP] Table '{table_name}' does not exist. Skipping Unknown member insertion.")
            continue
            
        print(f"[INFO] Merging Unknown member (-1) into '{table_name}'...")
        spark.sql(merge_sql)
        
        # Verify row exists
        key_col_name = table_name.split(".")[-1] + "_key"
        if table_name == "gold.dim_cancellation_reason":
            key_col_name = "cancellation_reason_key"
            
        check_df = spark.table(table_name).filter(col(key_col_name) == -1)
        if check_df.count() == 1:
            print(f"[OK] Unknown member verified for '{table_name}'.")
        else:
            print(f"[ERROR] Verification failed for '{table_name}'! Row count = {check_df.count()}")
            
    print("=========================================================================\n")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------
# CELL 5: MAIN RUNNER
# -------------------------------------------------------------------------
validate_gold_schemas()
generate_dim_date("2020-01-01", "2030-12-31")
insert_unknown_members()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
