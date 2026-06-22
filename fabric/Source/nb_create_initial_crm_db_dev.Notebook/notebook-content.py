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
# META     },
# META     "environment": {
# META       "environmentId": "0519000f-54ce-ab89-4a90-a6f539ba103a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql import functions as F
from datetime import datetime, date, timedelta
from decimal import Decimal

spark.sql("CREATE SCHEMA IF NOT EXISTS dbo")

tables = [
    "dbo.quotation_item",
    "dbo.quotation",
    "dbo.vehicle",
    "dbo.insurance_providers",
    "dbo.agents",
    "dbo.customers"
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")


# =========================
# Create dbo tables
# =========================

spark.sql("""
CREATE TABLE dbo.customers (
    customer_id STRING,
    full_name STRING,
    gender STRING,
    dob DATE,
    phone_number STRING,
    email STRING,
    city STRING,
    district STRING,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.agents (
    agent_id STRING,
    agent_name STRING,
    region STRING,
    branch STRING,
    manager_name STRING,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.insurance_providers (
    provider_code STRING,
    provider_name STRING,
    provider_group STRING,
    active_flag INT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.vehicle (
    vehicle_id STRING,
    customer_id STRING,
    plate_number STRING,
    vehicle_brand STRING,
    vehicle_model STRING,
    manufacture_year INT,
    vehicle_value DECIMAL(18,2),
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.quotation (
    quotation_id STRING,
    customer_id STRING,
    agent_id STRING,
    provider_code STRING,
    quotation_date TIMESTAMP,
    quotation_status STRING,
    package_code STRING,
    premium_amount DECIMAL(18,2),
    quotation_expiry_date TIMESTAMP,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.quotation_item (
    quotation_item_id STRING,
    quotation_id STRING,
    coverage_type STRING,
    coverage_amount DECIMAL(18,2),
    deductible_amount DECIMAL(18,2),
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# Mock data until 2026-05-25
# =========================

mock_end_date = datetime(2026, 5, 25, 12, 0, 0)

providers = [
    ("BV", "Bao Viet", "Domestic", 1),
    ("PVI", "PVI Insurance", "Domestic", 1),
    ("PTI", "PTI Insurance", "Domestic", 1),
    ("MIC", "MIC Insurance", "Domestic", 1),
    ("LIB", "Liberty Insurance", "International", 1),
    ("BIC", "BIC Insurance", "Domestic", 1)
]

provider_rows = [
    Row(
        provider_code=code,
        provider_name=name,
        provider_group=group,
        active_flag=active,
        created_date=mock_end_date
    )
    for code, name, group, active in providers
]

provider_df = (
    spark.createDataFrame(provider_rows)
    .withColumn("active_flag", F.col("active_flag").cast("int"))
    .withColumn("updated_date", F.col("created_date"))
)

provider_df.write.mode("append").saveAsTable("dbo.insurance_providers")


agents = [
    ("AG001", "Nguyen Van An", "North", "Ha Noi", "Tran Minh"),
    ("AG002", "Tran Thi Hoa", "South", "Ho Chi Minh", "Le Anh"),
    ("AG003", "Pham Minh Duc", "Central", "Da Nang", "Nguyen Long"),
    ("AG004", "Le Thi Mai", "South", "Can Tho", "Le Anh")
]

agent_rows = [
    Row(
        agent_id=agent_id,
        agent_name=agent_name,
        region=region,
        branch=branch,
        manager_name=manager_name,
        created_date=mock_end_date
    )
    for agent_id, agent_name, region, branch, manager_name in agents
]

agent_df = (
    spark.createDataFrame(agent_rows)
    .withColumn("updated_date", F.col("created_date"))
)

agent_df.write.mode("append").saveAsTable("dbo.agents")


customers = []
vehicles = []
quotations = []
quotation_items = []

for i in range(1, 1001):
    customer_id = f"CUS{i:04d}"
    vehicle_id = f"VEH{i:04d}"
    quotation_id = f"QUO{i:05d}"
    quotation_item_id = f"QI{i:05d}"

    created_ts = mock_end_date - timedelta(days=i % 365)

    customers.append(Row(
        customer_id=customer_id,
        full_name=f"Customer {i}",
        gender="Male" if i % 2 == 0 else "Female",
        dob=date(1995, 1, 1) - timedelta(days=i * 10),
        phone_number=f"090{i:07d}",
        email=f"customer{i}@mail.com",
        city=["Ha Noi", "Ho Chi Minh", "Da Nang"][i % 3],
        district=f"District {i % 10 + 1}",
        created_date=created_ts
    ))

    vehicles.append(Row(
        vehicle_id=vehicle_id,
        customer_id=customer_id,
        plate_number=f"51A-{i:05d}",
        vehicle_brand=["Toyota", "Hyundai", "Mazda", "VinFast"][i % 4],
        vehicle_model=f"Model {i % 10 + 1}",
        manufacture_year=2020 + (i % 5),
        vehicle_value=Decimal(str(500000000 + i * 100000)).quantize(Decimal("0.00")),
        created_date=created_ts
    ))

    quotations.append(Row(
        quotation_id=quotation_id,
        customer_id=customer_id,
        agent_id=["AG001", "AG002", "AG003", "AG004"][i % 4],
        provider_code=["BV", "PVI", "PTI", "MIC", "LIB"][i % 5],
        quotation_date=created_ts,
        quotation_status=["QUOTED", "ACCEPTED", "REJECTED", "EXPIRED", "CONVERTED"][i % 5],
        package_code=["BASIC", "STANDARD", "PREMIUM", "VIP"][i % 4],
        premium_amount=Decimal(str(5000000 + i * 10000)).quantize(Decimal("0.00")),
        quotation_expiry_date=created_ts + timedelta(days=30),
        created_date=created_ts
    ))

    quotation_items.append(Row(
        quotation_item_id=quotation_item_id,
        quotation_id=quotation_id,
        coverage_type=["Physical Damage", "Theft", "Third Party Liability", "Personal Accident"][i % 4],
        coverage_amount=Decimal(str(100000000 + i * 50000)).quantize(Decimal("0.00")),
        deductible_amount=Decimal("1000000.00"),
        created_date=created_ts
    ))


customer_df = (
    spark.createDataFrame(customers)
    .withColumn("updated_date", F.col("created_date"))
)

vehicle_df = (
    spark.createDataFrame(vehicles)
    .withColumn("manufacture_year", F.col("manufacture_year").cast("int"))
    .withColumn("vehicle_value", F.col("vehicle_value").cast("decimal(18,2)"))
    .withColumn("updated_date", F.col("created_date"))
)

quotation_df = (
    spark.createDataFrame(quotations)
    .withColumn("premium_amount", F.col("premium_amount").cast("decimal(18,2)"))
    .withColumn("updated_date", F.col("created_date"))
)

quotation_item_df = (
    spark.createDataFrame(quotation_items)
    .withColumn("coverage_amount", F.col("coverage_amount").cast("decimal(18,2)"))
    .withColumn("deductible_amount", F.col("deductible_amount").cast("decimal(18,2)"))
    .withColumn("updated_date", F.col("created_date"))
)


customer_df.write.mode("append").saveAsTable("dbo.customers")
vehicle_df.write.mode("append").saveAsTable("dbo.vehicle")
quotation_df.write.mode("append").saveAsTable("dbo.quotation")
quotation_item_df.write.mode("append").saveAsTable("dbo.quotation_item")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM dbo.customers
UNION ALL SELECT 'agents', COUNT(*) FROM dbo.agents
UNION ALL SELECT 'insurance_providers', COUNT(*) FROM dbo.insurance_providers
UNION ALL SELECT 'vehicle', COUNT(*) FROM dbo.vehicle
UNION ALL SELECT 'quotation', COUNT(*) FROM dbo.quotation
UNION ALL SELECT 'quotation_item', COUNT(*) FROM dbo.quotation_item
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime

# =========================
# Config
# =========================

mock_row_count = 1_000_000_000   # đổi thành 5_000_000 hoặc 10_000_000
mock_end_date = datetime(2026, 5, 25, 12, 0, 0)

spark.sql("CREATE SCHEMA IF NOT EXISTS dbo")

tables = [
    "dbo.quotation_item",
    "dbo.quotation",
    "dbo.vehicle",
    "dbo.insurance_providers",
    "dbo.agents",
    "dbo.customers"
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")


# =========================
# Create dbo tables
# =========================

spark.sql("""
CREATE TABLE dbo.customers (
    customer_id STRING,
    full_name STRING,
    gender STRING,
    dob DATE,
    phone_number STRING,
    email STRING,
    city STRING,
    district STRING,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.agents (
    agent_id STRING,
    agent_name STRING,
    region STRING,
    branch STRING,
    manager_name STRING,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.insurance_providers (
    provider_code STRING,
    provider_name STRING,
    provider_group STRING,
    active_flag INT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.vehicle (
    vehicle_id STRING,
    customer_id STRING,
    plate_number STRING,
    vehicle_brand STRING,
    vehicle_model STRING,
    manufacture_year INT,
    vehicle_value DECIMAL(18,2),
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.quotation (
    quotation_id STRING,
    customer_id STRING,
    agent_id STRING,
    provider_code STRING,
    quotation_date TIMESTAMP,
    quotation_status STRING,
    package_code STRING,
    premium_amount DECIMAL(18,2),
    quotation_expiry_date TIMESTAMP,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE dbo.quotation_item (
    quotation_item_id STRING,
    quotation_id STRING,
    coverage_type STRING,
    coverage_amount DECIMAL(18,2),
    deductible_amount DECIMAL(18,2),
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")


# =========================
# Fixed provider data
# =========================

providers = [
    ("BV", "Bao Viet", "Domestic", 1),
    ("PVI", "PVI Insurance", "Domestic", 1),
    ("PTI", "PTI Insurance", "Domestic", 1),
    ("MIC", "MIC Insurance", "Domestic", 1),
    ("LIB", "Liberty Insurance", "International", 1),
    ("BIC", "BIC Insurance", "Domestic", 1)
]

provider_df = (
    spark.createDataFrame(
        providers,
        ["provider_code", "provider_name", "provider_group", "active_flag"]
    )
    .withColumn("active_flag", F.col("active_flag").cast("int"))
    .withColumn("created_date", F.lit(mock_end_date).cast("timestamp"))
    .withColumn("updated_date", F.lit(mock_end_date).cast("timestamp"))
)

provider_df.write.mode("append").saveAsTable("dbo.insurance_providers")


# =========================
# Fixed agent data
# =========================

agents = [
    ("AG001", "Nguyen Van An", "North", "Ha Noi", "Tran Minh"),
    ("AG002", "Tran Thi Hoa", "South", "Ho Chi Minh", "Le Anh"),
    ("AG003", "Pham Minh Duc", "Central", "Da Nang", "Nguyen Long"),
    ("AG004", "Le Thi Mai", "South", "Can Tho", "Le Anh")
]

agent_df = (
    spark.createDataFrame(
        agents,
        ["agent_id", "agent_name", "region", "branch", "manager_name"]
    )
    .withColumn("created_date", F.lit(mock_end_date).cast("timestamp"))
    .withColumn("updated_date", F.lit(mock_end_date).cast("timestamp"))
)

agent_df.write.mode("append").saveAsTable("dbo.agents")


# =========================
# Generate scalable mock data
# =========================

base_df = (
    spark.range(1, mock_row_count + 1)
    .withColumnRenamed("id", "i")
    .withColumn(
        "created_date",
        F.expr("""
            timestampadd(
                DAY,
                -CAST(i % 365 AS INT),
                timestamp'2026-05-25 12:00:00'
            )
        """)
    )
)

customer_df = base_df.select(
    F.format_string("CUS%04d", F.col("i")).alias("customer_id"),
    F.concat(F.lit("Customer "), F.col("i")).alias("full_name"),
    F.when(F.col("i") % 2 == 0, "Male").otherwise("Female").alias("gender"),
    F.expr("""
        date_add(
            date'1980-01-01',
            CAST(i % 12000 AS INT)
        )
    """).alias("dob"),
    F.format_string("090%07d", F.col("i")).alias("phone_number"),
    F.concat(F.lit("customer"), F.col("i"), F.lit("@mail.com")).alias("email"),
    F.element_at(
        F.array(F.lit("Ha Noi"), F.lit("Ho Chi Minh"), F.lit("Da Nang")),
        (F.col("i") % 3 + 1).cast("int")
    ).alias("city"),
    F.concat(F.lit("District "), (F.col("i") % 10 + 1)).alias("district"),
    F.col("created_date"),
    F.col("created_date").alias("updated_date")
)

vehicle_df = base_df.select(
    F.format_string("VEH%04d", F.col("i")).alias("vehicle_id"),
    F.format_string("CUS%04d", F.col("i")).alias("customer_id"),
    F.format_string("51A-%05d", F.col("i")).alias("plate_number"),
    F.element_at(
        F.array(F.lit("Toyota"), F.lit("Hyundai"), F.lit("Mazda"), F.lit("VinFast")),
        (F.col("i") % 4 + 1).cast("int")
    ).alias("vehicle_brand"),
    F.concat(F.lit("Model "), (F.col("i") % 10 + 1)).alias("vehicle_model"),
    (F.lit(2020) + (F.col("i") % 5)).cast("int").alias("manufacture_year"),

    # 300M - 1.8B
    (
        F.rand(seed=42) * 1500000000 + 300000000
    ).cast("decimal(18,2)").alias("vehicle_value"),

    F.col("created_date"),
    F.col("created_date").alias("updated_date")
)

quotation_df = base_df.select(
    F.format_string("QUO%05d", F.col("i")).alias("quotation_id"),
    F.format_string("CUS%04d", F.col("i")).alias("customer_id"),
    F.element_at(
        F.array(F.lit("AG001"), F.lit("AG002"), F.lit("AG003"), F.lit("AG004")),
        (F.col("i") % 4 + 1).cast("int")
    ).alias("agent_id"),
    F.element_at(
        F.array(F.lit("BV"), F.lit("PVI"), F.lit("PTI"), F.lit("MIC"), F.lit("LIB")),
        (F.col("i") % 5 + 1).cast("int")
    ).alias("provider_code"),
    F.col("created_date").alias("quotation_date"),
    F.element_at(
        F.array(
            F.lit("QUOTED"),
            F.lit("ACCEPTED"),
            F.lit("REJECTED"),
            F.lit("EXPIRED"),
            F.lit("CONVERTED")
        ),
        (F.col("i") % 5 + 1).cast("int")
    ).alias("quotation_status"),
    F.element_at(
        F.array(F.lit("BASIC"), F.lit("STANDARD"), F.lit("PREMIUM"), F.lit("VIP")),
        (F.col("i") % 4 + 1).cast("int")
    ).alias("package_code"),

    # 3M - 30M
    (
        F.rand(seed=43) * 27000000 + 3000000
    ).cast("decimal(18,2)").alias("premium_amount"),

    F.expr("timestampadd(DAY, 30, created_date)").alias("quotation_expiry_date"),
    F.col("created_date"),
    F.col("created_date").alias("updated_date")
)

quotation_item_df = base_df.select(
    F.format_string("QI%05d", F.col("i")).alias("quotation_item_id"),
    F.format_string("QUO%05d", F.col("i")).alias("quotation_id"),
    F.element_at(
        F.array(
            F.lit("Physical Damage"),
            F.lit("Theft"),
            F.lit("Third Party Liability"),
            F.lit("Personal Accident")
        ),
        (F.col("i") % 4 + 1).cast("int")
    ).alias("coverage_type"),

    # 100M - 2B
    (
        F.rand(seed=44) * 1900000000 + 100000000
    ).cast("decimal(18,2)").alias("coverage_amount"),

    F.lit(1000000).cast("decimal(18,2)").alias("deductible_amount"),
    F.col("created_date"),
    F.col("created_date").alias("updated_date")
)


# =========================
# Write data
# =========================

customer_df.write.mode("append").saveAsTable("dbo.customers")
vehicle_df.write.mode("append").saveAsTable("dbo.vehicle")
quotation_df.write.mode("append").saveAsTable("dbo.quotation")
quotation_item_df.write.mode("append").saveAsTable("dbo.quotation_item")


# =========================
# Check row counts
# =========================

display(spark.sql("""
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM dbo.customers
UNION ALL SELECT 'agents', COUNT(*) FROM dbo.agents
UNION ALL SELECT 'insurance_providers', COUNT(*) FROM dbo.insurance_providers
UNION ALL SELECT 'vehicle', COUNT(*) FROM dbo.vehicle
UNION ALL SELECT 'quotation', COUNT(*) FROM dbo.quotation
UNION ALL SELECT 'quotation_item', COUNT(*) FROM dbo.quotation_item
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
