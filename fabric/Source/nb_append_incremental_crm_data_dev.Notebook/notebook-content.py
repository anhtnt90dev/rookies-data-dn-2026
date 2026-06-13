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

from pyspark.sql import Row
from pyspark.sql import functions as F
from datetime import datetime, date, timedelta
from decimal import Decimal

# =========================
# Incremental mock data
# From 2025-05-26 to 2025-06-06
# =========================

incremental_start_date = datetime(2025, 5, 26, 0, 0, 0)
incremental_end_date = datetime(2025, 6, 6, 12, 0, 0)

# Number of new records
new_record_count = 200

# Get current record count
current_customer_count = spark.table("dbo.customers").count()

customers = []
vehicles = []
quotations = []
quotation_items = []

total_days = (incremental_end_date - incremental_start_date).days + 1

for offset in range(1, new_record_count + 1):
    i = current_customer_count + offset

    customer_id = f"CUS{i:04d}"
    vehicle_id = f"VEH{i:04d}"
    quotation_id = f"QUO{i:05d}"
    quotation_item_id = f"QI{i:05d}"

    created_ts = incremental_start_date + timedelta(
        days=(offset - 1) % total_days,
        hours=offset % 24
    )

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
