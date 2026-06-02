# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

batch_id = 2001

customer_source_rows = [
    Row(customer_id=1, customer_name="An", email="an@test.com"),
    Row(customer_id=2, customer_name="Binh", email="binh@test.com"),
    Row(customer_id=3, customer_name="Chi", email="chi@test.com"),
    Row(customer_id=4, customer_name="Dung", email="dung@test.com"),
    Row(customer_id=5, customer_name=None, email="invalid@test.com")
]

spark.createDataFrame(customer_source_rows).write.mode("overwrite").saveAsTable("test_crm_customer_source")

customer_bronze_df = (
    spark.table("test_crm_customer_source")
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_loaded_at", current_timestamp())
    .withColumn("_source_system", lit("CRM"))
    .withColumn("_source_name", lit("customer"))
)

customer_bronze_df.write.mode("overwrite").saveAsTable("bronze_customer")

customer_bronze_batch_df = spark.table("bronze_customer").where(f"_batch_id = {batch_id}")

customer_silver_df = customer_bronze_batch_df.where("customer_name IS NOT NULL")
invalid_customer_df = customer_bronze_batch_df.where("customer_name IS NULL")

customer_silver_df.write.mode("overwrite").saveAsTable("silver_customer")

rejected_row_count = invalid_customer_df.count()
rejected_count = rejected_row_count  # Backward-compatible alias for the original simulation variable.

customer_gold_df = spark.table("silver_customer").select(
    "customer_id",
    "customer_name",
    "email",
    "_batch_id",
    "_loaded_at"
)

customer_gold_df.write.mode("overwrite").saveAsTable("gold_dim_customer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
