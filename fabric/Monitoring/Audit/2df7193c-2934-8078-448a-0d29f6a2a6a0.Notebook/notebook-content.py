# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "44c157dd-49ca-46c5-896a-9cb48544300e",
# META       "default_lakehouse_name": "audit_lakehouse_test",
# META       "default_lakehouse_workspace_id": "e1832509-bd92-47cc-be34-c5e939a6456a",
# META       "known_lakehouses": [
# META         {
# META           "id": "44c157dd-49ca-46c5-896a-9cb48544300e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql import functions as F

batch_id = 2004
source_table = "test_crm_customer_source"
bronze_table = "bronze_customer"
silver_table = "silver_customer"
gold_table = "gold_dim_customer"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_source_rows = [
    Row(customer_id=6, customer_name="An4", email="an4@test.com"),
    Row(customer_id=7, customer_name="Binh4", email="binh4@test.com"),
    Row(customer_id=8, customer_name="Chi4", email="chi4@test.com"),
    Row(customer_id=9, customer_name="Dung4", email="dung4@test.com"),
    Row(customer_id=10, customer_name=None, email="invalid4@test.com"),
]

spark.createDataFrame(customer_source_rows).write.format("delta").mode("overwrite").saveAsTable(source_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_bronze_df = (
    spark.table(source_table)
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_loaded_at", F.current_timestamp())
    .withColumn("_source_system", F.lit("CRM"))
    .withColumn("_source_name", F.lit("customer"))
    .withColumn("_source_file", F.lit("Files/landing/crm_system/customer/test_customer_file.json"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_bronze_batch_df = spark.table(bronze_table).where(F.col("_batch_id") == F.lit(batch_id))
customer_silver_df = customer_bronze_batch_df.where(F.col("customer_name").isNotNull())
invalid_customer_df = customer_bronze_batch_df.where(F.col("customer_name").isNull())

customer_silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

rejected_row_count = invalid_customer_df.count()
rejected_count = rejected_row_count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_gold_df = spark.table(silver_table).select(
    "customer_id",
    "customer_name",
    "email",
    "_batch_id",
    "_loaded_at",
)

customer_gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print({
    "batch_id": batch_id,
    "source_table": source_table,
    "bronze_table": bronze_table,
    "silver_table": silver_table,
    "gold_table": gold_table,
    "rejected_row_count": rejected_row_count,
})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
