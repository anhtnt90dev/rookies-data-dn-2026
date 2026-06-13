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

# PARAMETERS CELL ********************

session_id = ""
batch_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_id = int(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

error_message = "Bronze layer failed. Pipeline will run in RECOVERY mode next time."

safe_error = error_message.replace("'", "''")

spark.sql(f"""
    UPDATE cfg.next_run_mode
    SET next_run_mode = 'RECOVERY',
        batch_id = {batch_id},
        session_id = '{session_id}',
        updated_at = current_timestamp()
    """
)

spark.sql(f"""
UPDATE log.audit_session
SET session_status = 'FAILED',
    error_code = 'BRONZE_LAYER_FAILED',
    error_message = '{safe_error}',
    session_finished = current_timestamp(),
    updated_at = current_timestamp()
WHERE id = '{session_id}'
""")

raise Exception(error_message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
