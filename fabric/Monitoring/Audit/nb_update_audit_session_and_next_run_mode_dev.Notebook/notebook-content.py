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

# PARAMETERS CELL ********************

session_id = ""
batch_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("The 'batch_id' parameter must be provided as a non-empty integer.")
else:
    batch_id = int(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
    session_finished = current_timestamp(),
    updated_at = current_timestamp()
WHERE id = '{session_id}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
