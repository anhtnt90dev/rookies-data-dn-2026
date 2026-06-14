# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a667e77c-0848-4e2e-90dc-502057b719c0",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "fe74f781-d77f-46e7-accd-2e57689ef181",
# META       "known_lakehouses": [
# META         {
# META           "id": "a667e77c-0848-4e2e-90dc-502057b719c0"
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
