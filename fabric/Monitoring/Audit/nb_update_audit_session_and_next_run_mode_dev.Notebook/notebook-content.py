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
