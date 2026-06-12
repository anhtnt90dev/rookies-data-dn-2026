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
layer = ""

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

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_next_run_mode_recovery(batch_id: int, session_id: str):
    spark.sql(f"""
        UPDATE cfg.next_run_mode
        SET next_run_mode = 'RECOVERY',
            batch_id = {batch_id},
            session_id = '{session_id}',
            updated_at = current_timestamp()
    """)


def update_audit_session_failed(session_id: str, error_message: str):
    safe_error = str(error_message).replace("'", "''")

    spark.sql(f"""
        UPDATE log.audit_session
        SET session_status = 'FAILED',
            error_code = 'BRONZE_GATE_FAILED',
            error_message = '{safe_error}',
            session_finished = current_timestamp(),
            updated_at = current_timestamp()
        WHERE id = '{session_id}'
    """)


audit_df = (
    spark.table("log.audit_table_session")
    .where(
        (F.col("session_id") == F.lit(session_id))
        & (F.col("batch_id") == F.lit(batch_id))
    )
)

invalid_df = audit_df.where(
    ~F.col("bronze_status").isin("SUCCESS", "SKIPPED")
)

invalid_count = invalid_df.count()

if invalid_count > 0:
    error_message = (
        f"{layer} gate failed. "
        f"{invalid_count} table(s) are not SUCCESS/SKIPPED."
    )

    update_next_run_mode_recovery(batch_id, session_id)
    update_audit_session_failed(session_id, error_message)

    raise Exception(error_message)

print(f"{layer} gate passed. All tables are SUCCESS or SKIPPED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
