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

# Driver flow for pipeline audit logging validation.
# Run this notebook after attaching the target Lakehouse.
try:
    notebook_runner = notebookutils.notebook
except NameError:
    from notebookutils import notebook as notebook_runner


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_runner.run("nb_audit_pipeline_log_dev", 300)
notebook_runner.run("nb_audit_row_count_simulation_setup_dev", 300)
notebook_runner.run("nb_audit_row_count_simulation_run_dev", 300)
notebook_runner.run("nb_etl_monitoring_report_dev", 300)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
