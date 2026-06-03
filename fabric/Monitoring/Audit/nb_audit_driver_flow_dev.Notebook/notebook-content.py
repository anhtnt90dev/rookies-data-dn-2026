# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_runner.run("nb_audit_logging_helper_dev", 300)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_runner.run("nb_audit_row_count_dev", 300)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_runner.run("nb_audit_row_count_simulation_setup_dev", 300)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_runner.run("nb_audit_row_count_simulation_run_dev", 300)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optional monitoring views after the audit data exists.
notebook_runner.run("nb_etl_monitoring_report_dev", 300)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
