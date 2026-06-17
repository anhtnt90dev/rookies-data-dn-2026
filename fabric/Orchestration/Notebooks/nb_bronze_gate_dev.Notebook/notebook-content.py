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

result = ""
run_mode = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if run_mode =="NEW" and result == "NO_DATA":
    raise Exception(
       "Bronze validation failed: no data was loaded during NEW execution."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
