# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf1b63ae-986e-4368-a13e-ed5eed5fd990",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "82a15c8e-ce8d-4f2c-827e-94b17659ecd8",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf1b63ae-986e-4368-a13e-ed5eed5fd990"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Gold Layer audit logging override helper
# This notebook overrides start_table_layer, finish_table_layer, and should_process_table_layer
# to prevent conformed tables from interacting with/writing to log.audit_table_session.

import uuid

def start_table_layer(session_id, source_table_id, source_table_name, layer, batch_id, **kwargs):
    dummy_id = str(uuid.uuid4())
    print(f"[BYPASS AUDIT] start_table_layer bypassed for GOLD table: {source_table_name}")
    return dummy_id

def finish_table_layer(table_session_id, layer, status, **kwargs):
    print(f"[BYPASS AUDIT] finish_table_layer bypassed for GOLD table session: {table_session_id}, status: {status}")
    return

def should_process_table_layer(batch_id, source_table_id, layer, **kwargs):
    # Idempotent Delta updates mean we can always run the Gold notebook tables on recovery,
    # and we skip query overhead on normal runs.
    return True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
