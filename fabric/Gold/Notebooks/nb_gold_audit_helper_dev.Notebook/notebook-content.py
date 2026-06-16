# Fabric notebook source

# METADATA ********************

# META {
#   "kernel_info": {
#     "name": "synapse_pyspark"
#   }
# }

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
