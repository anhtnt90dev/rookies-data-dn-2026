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

import uuid

# Disable auto broadcast join to prevent 8GB limit issues on large tables (e.g. 100M records)
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("[GOLD AUDIT HELPER] Disabled autoBroadcastJoinThreshold successfully.")
except Exception as e:
    print(f"[GOLD AUDIT HELPER WARNING] Failed to set autoBroadcastJoinThreshold: {e}")

def start_table_layer(session_id, source_table_id, source_table_name, layer, batch_id, **kwargs):
    from pyspark.sql import functions as F
    
    print(f"[GOLD AUDIT HELPER] start_table_layer called for GOLD table: {source_table_name} (ID: {source_table_id})")
    
    # 1. Resolve source_table_id mapped to this conformed table
    try:
        df_tables = spark.table("cfg.dim_fact_table").select("id", "table_name").collect()
        mappings = spark.table("cfg.source_dim_fact").select("dim_fact_table_id", "source_table_id").collect()
        src_tables = spark.table("cfg.source_table").select("id", "source_name").collect()
        
        df_names = {row["id"]: row["table_name"] for row in df_tables}
        src_names = {row["id"]: row["source_name"] for row in src_tables}
        
        from collections import defaultdict
        df_to_srcs = defaultdict(list)
        for row in mappings:
            df_id = int(row["dim_fact_table_id"])
            src_id = int(row["source_table_id"])
            df_to_srcs[df_id].append(src_id)
            
        dim_fact_to_source = {}
        for df_id, src_ids in df_to_srcs.items():
            df_name = df_names.get(df_id, "").lower()
            matched_id = None
            for s_id in src_ids:
                s_name = src_names.get(s_id, "").lower()
                s_norm = s_name[:-1] if s_name.endswith('s') else s_name
                if s_norm in df_name:
                    if matched_id is None or len(s_norm) > len(src_names.get(matched_id, "")):
                        matched_id = s_id
            dim_fact_to_source[df_id] = matched_id if matched_id is not None else src_ids[0]

        source_id = dim_fact_to_source.get(int(source_table_id))
    except Exception as e:
        print(f"[GOLD AUDIT HELPER WARNING] Failed to resolve source mapping: {e}")
        source_id = None

    # 2. Look up the active session in log.audit_table_session
    if source_id:
        try:
            rows = spark.table("log.audit_table_session") \
                .where((F.col("batch_id") == F.lit(int(batch_id))) & (F.col("source_table_id") == F.lit(source_id))) \
                .orderBy(F.col("created_at").desc()) \
                .select("id") \
                .limit(1) \
                .collect()
            if rows:
                real_session_id = rows[0]["id"]
                print(f"[GOLD AUDIT HELPER] Found active source table session ID: {real_session_id} for source table ID: {source_id}")
                return real_session_id
        except Exception as e:
            print(f"[GOLD AUDIT HELPER WARNING] Failed to lookup session ID from log.audit_table_session: {e}")

    # Fallback to dummy UUID if mapping/lookup fails
    dummy_id = f"dummy_{uuid.uuid4()}"
    print(f"[GOLD AUDIT HELPER] Bypassing audit, using dummy ID: {dummy_id}")
    return dummy_id

def finish_table_layer(table_session_id, layer, status, **kwargs):
    print(f"[GOLD AUDIT HELPER] finish_table_layer called for GOLD table session: {table_session_id}, status: {status}")
    
    if not table_session_id or str(table_session_id).startswith("dummy_"):
        print(f"[GOLD AUDIT HELPER] Bypassing finish_table_layer because of dummy session ID: {table_session_id}")
        return
        
    try:
        from pyspark.sql import Row
        from pyspark.sql import functions as F
        
        source_row_count = kwargs.get("source_row_count", 0)
        target_row_count = kwargs.get("target_row_count", 0)
        inserted_row = kwargs.get("inserted_row", 0)
        updated_row = kwargs.get("updated_row", 0)
        deleted_row = kwargs.get("deleted_row", 0)
        rejected_row = kwargs.get("rejected_row", 0)
        error_message = kwargs.get("error_message", None)
        error_type = kwargs.get("error_type", None)
        is_retryable = kwargs.get("is_retryable", None)
        
        attempt_no = 1
        if "get_next_attempt_no" in globals():
            try:
                attempt_no = globals()["get_next_attempt_no"]("log.audit_detail", str(table_session_id), layer)
            except Exception as inner_e:
                print(f"[GOLD AUDIT HELPER WARNING] get_next_attempt_no failed, fallback to 1: {inner_e}")
                
        row_values = {
            "id": globals()["new_audit_id"]() if "new_audit_id" in globals() else str(uuid.uuid4()),
            "table_session_id": str(table_session_id),
            "attempt_no": attempt_no,
            "detail_status": status,
            "layer": layer,
            "watermark_before": None,
            "watermark_after": None,
            "load_window_start": None,
            "load_window_end": None,
            "source_row_count": int(source_row_count) if source_row_count is not None else 0,
            "target_row_count": int(target_row_count) if target_row_count is not None else 0,
            "inserted_row": int(inserted_row) if inserted_row is not None else 0,
            "updated_row": int(updated_row) if updated_row is not None else 0,
            "deleted_row": int(deleted_row) if deleted_row is not None else 0,
            "rejected_row": int(rejected_row) if rejected_row is not None else 0,
            "error_message": str(error_message) if error_message is not None else None,
            "error_type": str(error_type) if error_type is not None else None,
            "is_retryable": bool(is_retryable) if is_retryable is not None else None,
            "duration_ms": None,
            "sla_target_ms": None,
            "sla_breached": None
        }
        
        if "append_audit_detail" in globals():
            globals()["append_audit_detail"](row_values, "log.audit_detail")
        else:
            schema = globals()["AUDIT_DETAIL_SCHEMA"] if "AUDIT_DETAIL_SCHEMA" in globals() else None
            if schema:
                detail_df = spark.createDataFrame([Row(**row_values)], schema)
            else:
                detail_df = spark.createDataFrame([row_values])
            detail_df = detail_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())
            detail_df.write.format("delta").mode("append").saveAsTable("log.audit_detail")
            
        print(f"[GOLD AUDIT HELPER] Successfully appended audit detail for session: {table_session_id}")
    except Exception as e:
        print(f"[GOLD AUDIT HELPER ERROR] Failed to write to log.audit_detail: {e}")


def should_process_table_layer(batch_id, source_table_id, layer, **kwargs):
    # Idempotent Delta updates mean we can always run the Gold notebook tables on recovery,
    # and we skip query overhead on normal runs.
    return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
