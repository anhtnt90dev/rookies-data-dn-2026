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

run_mode = ""
batch_id = ""
pipeline_run_id = ""
previous_session_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark.catalog.currentDatabase())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
import uuid
from enum import Enum
from datetime import datetime
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class AuditStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class RunMode(str, Enum):
    NEW = "NEW"
    RECOVERY = "RECOVERY"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALID_TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

AUDIT_SESSION_TABLE = "log.audit_session"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def enum_value(value):
    if isinstance(value, Enum):
        return value.value
    return str(value).upper()


def validate_table_name(table_name: str) -> str:
    if not VALID_TABLE_NAME_PATTERN.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def require_status(status: str, allowed_statuses) -> str:
    status_value = enum_value(status)
    allowed_values = {enum_value(item) for item in allowed_statuses}

    if status_value not in allowed_values:
        raise ValueError(
            f"status must be one of: {', '.join(sorted(allowed_values))}"
        )

    return status_value


def new_audit_id() -> str:
    return str(uuid.uuid4())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def start_pipeline_session(
    pipeline_name: str,
    pipeline_run_id: str,
    batch_id: int,
    run_mode: str = RunMode.NEW,
    sla_target_ms: int = None,
    audit_session_table: str = AUDIT_SESSION_TABLE,
) -> str:

    audit_session_table = validate_table_name(audit_session_table)

    run_mode = require_status(
        run_mode,
        [RunMode.NEW, RunMode.RECOVERY]
    )

    session_id = new_audit_id()
    
    query = f"""
        INSERT INTO {audit_session_table}
        VALUES (
            '{session_id}',
            '{AuditStatus.RUNNING.value}',
            '{run_mode}',
            {int(batch_id)},
            '{pipeline_name}',
            '{pipeline_run_id}',
            current_timestamp(),
            NULL,
            NULL,
            {sla_target_ms if sla_target_ms is not None else 'NULL'},
            NULL,
            current_timestamp(),
            current_timestamp()
        )
    """

    spark.sql(query)

    print(
        f"Started pipeline session: "
        f"session_id={session_id}, "
        f"pipeline_run_id={pipeline_run_id}"
    )

    return session_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_mode = RunMode.NEW if run_mode == "NEW" else RunMode.RECOVERY
batch_id = int(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

session_id = start_pipeline_session(
    pipeline_name="pl_master_etl",
    pipeline_run_id=pipeline_run_id,
    batch_id=batch_id,
    run_mode=run_mode
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


result = {
    "batch_id": batch_id,
    "session_id": session_id,
    "run_mode": run_mode,
    "previous_session_id" : previous_session_id
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
