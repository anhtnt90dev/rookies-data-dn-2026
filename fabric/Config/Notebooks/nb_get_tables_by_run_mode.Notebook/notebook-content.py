# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59e55d5a-c0cc-429c-8dcb-068cfbec22d2",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "d7a45747-6b09-483f-b813-8aee84a3afc6",
# META       "known_lakehouses": [
# META         {
# META           "id": "59e55d5a-c0cc-429c-8dcb-068cfbec22d2"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

p_run_mode="NEW"
p_previous_session_id=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_mode = p_run_mode.upper()
RECOVERY_MODE="RECOVERY"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set(
    "spark.sql.execution.arrow.pyspark.enabled",
    "true"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE_COLUMNS = [
    "id",
    "source_system",
    "source_type",
    "source_name",
    "source_location",
    "source_format",
    "delimiter",
    "load_type",
    "primary_key",
    "source_to_bronze_mapping_path",
    "bronze_to_silver_mapping_path",
    "silver_transform_name",
    "watermark_column",
    "bronze_table_name",
    "silver_table_name",
    "load_sequence",
    "is_active",
    "created_at",
    "updated_at"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_source_columns(alias="s"):
    """
    Return qualified source columns to avoid ambiguous column issue after join.
    """
    return [
        F.col(f"{alias}.{column}").alias(column)
        for column in SOURCE_COLUMNS
    ]


def dataframe_to_records(df):
    """
    Convert Spark DataFrame to Python list of dict.
    """
    return (
        df
        .toPandas()
        .to_dict("records")
    )


def get_new_run_mode():
    """
    Load all active source tables for NEW execution mode.
    """

    df = (
        spark.table("cfg.source_table")
        .alias("s")
        .filter(F.col("s.is_active") == True)
        .select(*get_source_columns("s"))
        .orderBy("load_sequence")
    )

    return dataframe_to_records(df)



def load_recovery_tables(previous_session_id):
    """
    Load failed Silver tables from previous failed session.
    """

    audit_df = (
        spark.table("log.audit_table_session")
        .alias("l")
        .filter(
            (F.col("l.session_id") == previous_session_id) &
            (F.col("l.silver_status") == "FAILED")
        )
        .select(
            F.col("l.source_table_id")
        )
    )


    source_df = (
        spark.table("cfg.source_table")
        .alias("s")
    )


    result_df = (
        source_df
        .join(
            audit_df,
            F.col("s.id") == F.col("source_table_id"),
            "inner"
        )
        .select(*get_source_columns("s"))
        .orderBy("load_sequence")
    )


    return dataframe_to_records(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



if run_mode == RECOVERY_MODE:
    if not p_previous_session_id:
        raise ValueError("p_previous_session_id is required when run_mode is RECOVERY")
    output = load_recovery_tables(p_previous_session_id)
else:
    output = get_new_run_mode()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(
    json.dumps(output, default=str)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
