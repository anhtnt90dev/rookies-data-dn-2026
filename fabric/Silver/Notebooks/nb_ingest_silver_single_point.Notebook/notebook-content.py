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

import json
import os
import hashlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from delta.tables import DeltaTable

from pyspark.sql import Row
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)




import re
import time
import uuid
from enum import Enum
from dataclasses import dataclass



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_pipeline_run_id = "14a8ca6f-0b1b-412b-b8eb"
p_session_id= "258bd566-6072-4fe9-9721-7a5066e0dc77"
p_batch_id="20260617130422"
p_list_config_load_table = "[{\"id\":1,\"source_system\":\"crm_system\",\"source_type\":\"database\",\"source_name\":\"customers\",\"source_location\":\"dbo.customers\",\"source_format\":\"table\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"customer_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/customer.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/customer.json\",\"silver_transform_name\":null,\"watermark_column\":\"updated_date\",\"bronze_table_name\":\"bronze.customer\",\"silver_table_name\":\"silver.customer\",\"load_sequence\":1,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":2,\"source_system\":\"crm_system\",\"source_type\":\"database\",\"source_name\":\"agents\",\"source_location\":\"dbo.agents\",\"source_format\":\"table\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"agent_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/agent.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/agent.json\",\"silver_transform_name\":null,\"watermark_column\":\"updated_date\",\"bronze_table_name\":\"bronze.agent\",\"silver_table_name\":\"silver.agent\",\"load_sequence\":2,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":3,\"source_system\":\"crm_system\",\"source_type\":\"database\",\"source_name\":\"insurance_providers\",\"source_location\":\"dbo.insurance_providers\",\"source_format\":\"table\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"provider_code\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/insurance_provider.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/provider.json\",\"silver_transform_name\":null,\"watermark_column\":\"updated_date\",\"bronze_table_name\":\"bronze.insurance_provider\",\"silver_table_name\":\"silver.provider\",\"load_sequence\":3,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":5,\"source_system\":\"crm_system\",\"source_type\":\"database\",\"source_name\":\"quotation\",\"source_location\":\"dbo.quotation\",\"source_format\":\"table\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"quotation_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/quotation.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/quotation.json\",\"silver_transform_name\":null,\"watermark_column\":\"updated_date\",\"bronze_table_name\":\"bronze.quotation\",\"silver_table_name\":\"silver.quotation\",\"load_sequence\":5,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":6,\"source_system\":\"crm_system\",\"source_type\":\"database\",\"source_name\":\"quotation_item\",\"source_location\":\"dbo.quotation_item\",\"source_format\":\"table\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"quotation_item_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/quotation_item.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/quotation_item.json\",\"silver_transform_name\":null,\"watermark_column\":\"updated_date\",\"bronze_table_name\":\"bronze.quotation_item\",\"silver_table_name\":\"silver.quotation_item\",\"load_sequence\":6,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":7,\"source_system\":\"policy_system\",\"source_type\":\"file\",\"source_name\":\"policy\",\"source_location\":\"Files/landing/policy_system/policy\",\"source_format\":\"json\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"policy_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/policy.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/policy.json\",\"silver_transform_name\":null,\"watermark_column\":\"last_updated\",\"bronze_table_name\":\"bronze.policy\",\"silver_table_name\":\"silver.policy\",\"load_sequence\":7,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":8,\"source_system\":\"policy_system\",\"source_type\":\"file\",\"source_name\":\"cancellation\",\"source_location\":\"Files/landing/policy_system/cancellation\",\"source_format\":\"json\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"cancellation_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/cancellation.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/cancellation.json\",\"silver_transform_name\":null,\"watermark_column\":\"last_updated\",\"bronze_table_name\":\"bronze.cancellation\",\"silver_table_name\":\"silver.cancellation\",\"load_sequence\":8,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"},{\"id\":9,\"source_system\":\"payment_system\",\"source_type\":\"file\",\"source_name\":\"payment\",\"source_location\":\"Files/landing/payment_system/payment\",\"source_format\":\"json\",\"delimiter\":null,\"load_type\":\"FULL\",\"primary_key\":\"payment_id\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/payment.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/payment.json\",\"silver_transform_name\":null,\"watermark_column\":\"last_updated\",\"bronze_table_name\":\"bronze.payment\",\"silver_table_name\":\"silver.payment\",\"load_sequence\":9,\"is_active\":true,\"created_at\":\"2026-06-17T12:52:57.087947\",\"updated_at\":\"2026-06-17T12:52:57.087947\"}]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

session_id = p_session_id
pipeline_run_id = p_pipeline_run_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# json_string = json.dumps(p_list_config_load_table)
# print(json_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@dataclass(frozen=True)
class LayerColumns:
    started: str
    ended: str
    status: str

class AuditStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    NOT_RUN = "NOT_RUN"
    CANCELLED = "CANCELLED"

class ErrorType(str, Enum):
    SYSTEM = "SYSTEM"
    DATA = "DATA"
    RULE = "RULE"
    CONFIG = "CONFIG"
    UNKNOWN = "UNKNOWN"

class Layer(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"

LAYER_COLUMN_MAP: dict[Layer, LayerColumns] = {
    Layer.BRONZE: LayerColumns(
        started="bronze_started_at",
        ended="bronze_ended_at",
        status="bronze_status",
    ),
    Layer.SILVER: LayerColumns(
        started="silver_started_at",
        ended="silver_ended_at",
        status="silver_status",
    ),
    Layer.GOLD: LayerColumns(
        started="gold_started_at",
        ended="gold_ended_at",
        status="gold_status",
    ),
}


AUDIT_TABLE_SESSION_TABLE = "log.audit_table_session"
AUDIT_DETAIL_TABLE = "log.audit_detail"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def require_layer(layer: str) -> str:
    layer_value = enum_value(layer)
    if layer_value not in {item.value for item in Layer}:
        raise ValueError("layer must be BRONZE, SILVER, or GOLD")
    return layer_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_table_layer_status_batch(
    session_id: str,
    layer: Layer,
    table_results: list[dict],
    audit_table: str = AUDIT_TABLE_SESSION_TABLE,
) -> dict:
    """
    Batch update audit table layer status for all ingested tables.
    Returns: dict[source_table_id -> table_session_id]
    """

    if not table_results:
        print("[AUDIT] No table results to update")
        return {}


    if isinstance(layer, str):
        try:
            layer = Layer(layer)
        except Exception:
            raise ValueError(f"Invalid layer: {layer}")


    layer_columns = LAYER_COLUMN_MAP[layer]

    layer_started_column = layer_columns.started
    layer_ended_column = layer_columns.ended
    layer_status_column = layer_columns.status


    schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("source_table_id", StringType(), True),
        StructField("source_table_name", StringType(), True),
        StructField("table_session_status", StringType(), True),
        StructField("layer_status", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True)
    ])

    records = []
    for r in table_results:
        records.append({
            "session_id": str(session_id),
            "source_table_id": str(r["source_table_id"]),
            "source_table_name": r.get("bronze_table_name"),
            "table_session_status": AuditStatus.RUNNING.value,
            "layer_status": AuditStatus.RUNNING.value,
            "load_type": r.get("load_type", "FULL"),
            "error_code": None,
            "error_message": None,
        })

    df = spark.createDataFrame(records, schema=schema)

    # ----------------------------
    # DELTA TABLE
    # ----------------------------
    delta_table = DeltaTable.forName(spark, audit_table)

    # ----------------------------
    # MERGE BATCH UPDATE
    # ----------------------------
    (
        delta_table.alias("t")
        .merge(
            df.alias("s"),
            """
            t.session_id = s.session_id
            AND t.source_table_id = s.source_table_id
            """
        )
        .whenMatchedUpdate(set={
            "table_session_status": F.lit(AuditStatus.RUNNING.value),
            layer_status_column: F.lit(AuditStatus.RUNNING.value),
            layer_started_column: F.current_timestamp(),
            layer_ended_column: F.lit(None),
            "error_code": F.lit(None),
            "error_message": F.lit(None),
            "load_type": F.col("s.load_type"),
            "updated_at": F.current_timestamp(),
        })
        .execute()
    )

    result_map = {
        r["source_table_id"]: None
        for r in table_results
    }

    print(f"[AUDIT] Batch updated {len(table_results)} tables for layer={layer}")

    return result_map

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_raw_config = json.loads(p_list_config_load_table)

config_load_tables: list[dict] = (
    _raw_config if isinstance(_raw_config, list) else [_raw_config]
)

batch_id = p_batch_id

print(f"[SETUP] Received {len(config_load_tables)} table(s) to process in this run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pipeline session identifier for audit tracing
session_id: str = ""

# Environment tag: 'dev' | 'staging' | 'prod'
run_env: str = "dev"

# Override load type at runtime: 'full' | 'incremental' | '' (use cfg value)
force_load_type: str = "incremental"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

# Config / metadata tables
SOURCE_CONFIG_TABLE: str = "cfg.source_tables"
NEXT_RUN_MODE_TABLE: str = "cfg.next_run_mode"
AUDIT_TABLE_SESSION: str = "log.audit_table_session"
INVALID_RECORD_TABLE = "log.invalid_record"

# Audit helper notebook (imported via %run or notebookutils)
AUDIT_LOGGING_HELPER_NOTEBOOK: str = "nb_audit_logging_helper_dev"

# Quarantine schema prefix for rejected rows
QUARANTINE_SCHEMA: str = "silver_quarantine"

# System metadata column added to every Silver row
INGESTION_TIMESTAMP_COLUMN: str = "_loaded_at"

# DQ failure reason column added to rejected rows
DQ_FAILURE_REASON_COLUMN: str = "__dq_failure_reason"

# Audit layer tag
LAYER_SILVER: str = "SILVER"

# Pipeline status constants
STATUS_SUCCESS: str = "SUCCESS"
STATUS_WARNING: str = "SKIPPED"
STATUS_FAILED: str = "FAILED"

# Run mode constants (from cfg.next_run_mode)
RUN_MODE_NEW: str = "NEW"
RUN_MODE_RECOVERY: str = "RECOVERY"

# Load type constants
LOAD_TYPE_FULL: str = "full"
LOAD_TYPE_INCREMENTAL: str = "incremental"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_mapping_json(mapping_path: str) -> dict:
    """
    Load and validate a Bronze-to-Silver column mapping JSON file.

    The mapping file must contain the keys: source_table, target_table, columns.
    Each entry in 'columns' must have 'target' and 'expression' keys.

    Parameters
    ----------
    mapping_path : str
        File path to the mapping JSON (Lakehouse path or ABFSS path).

    Returns
    -------
    dict
        Parsed mapping object.

    Raises
    ------
    ValueError
        If the mapping file is missing, malformed, or missing required keys.
    """
    required_keys = {"source_table", "target_table", "columns"}

    # Always prefix with lakehouse/default
    prefixed_path = os.path.join("/lakehouse/default", mapping_path)

    try:
        with open(prefixed_path, "r", encoding="utf-8") as mapping_file:
            mapping: dict = json.load(mapping_file)
    except FileNotFoundError:
        raise ValueError(f"[MAPPING] Mapping file not found: '{prefixed_path}'")
    except json.JSONDecodeError as json_error:
        raise ValueError(
            f"[MAPPING] Mapping file is malformed JSON: '{prefixed_path}' — {json_error}"
        )

    missing_keys = required_keys - mapping.keys()
    if missing_keys:
        raise ValueError(
            f"[MAPPING] Mapping file '{prefixed_path}' is missing required keys: {missing_keys}"
        )

    # print(
    #     f"[MAPPING] Loaded mapping | source={mapping['source_table']} "
    #     f"| target={mapping['target_table']} "
    #     f"| columns={len(mapping['columns'])}"
    # )
    return mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_bronze_table(
    source_table_name: str,
    load_type: str,
    batch_id: int | str | None = None,
) -> DataFrame:
    """
    Read the Bronze Delta table as a Spark DataFrame.

    For FULL load, returns all rows.
    For INCREMENTAL load, filters rows where _batch_id == batch_id.

    Parameters
    ----------
    source_table_name : str
        Fully qualified Bronze table name, e.g. 'bronze.agent'.
    load_type : str
        'full' or 'incremental'.
    batch_id : int or str or None
        The batch identifier for the current run.

    Returns
    -------
    DataFrame
        Raw Bronze DataFrame with no transformations applied.

    Raises
    ------
    AnalysisException
        Re-raised if the Bronze table does not exist.
    ValueError
        If incremental load is requested but batch_id is not provided.
    """
    try:
        bronze_df: DataFrame = spark.table(source_table_name)
    except AnalysisException as analysis_error:
        raise AnalysisException(
            f"[BRONZE] Table '{source_table_name}' does not exist or is not accessible: "
            f"{analysis_error}"
        )

    if load_type.lower() == LOAD_TYPE_INCREMENTAL:
        if not batch_id:
            raise ValueError(
                f"[BRONZE] Incremental load requires 'batch_id' "
                f"but none was provided for table '{source_table_name}'."
            )
        
        bronze_df = bronze_df.filter(
            F.col("_batch_id") == F.lit(str(batch_id))
        )
        print(
            f"[BRONZE] Incremental filter applied: _batch_id == {batch_id}"
        )

    source_row_count: int = bronze_df.count()
    # print(
    #     f"[BRONZE] Read {source_row_count:,} row(s) from '{source_table_name}' "
    #     f"(load_type={load_type})"
    # )
    return bronze_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def apply_column_transformations(
    bronze_df: DataFrame,
    column_mappings: list[dict],
    source_table_name: str,
) -> DataFrame:
    """
    Build the Silver DataFrame by selecting and transforming columns from
    Bronze based on the JSON mapping.

    Handles three expression types:
      1. Direct column reference  — expression == an existing Bronze column name
      2. SQL expression           — expression contains a SQL function/expression string
      3. Null expression          — expression is None (column is left as literal NULL;
                                    downstream validation will flag if required)

    Parameters
    ----------
    bronze_df : DataFrame
        Raw Bronze DataFrame (output of read_bronze_table).
    column_mappings : list[dict]
        List of {'target': str, 'expression': str | None} from mapping JSON.
    source_table_name : str
        Used in error messages for clarity.

    Returns
    -------
    DataFrame
        Mapped Silver DataFrame with only target columns, in mapping order.

    Raises
    ------
    ValueError
        If an expression references a column that does not exist in Bronze.
    """
    bronze_column_names: set[str] = set(bronze_df.columns)
    select_expressions: list = []
    mapped_columns: list[str] = []
    null_columns: list[str] = []

    for mapping_entry in column_mappings:
        target_column: str = mapping_entry["target"]
        expression: str | None = mapping_entry.get("expression")

        if expression is None:
            # Null expression → literal NULL; handled in DQ or downstream
            select_expressions.append(F.lit(None).cast(StringType()).alias(target_column))
            null_columns.append(target_column)

        elif expression in bronze_column_names:
            # Direct column reference (rename)
            select_expressions.append(F.col(expression).alias(target_column))
            mapped_columns.append(f"{expression} → {target_column}")

        else:
            # SQL expression — validate all referenced columns exist in Bronze
            try:
                select_expressions.append(
                    F.expr(expression).alias(target_column)
                )
                mapped_columns.append(f"expr('{expression}') → {target_column}")
            except Exception as expr_error:
                # Check if it's a column-not-found issue
                raise ValueError(
                    f"[TRANSFORM] Expression '{expression}' for target column '{target_column}' "
                    f"in source '{source_table_name}' is invalid: {expr_error}"
                )

    # Append system metadata column
    # select_expressions.append(
    #     F.current_timestamp().alias(INGESTION_TIMESTAMP_COLUMN)
    # )

    mapped_silver_df: DataFrame = bronze_df.select(*select_expressions)

    # Validate that SQL expressions did not silently fail on missing columns
    try:
        mapped_silver_df.schema  # triggers plan analysis
    except AnalysisException as analysis_error:
        raise ValueError(
            f"[TRANSFORM] Column resolution failed for '{source_table_name}': {analysis_error}"
        )

    # print(f"[TRANSFORM] Mapped columns   : {mapped_columns}")
    # print(f"[TRANSFORM] Null-expression  : {null_columns}")
    return mapped_silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def compute_row_hash(input_df: DataFrame, exclude_columns: list[str] | None = None) -> DataFrame:
    """
    Add a '__row_hash' column to the DataFrame by hashing all data columns
    (excluding system/metadata columns).

    The hash is deterministic and can be compared against the existing Silver
    layer to detect unchanged records.

    Parameters
    ----------
    input_df : DataFrame
        Input DataFrame to hash.
    exclude_columns : list[str] or None
        Column names to exclude from the hash (e.g. system metadata columns).

    Returns
    -------
    DataFrame
        Input DataFrame with an additional '__row_hash' string column.
    """
    default_exclude: set[str] = {INGESTION_TIMESTAMP_COLUMN, "__row_hash"}
    excluded: set[str] = default_exclude | set(exclude_columns or [])

    hash_columns: list[str] = [
        col_name for col_name in input_df.columns if col_name not in excluded
    ]

    # Concatenate all column values as strings separated by '|' then MD5-hash
    hashed_df: DataFrame = input_df.withColumn(
        "__row_hash",
        F.md5(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in hash_columns])
        ),
    )
    return hashed_df

def deduplicate_within_batch(input_df: DataFrame, primary_key_columns: list[str]) -> DataFrame:
    """
    Remove duplicate records within the current batch.

    When multiple rows share the same primary key within the batch, only the
    row with the latest '_loaded_at' (or row order if equal) is kept.

    Parameters
    ----------
    input_df : DataFrame
        Input DataFrame (already hash-enriched).
    primary_key_columns : list[str]
        Primary key columns used for deduplication.

    Returns
    -------
    DataFrame
        Deduplicated DataFrame — one row per primary key.
    """
    from pyspark.sql.window import Window

    dedup_window = Window.partitionBy(*primary_key_columns).orderBy(
        F.col(INGESTION_TIMESTAMP_COLUMN).desc()
    )
    deduped_df: DataFrame = (
        input_df.withColumn("__row_rank", F.row_number().over(dedup_window))
        .filter(F.col("__row_rank") == 1)
        .drop("__row_rank")
    )

    pre_count: int = input_df.count()
    post_count: int = deduped_df.count()
    # print(
    #     f"[DEDUP] Within-batch deduplication: {pre_count:,} → {post_count:,} rows "
    #     f"(removed {pre_count - post_count:,} duplicate(s)) on keys={primary_key_columns}"
    # )
    return deduped_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DQ_FAILURE_REASON_COLUMN: str = "__dq_failure_reason"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _validate_is_not_null(df: DataFrame, column_name: str, **_) -> F.Column:
    """True when column IS NULL (violation)."""
    return F.col(column_name).isNull()

def _validate_is_not_empty(df: DataFrame, column_name: str, **_) -> F.Column:
    """
    True when column is NULL, empty string, or whitespace only (violation).
    """
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == "")
    )


def _validate_max_length(df: DataFrame, column_name: str, max_length: int, **_) -> F.Column:
    """True when string length exceeds max_length (violation)."""
    return F.length(F.col(column_name).cast(StringType())) > F.lit(int(max_length))


def _validate_regex(df: DataFrame, column_name: str, pattern: str, **_) -> F.Column:
    """True when value does NOT match the regex pattern (violation)."""
    return (
        F.col(column_name).isNull()
        | ~F.col(column_name).cast(StringType()).rlike(pattern)
    )

def _validate_date_iso8601(df: DataFrame, column_name: str, **_) -> F.Column:
    """
    True when value is NOT a valid ISO 8601 date/timestamp (violation).

    Valid examples:
      2025-06-08
      2025-06-08T14:30:45
      2025-06-08T14:30:45.640
      2025-06-08T14:30:45Z
      2025-06-08T14:30:45.123Z
      2025-06-08T14:30:45+07:00
      2025-06-08T14:30:45.123+07:00
      2026-06-01 10:20:00          ← SQL/Spark default format
      2026-06-01 10:20:00.123      ← SQL with fractional seconds
    """
    ISO8601_PATTERN = (
        r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"  # YYYY-MM-DD
        r"(?:[T ]"                                              # separator: T (ISO 8601) or space (SQL)
        r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d"                  # HH:MM:SS
        r"(?:\.\d{1,9})?"                                       # .fractional (optional)
        r"(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)?"               # timezone (optional)
        r")?$"
    )

    value = F.col(column_name)
    is_valid = value.cast(StringType()).rlike(ISO8601_PATTERN)

    return value.isNotNull() & ~is_valid

def _validate_is_numeric(
    df: DataFrame,
    column_name: str,
    **_,
) -> F.Column:
    """
    True when value is NOT numeric (violation).
    """
    value = F.trim(F.col(column_name).cast("string"))

    return (
        value.isNotNull()
        & (value != "")
        & value.cast("double").isNull()
    )

def _validate_min_value(
    df: DataFrame,
    column_name: str,
    min_value: float,
    inclusive: bool = True,
    **_,
) -> F.Column:
    """
    True when value violates minimum value rule.
    """
    if inclusive:
        return (
            F.col(column_name).isNotNull()
            & (F.col(column_name) < F.lit(min_value))
        )

    return (
        F.col(column_name).isNotNull()
        & (F.col(column_name) <= F.lit(min_value))
    )

def _validate_less_than(
    df: DataFrame,
    column_name,
    compare_column,
    **_,
) -> F.Column:
    """
    True when column_name >= compare_column (violation).

    Valid:
        column_name < compare_column

    Invalid:
        column_name == compare_column
        column_name > compare_column
    """
    return (
        F.col(column_name).isNotNull()
        & F.col(compare_column).isNotNull()
        & (F.col(column_name) >= F.col(compare_column))
    )

def _validate_accepted_values(df: DataFrame, column_name: str, values: list, **_) -> F.Column:
    """True when value is NOT in the accepted values list (violation)."""
    return ~F.col(column_name).isin(values)


def _validate_data_type(df: DataFrame, column_name: str, type: str, **_) -> F.Column:
    """
    True when the value cannot be cast to the expected type (violation).
    Supports: int, long, double, float, boolean, date, timestamp, string.
    """
    type_map = {
        "int":       "int",
        "integer":   "int",
        "long":      "long",
        "bigint":    "long",
        "double":    "double",
        "float":     "float",
        "boolean":   "boolean",
        "date":      "date",
        "timestamp": "timestamp",
        "string":    None,  # strings are always valid — no cast needed
    }
    spark_type = type_map.get(type.lower())
    if spark_type is None:
        return F.lit(False)
    return F.col(column_name).cast(spark_type).isNull() & F.col(column_name).isNotNull()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_invalid_batch_records(
    rejected_df: DataFrame,
    table_session_id: str,
    target_table: str,
    layer: str,
    record_key_column: str,
) -> None:
    """
    Log invalid records into log.invalid_records.

    Each validation error becomes one row in the log table.
    """

    if rejected_df.isEmpty():
        return

    log_df = (
        rejected_df
        # Split multiple validation errors
        .withColumn(
            "error_item",
            F.explode(
                F.split(
                    F.col("__dq_failure_reason"),
                    r"\s*\|\s*"
                )
            )
        )
        # Parse error column + reason
        .withColumn(
            "error_column",
            F.split(F.col("error_item"), "::").getItem(0)
        )
        .withColumn(
            "error_reason",
            F.split(F.col("error_item"), "::").getItem(1)
        )
        .select(
            F.expr("uuid()").alias("id"),

            F.lit(table_session_id).alias("table_session_id"),

            F.lit(layer).alias("layer"),

            F.lit(target_table).alias("target_table"),

            F.col(record_key_column)
                .cast("string")
                .alias("record_key"),

            F.to_json(
                F.struct(
                    *[
                        c
                        for c in rejected_df.columns
                        if c != "__dq_failure_reason"
                    ]
                )
            ).alias("raw_data"),

            F.col("error_column"),

            F.col("error_reason"),

            F.lit("DQ_VALIDATION").alias("error_type"),

            F.lit(False).alias("is_retryable"),

            F.current_timestamp().alias("created_at"),
        )
    )

    log_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(INVALID_RECORD_TABLE)

    # print(
    #     f"[DQ LOG] Logged {log_df.count():,} invalid record errors "
    #     f"to log.invalid_record"
    # )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALIDATORS: dict[str, callable] = {
    "_validate_is_not_null": _validate_is_not_null,
    "_validate_is_not_empty": _validate_is_not_empty,
    "_validate_max_length": _validate_max_length,
    "_validate_regex": _validate_regex,
    "_validate_date_iso8601": _validate_date_iso8601,
    "_validate_less_than": _validate_less_than,
    "_validate_is_numeric": _validate_is_numeric,
    "_validate_min_value": _validate_min_value,
    "_validate_accepted_values": _validate_accepted_values,
    "_validate_data_type": _validate_data_type,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_dq_validation(
    input_df: DataFrame,
    column_mappings: list[dict],
    source_table: str,
    quarantine_table_name: str,
) -> tuple[DataFrame, DataFrame, int]:
    """
    Apply all DQ rules declared in the column-mapping JSON to *input_df*.

    Iterates over every column entry in *column_mappings*, reads its
    ``validates`` list, and dispatches each rule to the matching function in
    ``VALIDATORS``.  Rows that violate at least one rule are tagged with a
    pipe-delimited failure-reason string in ``__dq_failure_reason`` and
    separated into *df_rejected*; clean rows are returned as *df_valid*.

    Parameters
    ----------
    input_df : DataFrame
        The mapped Silver DataFrame (output of apply_column_transformations).
    column_mappings : list[dict]
        The ``columns`` list from the Bronze-to-Silver mapping JSON.
    source_table : str
        Source table name used in log messages (e.g. ``"bronze.customer"``).
    quarantine_table_name : str
        Target quarantine table for rejected rows.

    Returns
    -------
    tuple[DataFrame, DataFrame, int]
        ``(df_valid, df_rejected, rejected_row_count)``

    Raises
    ------
    ValueError
        If a ``name_func`` value is not registered in ``VALIDATORS``.

    Notes
    -----
    Mapping JSON rule structure::

        {
            "target": "columns mapping",
            "validates": [
                {
                    "name_func": "name function validate",
                    "params": [
                        {
                            "key": "name param",
                            "type": "Type Of Param",
                            "reason_error": "Error Throw When Error"
                        }
                    ]
                }
            ]
        }

    ``params[0].key``          → column_name passed to the validator.
    ``params[0].reason_error`` → human-readable failure message.
    All remaining keys in ``params[0]`` are forwarded as keyword arguments
    (e.g. ``max_length``, ``pattern``, ``values``, ``reference_table``).
    """

    failure_conditions: list[F.Column] = []
    reason_when_clauses: list[F.Column] = []
    KEY_NAME_FUNCTION = "name_func"
    
    available_columns = set(input_df.columns)

    # ------------------------------------------------------------------
    # Iterate columns → validates → params  (mirrors stage-validate skeleton)
    # ------------------------------------------------------------------

    for column_cfg in column_mappings:
        target_column: str = column_cfg.get("target", "")
        validates: list[dict] = column_cfg.get("validates", [])

        if not validates:
            continue

        for validate in validates:

            func_name: str = validate.get(KEY_NAME_FUNCTION, "")

            if func_name not in VALIDATORS:
                raise ValueError(
                    f"[DQ] Unknown validator '{func_name}' on column '{target_column}' "
                    f"(source='{source_table}'). "
                    f"Registered validators: {sorted(VALIDATORS.keys())}"
                )

            validator_func = VALIDATORS[func_name]

            # params is a list with one param-object per the mapping spec
            param_list: list[dict] = validate.get("params", [])

            for param in param_list:
                # "key" → column_name; fall back to "target" if absent
                column_name: str = param.get("key", target_column)
                reason_error: str = param.get(
                    "reason_error", f"{func_name}:{column_name}"
                )

                # Validate mapping configuration
                if column_name not in available_columns:
                    raise ValueError(
                        f"[DQ] Column '{column_name}' "
                        f"not found in dataframe. "
                        f"Validator='{func_name}' "
                        f"Source='{source_table}'"
                    )

                # Forward all remaining param keys as kwargs to the validator
                validator_kwargs = {
                    key: value
                    for key, value in param.items()
                    if key not in {"key", "reason_error"}
                }

             

                # Evaluate the failure-condition Column expression
                failure_col: F.Column = validator_func(
                    input_df, column_name, **validator_kwargs
                )


                # Emit reason_error only when this specific rule fires
                reason_when_clauses.append(
                    F.when(
                        failure_col,
                        F.lit(
                            f"{column_name}::{reason_error}"
                        ),
                    )
                )

                # Append to failure conditions list to make validation active
                failure_conditions.append(failure_col)

                # print(
                #     f"[DQ] Registered rule "
                #     f"| column='{column_name}' "
                #     f"| validator='{func_name}'"
                # )

    # ------------------------------------------------------------------
    # Short-circuit: no rules configured
    # ------------------------------------------------------------------
    if not failure_conditions:
        print(f"[DQ] No validation rules configured for '{source_table}'. Skipping.")
        return (
            input_df,
            spark.createDataFrame([], input_df.schema),
            0,
        )

    # ------------------------------------------------------------------
    # Combine all failure conditions (any rule failing = row rejected)
    # ------------------------------------------------------------------
    is_any_rule_failing: F.Column = F.lit(False)
    for condition in failure_conditions:
        is_any_rule_failing = is_any_rule_failing | condition
    

    # ------------------------------------------------------------------
    # Build combined failure-reason string (pipe-separated active reasons)
    # ------------------------------------------------------------------

    combined_reason = F.concat_ws(
    " | ",
    F.filter(
            F.array(*reason_when_clauses),
            lambda x: x.isNotNull()
        )   
    )

    # ------------------------------------------------------------------
    # Tag each row; split into valid / rejected
    # ------------------------------------------------------------------

    tagged_df: DataFrame = input_df.withColumn(
        DQ_FAILURE_REASON_COLUMN,
        F.when(is_any_rule_failing, combined_reason).otherwise(F.lit(None)),
    )

    df_valid = (
        tagged_df
        .filter(F.col(DQ_FAILURE_REASON_COLUMN).isNull())
        .drop(DQ_FAILURE_REASON_COLUMN)
    )

    df_rejected: DataFrame = tagged_df.filter(
        F.col(DQ_FAILURE_REASON_COLUMN).isNotNull()
    )

    # Cache because count + downstream usage
    # df_valid.cache()
    # df_rejected.cache()

    # ------------------------------------------------------------------
    # Count & log
    # ------------------------------------------------------------------
    rejected_row_count: int = df_rejected.count()
    valid_row_count: int = df_valid.count()

    # print(
    #     f"[DQ] Validation complete "
    #     f"| source={source_table} "
    #     f"| valid={valid_row_count:,} "
    #     f"| rejected={rejected_row_count:,}"
    # )

    # After df_rejected is created
    # error_rows = (
    #     df_rejected
    #     .select(DQ_FAILURE_REASON_COLUMN)
    #     .limit(20)
    #     .collect()
    # )

    # for row in error_rows:
    #     reason_str: str = row[DQ_FAILURE_REASON_COLUMN] or ""
    #     # Each segment is "column_name::reason_error"; extract unique column names
    #     failed_columns = [
    #         segment.split("::")[0].strip()
    #         for segment in reason_str.split(" | ")
    #         if "::" in segment
    #     ]
    #     print(
    #         f"[DQ] Failed columns={failed_columns} "
    #         f"| Reason='{reason_str}'"
    #     )

    return df_valid, df_rejected, rejected_row_count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_silver_full_load(valid_df: DataFrame, silver_table_name: str) -> dict:
    """
    Write all valid records to Silver using FULL LOAD strategy.

    Truncates the target table first, then writes all rows in Delta format
    to guarantee ACID semantics.

    Parameters
    ----------
    valid_df : DataFrame
        Clean rows ready to be written (output of DQ validation).
    silver_table_name : str
        Fully qualified Silver table name, e.g. 'silver.agent'.

    Returns
    -------
    dict
        Write statistics: {target_table, inserted_row, load_mode}.

    Raises
    ------
    Exception
        Re-raised if the write operation fails.
    """
    inserted_count: int = valid_df.count()
    print(
        f"[SILVER FULL] Truncating '{silver_table_name}' and writing "
        f"{inserted_count:,} row(s)..."
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table_name)
    )

    print(f"[SILVER FULL] Full load complete. Rows written: {inserted_count:,}")
    return {
        "target_table": silver_table_name,
        "inserted_row": inserted_count,
        "updated_row": 0,
        "load_mode": LOAD_TYPE_FULL,
    }


def write_silver_merge(
    valid_df: DataFrame,
    silver_table_name: str,
    primary_key_columns: list[str],
) -> dict:
    """
    Write valid records to Silver using MERGE INTO (upsert) for incremental load.

    Builds a dynamic MERGE condition from the primary key columns.
    On match: updates all columns. On no match: inserts the new row.

    Parameters
    ----------
    valid_df : DataFrame
        Clean, deduplicated rows to merge.
    silver_table_name : str
        Fully qualified Silver table name.
    primary_key_columns : list[str]
        Column names that form the merge key.

    Returns
    -------
    dict
        Merge statistics: {target_table, inserted_row, updated_row, load_mode}.

    Raises
    ------
    ValueError
        If primary_key_columns is empty.
    AnalysisException
        If the Silver table does not exist.
    """
    if not primary_key_columns:
        raise ValueError(
            f"[SILVER MERGE] primary_key_columns must not be empty "
            f"for table '{silver_table_name}'."
        )

    try:
        silver_delta_table = DeltaTable.forName(spark, silver_table_name)
    except AnalysisException as analysis_error:
        raise AnalysisException(
            f"[SILVER MERGE] Silver table '{silver_table_name}' does not exist. "
            f"DDL must be created separately before running this notebook. "
            f"Original error: {analysis_error}"
        )

    # Build MERGE join condition: target.pk = source.pk AND ...
    merge_condition: str = " AND ".join(
        [f"target.{pk} = source.{pk}" for pk in primary_key_columns]
    )

    # Build SET clause for UPDATE: all non-PK columns
    update_columns: dict[str, str] = {
        col_name: f"source.{col_name}"
        for col_name in valid_df.columns
    }

    # print(
    #     f"[SILVER MERGE] Merging {valid_df.count():,} row(s) into '{silver_table_name}' "
    #     f"on condition: {merge_condition}"
    # )

    (
        silver_delta_table.alias("target")
        .merge(valid_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_columns)
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Retrieve merge operation metrics from Delta history
    merge_metrics: dict = (
        silver_delta_table
        .history(1)
        .select("operationMetrics")
        .first()[0]
    )
    inserted_count: int = int(merge_metrics.get("numTargetRowsInserted", 0))
    updated_count: int = int(merge_metrics.get("numTargetRowsUpdated", 0))

    print(
        f"[SILVER MERGE] Merge complete: inserted={inserted_count:,} | updated={updated_count:,}"
    )
    return {
        "target_table": silver_table_name,
        "inserted_row": inserted_count,
        "updated_row": updated_count,
        "load_mode": LOAD_TYPE_INCREMENTAL,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_single_table(
    config_row: dict,
    batch_id: str,
    force_load_type: str,
) -> dict:
    """
    Execute the full Bronze → Silver pipeline for a single table config.

    Parameters
    ----------
    config_row : dict
        A single entry from the p_config_load_table parameter array.
    batch_id : str
        Pipeline batch identifier used for incremental filtering.
    force_load_type : str
        Runtime override for load type ('full' | 'incremental' | '').

    Returns
    -------
    dict
        Result summary for this table including status, row counts, and any
        error message. Keys:
            source_table_id, bronze_table_name, silver_table_name,
            status, source_row_count, target_row_count,
            inserted_row_count, updated_row_count, rejected_row_count,
            error_message, load_type
    """
    # -----------------------------------------------------------------------
    # Local tracking variables
    # -----------------------------------------------------------------------
    _source_row_count: int = 0
    _target_row_count: int = 0
    _inserted_row_count: int = 0
    _updated_row_count: int = 0
    _rejected_row_count: int = 0
    _pipeline_status: str = STATUS_FAILED
    _error_message: str | None = None

    # -----------------------------------------------------------------------
    # Resolve config values
    # -----------------------------------------------------------------------
    _resolved_load_type: str = (
        force_load_type.lower()
        if force_load_type
        else config_row.get("load_type", LOAD_TYPE_INCREMENTAL).lower()
    )

    _BRONZE_TABLE_NAME: str = config_row["bronze_table_name"]
    _SILVER_TABLE_NAME: str = config_row["silver_table_name"]
    _MAPPING_PATH: str = config_row["bronze_to_silver_mapping_path"]
    _PRIMARY_KEY_COLUMNS: list[str] = [
        pk.strip() for pk in config_row["primary_key"].split(",") if pk.strip()
    ]
    _QUARANTINE_TABLE_NAME: str = f"{QUARANTINE_SCHEMA}.{_SILVER_TABLE_NAME.split('.')[-1]}"

    print(
        f"[TABLE START] bronze={_BRONZE_TABLE_NAME} | silver={_SILVER_TABLE_NAME} "
        f"| load_type={_resolved_load_type}"
    )

    try:
        # -------------------------------------------------------------------
        # STEP 1 — Load JSON Mapping File
        # -------------------------------------------------------------------
        mapping: dict = load_mapping_json(_MAPPING_PATH)
        column_mappings: list[dict] = mapping["columns"]

        # -------------------------------------------------------------------
        # STEP 2 — Read Bronze Source Table
        # -------------------------------------------------------------------
        bronze_df: DataFrame = read_bronze_table(
            source_table_name=_BRONZE_TABLE_NAME,
            load_type=_resolved_load_type,
            batch_id=batch_id,
        )
        _source_row_count = bronze_df.count()

        # -------------------------------------------------------------------
        # STEP 3 — Apply Column Transformations
        # -------------------------------------------------------------------
        mapped_silver_df: DataFrame = apply_column_transformations(
            bronze_df=bronze_df,
            column_mappings=column_mappings,
            source_table_name=_BRONZE_TABLE_NAME,
        )

        # -------------------------------------------------------------------
        # STEP 4 — Deduplication (within-batch hash-based)
        # -------------------------------------------------------------------
        hashed_silver_df: DataFrame = compute_row_hash(
            input_df=mapped_silver_df,
            exclude_columns=[INGESTION_TIMESTAMP_COLUMN],
        )
        deduped_batch_df: DataFrame = deduplicate_within_batch(
            input_df=hashed_silver_df,
            primary_key_columns=_PRIMARY_KEY_COLUMNS,
        )
        mapped_silver_df_final: DataFrame = deduped_batch_df.drop("__row_hash")

        # -------------------------------------------------------------------
        # STEP 5 — Data Quality (DQ) Validation
        # -------------------------------------------------------------------
        df_valid, df_rejected, _rejected_row_count = run_dq_validation(
            input_df=mapped_silver_df_final,
            column_mappings=column_mappings,
            source_table=_BRONZE_TABLE_NAME,
            quarantine_table_name=_QUARANTINE_TABLE_NAME,
        )

        # Log invalid records (uses a placeholder table_session_id="pending"
        # because the real ID is resolved in the orchestration layer)
        if _rejected_row_count > 0:
            log_invalid_batch_records(
                df_rejected,
                table_session_id="pending",
                layer=LAYER_SILVER,
                target_table=_SILVER_TABLE_NAME,
                record_key_column=config_row["primary_key"],
            )

        # If all rows are rejected, skip MERGE and mark as WARNING
        if df_valid.count() == 0:
            print(
                f"[{_BRONZE_TABLE_NAME}] All {_rejected_row_count:,} row(s) rejected by DQ. "
                "Skipping MERGE. Status=WARNING."
            )
            _pipeline_status = STATUS_WARNING
            _target_row_count = 0

        else:
            # ---------------------------------------------------------------
            # STEP 6 — Write to Silver (FULL LOAD or MERGE)
            # ---------------------------------------------------------------
            write_stats: dict

            if _resolved_load_type == LOAD_TYPE_FULL:
                write_stats = write_silver_full_load(
                    valid_df=df_valid,
                    silver_table_name=_SILVER_TABLE_NAME,
                )
            else:
                write_stats = write_silver_merge(
                    valid_df=df_valid,
                    silver_table_name=_SILVER_TABLE_NAME,
                    primary_key_columns=_PRIMARY_KEY_COLUMNS,
                )

            _inserted_row_count = write_stats.get("inserted_row", 0)
            _updated_row_count = write_stats.get("updated_row", 0)
            _target_row_count = _inserted_row_count + _updated_row_count
            _pipeline_status = STATUS_SUCCESS

    except Exception as pipeline_error:
        _error_message = (
            f"{type(pipeline_error).__name__}: {str(pipeline_error)}\n"
            f"{traceback.format_exc()}"
        )
        _pipeline_status = STATUS_FAILED
        print(f"[PIPELINE ERROR] table={_BRONZE_TABLE_NAME}\n{_error_message}")

    result = {
        "source_table_id": config_row["id"],
        "bronze_table_name": _BRONZE_TABLE_NAME,
        "silver_table_name": _SILVER_TABLE_NAME,
        "status": _pipeline_status,
        "load_type": _resolved_load_type,
        "source_row_count": _source_row_count,
        "target_row_count": _target_row_count,
        "inserted_row_count": _inserted_row_count,
        "updated_row_count": _updated_row_count,
        "rejected_row_count": _rejected_row_count,
        "error_message": _error_message,
    }
    print(
        f"[TABLE END] bronze={_BRONZE_TABLE_NAME} | status={_pipeline_status} "
        f"| inserted={_inserted_row_count} | updated={_updated_row_count} "
        f"| rejected={_rejected_row_count}"
    )
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n" + "=" * 70)
print("[PHASE 1] Starting audit table sessions")
print("=" * 70)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── PHASE 1: Mark all tables as RUNNING before ingestion ──
pre_ingest_results = [
    {
        "source_table_id": _cfg["id"],
        "bronze_table_name": _cfg["bronze_table_name"],
        "load_type": (
            force_load_type.upper()
            if force_load_type
            else _cfg.get("load_type", "FULL").upper()
        ),
    }
    for _cfg in config_load_tables
]

update_table_layer_status_batch(
    session_id=session_id,
    layer=Layer.SILVER,
    table_results=pre_ingest_results,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MAX_WORKERS: int = 9

print("\n" + "=" * 70)
print(f"[PHASE 2] Processing {len(config_load_tables)} table(s) in parallel (max_workers={MAX_WORKERS})")
print("=" * 70)

table_results: list[dict] = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as _executor:
    _future_to_cfg = {
        _executor.submit(
            process_single_table,
            config_row=_cfg,
            batch_id=batch_id,
            force_load_type=force_load_type,
        ): _cfg
        for _cfg in config_load_tables
    }

    for _future in as_completed(_future_to_cfg):
        _cfg = _future_to_cfg[_future]
        try:
            _result = _future.result()
            table_results.append(_result)
        except Exception as _future_err:
            # Unexpected exception escaping process_single_table — capture and continue
            _err_msg = (
                f"{type(_future_err).__name__}: {str(_future_err)}\n"
                f"{traceback.format_exc()}"
            )
            print(f"[PHASE 2] Unhandled error for {_cfg['bronze_table_name']}: {_err_msg}")
            table_results.append({
                "source_table_id": _cfg["id"],
                "bronze_table_name": _cfg["bronze_table_name"],
                "silver_table_name": _cfg["silver_table_name"],
                "status": STATUS_FAILED,
                "load_type": force_load_type or _cfg.get("load_type", ""),
                "source_row_count": 0,
                "target_row_count": 0,
                "inserted_row_count": 0,
                "updated_row_count": 0,
                "rejected_row_count": 0,
                "error_message": _err_msg,
            })


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n" + "=" * 70)
print("[PHASE 3] Writing audit finish records for all tables")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def new_audit_id() -> str:
    return str(uuid.uuid4())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AUDIT_DETAIL_SCHEMA = StructType([

    StructField(
        "id",
        StringType(),
        False
    ),

    StructField(
        "table_session_id",
        StringType(),
        False
    ),

    StructField(
        "attempt_no",
        IntegerType(),
        True
    ),

    StructField(
        "detail_status",
        StringType(),
        True
    ),

    StructField(
        "layer",
        StringType(),
        True
    ),

    StructField(
        "watermark_before",
        StringType(),
        True
    ),

    StructField(
        "watermark_after",
        StringType(),
        True
    ),

    StructField(
        "source_row_count",
        IntegerType(),
        True
    ),

    StructField(
        "target_row_count",
        IntegerType(),
        True
    ),

    StructField(
        "inserted_row",
        IntegerType(),
        True
    ),

    StructField(
        "updated_row",
        IntegerType(),
        True
    ),

    StructField(
        "deleted_row",
        IntegerType(),
        True
    ),

    StructField(
        "rejected_row",
        IntegerType(),
        True
    ),

    StructField(
        "error_message",
        StringType(),
        True
    ),

    StructField(
        "error_type",
        StringType(),
        True
    ),

    StructField(
        "is_retryable",
        BooleanType(),
        True
    ),

    StructField(
        "duration_ms",
        LongType(),
        True
    ),

    StructField(
        "sla_target_ms",
        LongType(),
        True
    ),

    StructField(
        "sla_breached",
        BooleanType(),
        True
    )
])



# ============================================================
# INSERT AUDIT DETAIL
# ============================================================

def append_audit_detail_batch(
    detail_df,
    audit_detail_table= AUDIT_DETAIL_TABLE
):

    if detail_df.rdd.isEmpty():

        print(
            "[AUDIT] No audit detail rows"
        )

        return


    final_df = (

        detail_df

        .withColumn(
            "created_at",
            F.current_timestamp()
        )

        .withColumn(
            "updated_at",
            F.current_timestamp()
        )

    )


    (
        final_df

        .write

        .format("delta")

        .mode("append")

        .saveAsTable(
            audit_detail_table
        )
    )


    print(
        f"[AUDIT] Inserted {final_df.count()} audit detail rows"
    )




# ============================================================
# FINISH SILVER BATCH
# ============================================================

def finish_table_layer_batch(
    session_id: str,
    table_results: list[dict],
    audit_table_session_table= AUDIT_TABLE_SESSION_TABLE,
    audit_detail_table= AUDIT_DETAIL_TABLE
):


    # ========================================================
    # 1. BUILD SILVER RESULT DATAFRAME
    # ========================================================
    source_rows = [
        Row(
            source_table_id=str(r["source_table_id"]),
            silver_status=r.get("status"),
            error_code=r.get("error_code"),
            error_message=r.get("error_message"),
            watermark_after=r.get("watermark_after"),
            sla_target_ms=r.get("sla_target_ms"),
            source_row_count=r.get("source_row_count"),
            target_row_count=r.get("target_row_count"),
            inserted_row=r.get("inserted_row_count"),
            updated_row=r.get("updated_row_count"),
            rejected_row=r.get("rejected_row_count"),
            error_type=r.get("error_type")
        )
        for r in table_results
    ]

    source_schema = StructType([
        StructField(
            "source_table_id",
            StringType(),
            False
        ),
        StructField(
            "silver_status",
            StringType(),
            True
        ),
        StructField(
            "error_code",
            StringType(),
            True
        ),
        StructField(
            "error_message",
            StringType(),
            True
        ),
        StructField(
            "watermark_after",
            StringType(),
            True
        ),
        StructField(
            "sla_target_ms",
            LongType(),
            True
        ),
        StructField(
            "source_row_count",
            IntegerType(),
            True
        ),
        StructField(
            "target_row_count",
            IntegerType(),
            True
        ),
        StructField(
            "inserted_row",
            IntegerType(),
            True
        ),

        StructField(
            "updated_row",
            IntegerType(),
            True
        ),
        StructField(
            "rejected_row",
            IntegerType(),
            True
        ),
        StructField(
            "error_type",
            StringType(),
            True
        )
    ])


    source_df = spark.createDataFrame(
        source_rows,
        source_schema
    )

    # ========================================================
    # 2. GET EXISTING TABLE 
    # ========================================================
    table_session_df = (
        spark.table(
            audit_table_session_table
        )
        .filter(
            F.col("session_id") == session_id
        )
        .select(
            F.col("id")
            .alias(
                "table_session_id"
            ),
            "source_table_id"
        )
    )

    # ========================================================
    # 3. VALIDATE RELATION
    # ========================================================

    missing_df = (
        source_df
        .join(
            table_session_df,
            on="source_table_id",
            how="left"
        )
        .filter(
            F.col("table_session_id")
            .isNull()
        )
    )

    if missing_df.count() > 0:
        print(
            "[AUDIT] WARNING — Missing audit_table_session for:"
        )
        missing_df.show(
            False
        )


    # ========================================================
    # 4. UPDATE SILVER STATUS ONLY
    # ========================================================
    source_df.createOrReplaceTempView(
        "silver_update_source"
    )
    
    spark.sql(
        f"""
        MERGE INTO {audit_table_session_table} target
        USING silver_update_source source
        ON
        target.session_id = '{session_id}'
        AND
        target.source_table_id =
        source.source_table_id
        WHEN MATCHED THEN
        UPDATE SET
        target.silver_status =
        source.silver_status,
        target.silver_ended_at =
        current_timestamp(),
        target.error_code =
        CASE
            WHEN source.silver_status =
            '{AuditStatus.FAILED.value}'
            THEN source.error_code
            ELSE NULL
        END,
        target.error_message =
        CASE
            WHEN source.silver_status =
            '{AuditStatus.FAILED.value}'
            THEN source.error_message
            ELSE NULL
        END,
        target.updated_at =
        current_timestamp()
        """
    )



    # ========================================================
    # 5. CREATE AUDIT DETAIL DATA
    # ========================================================


    detail_df = (
        source_df
        .join(
            table_session_df,
            on="source_table_id",
            how="inner"
        )
        .select(
            F.expr(
                "uuid()"
            )
            .alias(
                "id"
            ),

            "table_session_id",
            F.lit(None)
            .cast("int")
            .alias(
                "attempt_no"
            ),
            F.col(
                "silver_status"
            )
            .alias(
                "detail_status"
            ),
            F.lit(
                "SILVER"
            )
            .alias(
                "layer"
            ),

            F.lit(None)
            .cast("string")
            .alias(
                "watermark_before"
            ),
            "watermark_after",
            "source_row_count",
            "target_row_count",
            "inserted_row",
            "updated_row",
            F.lit(0)
            .alias(
                "deleted_row"
            ),
            "rejected_row",
            "error_message",
            "error_type",
            (
                F.col("silver_status") == AuditStatus.FAILED.value
            )
            .alias(
                "is_retryable"
            ),

            F.lit(None)
            .cast("long")
            .alias(
                "duration_ms"
            ),
            "sla_target_ms",
            F.lit(None)
            .cast("boolean")
            .alias(
                "sla_breached"
            )
        )
    )


    # ========================================================
    # 6. INSERT DETAIL
    # ========================================================

    append_audit_detail_batch(
        detail_df,
        audit_detail_table
    )


    print(
        f"""
        [AUDIT]
        Finished SILVER layer
        session_id={session_id}
        tables={len(table_results)}
        """
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Debug: inspect first row keys
if table_results:
    print(f"[AUDIT DEBUG] table_results[0] keys: {list(table_results[0].keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    table_sessions = finish_table_layer_batch(session_id=session_id,table_results=table_results)
except Exception as _audit_finish_err:
        print(
            f"[AUDIT] Warning — finish_table_layer_batch failed for"
            f"{_audit_finish_err}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n" + "=" * 70)
print("[PHASE 4] PIPELINE SUMMARY")
print("=" * 70)

for _r in table_results:
    print(
        f"  [{_r['status']:>7}] {_r['silver_table_name']:<40} "
        f"src={_r['source_row_count']:>6} "
        f"ins={_r['inserted_row_count']:>6} "
        f"upd={_r['updated_row_count']:>6} "
        f"rej={_r['rejected_row_count']:>6}"
    )

_failed_tables = [_r["silver_table_name"] for _r in table_results if _r["status"] == STATUS_FAILED]

if _failed_tables:
    print(f"\n[SUMMARY] {len(_failed_tables)} table(s) FAILED: {_failed_tables}")
else:
    print(f"\n[SUMMARY] All {len(table_results)} table(s) completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(json.dumps(table_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
