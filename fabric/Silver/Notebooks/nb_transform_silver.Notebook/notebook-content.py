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

# CELL ********************

# # DUMP FOR TEST
# p_batch_id = 1
# p_config_load_table = "{\"id\":3,\"source_system\":\"crm_system\",\"source_type\":\"database\",\"source_name\":\"insurance_providers\",\"source_location\":\"dbo.insurance_providers\",\"source_format\":\"table\",\"delimiter\":null,\"load_type\":\"INCREMENTAL\",\"primary_key\":\"provider_code\",\"source_to_bronze_mapping_path\":\"Files/config/mapping/source-to-bronze/insurance_provider.json\",\"bronze_to_silver_mapping_path\":\"Files/config/mapping/bronze-to-silver/insurance_provider.json\",\"silver_transform_name\":null,\"watermark_column\":\"updated_date\",\"bronze_table_name\":\"bronze.insurance_provider\",\"silver_table_name\":\"silver.provider\",\"load_sequence\":3,\"is_active\":true,\"created_at\":\"2026-06-07T14:44:47.158114\",\"updated_at\":\"2026-06-07T14:44:47.158114\"}"
# p_session_id="3a967666-924c-4c08-904d-7b734f4880cc"
# p_pipeline_run_id="PL_01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# IMPORTS & CONSTANTS
# ---------------------------------------------------------------------------
import json
import os
import hashlib
import traceback
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ##  PARAMETERS
# Parameters injected by Fabric Pipeline. Edit defaults for local development only.

# PARAMETERS CELL ********************

# ---------------------------------------------------------------------------
# FABRIC NOTEBOOK PARAMETERS
# These values are overridden at runtime by the Fabric Pipeline activity.
# ---------------------------------------------------------------------------

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

# MARKDOWN ********************

# ---
# ## IMPORTS & SETUP

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
STATUS_WARNING: str = "WARNING"
STATUS_FAILED: str = "FAILED"

# Run mode constants (from cfg.next_run_mode)
RUN_MODE_NEW: str = "NEW"
RUN_MODE_RECOVERY: str = "RECOVERY"

# Load type constants
LOAD_TYPE_FULL: str = "full"
LOAD_TYPE_INCREMENTAL: str = "incremental"

# ---------------------------------------------------------------------------
# SPARK SESSION
# On Microsoft Fabric, 'spark' is pre-injected. The line below is a fallback
# for local unit testing.
# ---------------------------------------------------------------------------
# try:
#     spark  # noqa: F821  — already available in Fabric runtime
# except NameError:
#     spark = SparkSession.builder.appName("nb_ingest_bronze_silver_dev").getOrCreate()

# ---------------------------------------------------------------------------
# IMPORT AUDIT LOGGING HELPER
# On Microsoft Fabric, notebooks can be referenced with %run.
# Uncomment the line below when running inside Fabric:
#   %run /nb_audit_logging_helper_dev
# For local testing, the functions start_table_layer / finish_table_layer
# must be available from the imported notebook's scope.
# ---------------------------------------------------------------------------
# %run /nb_audit_logging_helper_dev



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# str_config_load_table = p_config_load_table

print(p_config_load_table)

config_load_table = json.loads(p_config_load_table)
batch_id = p_batch_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"[SETUP] Bronze → Silver notebook initialised | env={run_env} | batch_id={batch_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ##  UTILITY FUNCTIONS
# Reusable functions used throughout the pipeline steps.

# CELL ********************

# ===========================================================================
# SECTION 3A — CONFIG & MAPPING UTILITIES
# ===========================================================================

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

    print(
        f"[MAPPING] Loaded mapping | source={mapping['source_table']} "
        f"| target={mapping['target_table']} "
        f"| columns={len(mapping['columns'])}"
    )
    return mapping


def get_next_run_mode(current_batch_id: int) -> dict:
    """
    Read the single-row control table cfg.next_run_mode to determine
    whether this is a NEW run or a RECOVERY run.

    Parameters
    ----------
    current_batch_id : int
        The batch_id passed as a notebook parameter.

    Returns
    -------
    dict
        Row from cfg.next_run_mode as a dictionary.
    """
    run_mode_df = spark.table(NEXT_RUN_MODE_TABLE).limit(1)

    if run_mode_df.count() == 0:
        print(f"[RUN_MODE] '{NEXT_RUN_MODE_TABLE}' is empty. Defaulting to NEW run.")
        return {"run_mode": RUN_MODE_NEW, "batch_id": current_batch_id}

    run_mode_row: dict = run_mode_df.first().asDict()
    print(
        f"[RUN_MODE] run_mode={run_mode_row.get('run_mode')} "
        f"| batch_id={run_mode_row.get('batch_id')}"
    )
    return run_mode_row


def get_failed_tables_from_previous_run(previous_batch_id: int) -> list[str]:
    """
    In RECOVERY mode, fetch the list of silver table names that failed
    in the previous pipeline run using the audit_table_session table.

    Parameters
    ----------
    previous_batch_id : int
        The batch_id of the failed run to recover from.

    Returns
    -------
    list[str]
        List of target_table_name values that need to be retried.
    """
    try:
        failed_tables_df = (
            spark.table(AUDIT_TABLE_SESSION)
            .filter(
                (F.col("batch_id") == previous_batch_id)
                & (F.col("layer") == LAYER_SILVER)
                & (F.col("status") == STATUS_FAILED)
            )
            .select("target_table_name")
        )
        failed_table_names: list[str] = [
            row["target_table_name"] for row in failed_tables_df.collect()
        ]
        print(
            f"[RECOVERY] Found {len(failed_table_names)} failed Silver table(s) "
            f"from batch_id={previous_batch_id}: {failed_table_names}"
        )
        return failed_table_names
    except AnalysisException as analysis_error:
        print(
            f"[RECOVERY] Warning — Could not query audit table: {analysis_error}. "
            "Proceeding as NEW run."
        )
        return []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# SECTION 3B — BRONZE READER
# ===========================================================================

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
    print(
        f"[BRONZE] Read {source_row_count:,} row(s) from '{source_table_name}' "
        f"(load_type={load_type})"
    )
    return bronze_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# SECTION 3C — COLUMN TRANSFORMATION ENGINE
# ===========================================================================

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

    print(f"[TRANSFORM] Mapped columns   : {mapped_columns}")
    print(f"[TRANSFORM] Null-expression  : {null_columns}")
    return mapped_silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# SECTION 3D — DEDUPLICATION ENGINE (Hash-based)
# ===========================================================================

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
    print(
        f"[DEDUP] Within-batch deduplication: {pre_count:,} → {post_count:,} rows "
        f"(removed {pre_count - post_count:,} duplicate(s)) on keys={primary_key_columns}"
    )
    return deduped_df


def deduplicate_against_silver(
    incoming_df: DataFrame,
    silver_table_name: str,
    primary_key_columns: list[str],
) -> DataFrame:
    """
    Compare incoming batch hashes against existing Silver records to skip
    rows whose data has not changed.

    If the Silver table does not exist yet, all incoming rows are treated as new.

    Parameters
    ----------
    incoming_df : DataFrame
        Batch DataFrame with '__row_hash' column.
    silver_table_name : str
        Fully qualified Silver table name.
    primary_key_columns : list[str]
        Join keys used to match existing Silver rows.

    Returns
    -------
    DataFrame
        Subset of incoming_df where the row hash differs from Silver
        (i.e. genuinely new or changed records).
    """
    try:
        silver_hash_df: DataFrame = spark.table(silver_table_name).select(
            *primary_key_columns, "__row_hash"
        )
    except AnalysisException:
        print(
            f"[DEDUP] Silver table '{silver_table_name}' not found. "
            "All incoming rows treated as new (first load)."
        )
        return incoming_df

    # Left anti join on PK + hash — only rows not already in Silver pass through
    join_condition = [
        incoming_df[pk] == silver_hash_df[pk] for pk in primary_key_columns
    ] + [incoming_df["__row_hash"] == silver_hash_df["__row_hash"]]

    changed_df: DataFrame = incoming_df.join(
        silver_hash_df, on=join_condition, how="left_anti"
    )

    pre_count: int = incoming_df.count()
    changed_count: int = changed_df.count()
    print(
        f"[DEDUP] Silver hash comparison: {pre_count:,} incoming → {changed_count:,} changed "
        f"(skipped {pre_count - changed_count:,} unchanged record(s))"
    )
    return changed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


DQ_FAILURE_REASON_COLUMN: str = "__dq_failure_reason"

# ---------------------------------------------------------------------------
# Validator helpers
# Each accepts (df, column_name, **params) and returns a Column expression
# that evaluates to True for FAILING rows.
# ---------------------------------------------------------------------------


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

def _validate_is_unique(df: DataFrame, column_name: str, **_) -> F.Column:
    """True when column value appears more than once in df (duplicate = violation)."""
    count_col = f"__cnt_{column_name}"
    counts = df.groupBy(column_name).agg(F.count("*").alias(count_col))
    dup_values = [
        row[column_name]
        for row in counts.filter(F.col(count_col) > 1).select(column_name).collect()
    ]
    if not dup_values:
        return F.lit(False)
    return F.col(column_name).isin(dup_values)


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
      2025-06-08T14:30:45Z
      2025-06-08T14:30:45.123Z
      2025-06-08T14:30:45+07:00
      2025-06-08T14:30:45.123+07:00
    """

    value = F.col(column_name)

    parsed_date = F.to_date(value, "yyyy-MM-dd")

    parsed_timestamp = F.coalesce(
        F.to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    )

    return (
        value.isNotNull()
        & parsed_date.isNull()
        & parsed_timestamp.isNull()
    )

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


def _validate_foreign_key(
    df: DataFrame,
    column_name: str,
    reference_table: str,
    reference_column: str,
    **_,
) -> F.Column:
    """
    True when the value does NOT exist in reference_table.reference_column (violation).
    Gracefully skips the check if the reference table cannot be read.
    """
    try:
        ref_df: DataFrame = (
            spark.table(reference_table)  # noqa: F821 — spark injected by Fabric
            .select(F.col(reference_column).alias("__ref_key"))
            .distinct()
        )
        valid_keys = {row["__ref_key"] for row in ref_df.collect()}
        return ~F.col(column_name).isin(list(valid_keys))
    except AnalysisException as analysis_error:
        print(
            f"[DQ] Warning — Could not read reference table '{reference_table}': "
            f"{analysis_error}. Skipping FK check for '{column_name}'."
        )
        return F.lit(False)


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

    if rejected_df.rdd.isEmpty():
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

    print(
        f"[DQ LOG] Logged {log_df.count():,} invalid record errors "
        f"to log.invalid_record"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Validator registry — maps name_func string → callable
# Add a new entry here to register additional validators without touching
# any other pipeline code.
# ---------------------------------------------------------------------------

VALIDATORS: dict[str, callable] = {
    "_validate_is_not_null": _validate_is_not_null,
    "_validate_is_not_empty": _validate_is_not_empty,
    "_validate_is_unique": _validate_is_unique,
    "_validate_max_length": _validate_max_length,
    "_validate_regex": _validate_regex,
    "_validate_date_iso8601": _validate_date_iso8601,
    "_validate_less_than": _validate_less_than,
    "_validate_is_numeric": _validate_is_numeric,
    "_validate_min_value": _validate_min_value,
    "_validate_accepted_values": _validate_accepted_values,
    "_validate_data_type": _validate_data_type,
    "_validate_foreign_key": _validate_foreign_key,
}


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

                print(
                    f"[DQ] Registered rule "
                    f"| column='{column_name}' "
                    f"| validator='{func_name}'"
                )

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

    print(
        f"[DQ] Validation complete "
        f"| source={source_table} "
        f"| valid={valid_row_count:,} "
        f"| rejected={rejected_row_count:,}"
    )

    return df_valid, df_rejected, rejected_row_count




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# SECTION 3F — SILVER WRITER (FULL LOAD & MERGE / INCREMENTAL)
# ===========================================================================

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

    print(
        f"[SILVER MERGE] Merging {valid_df.count():,} row(s) into '{silver_table_name}' "
        f"on condition: {merge_condition}"
    )

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

# MARKDOWN ********************

# ---
# ##  Read Config & Determine Run Mode

# CELL ********************

# ---------------------------------------------------------------------------
# Read source configuration and resolve run mode
# ---------------------------------------------------------------------------

# Read the configuration row for this pipeline invocation
config_row: dict = config_load_table

# Resolve load type: notebook parameter overrides config table value
resolved_load_type: str = (
    force_load_type.lower()
    if force_load_type
    else config_row.get("load_type", LOAD_TYPE_INCREMENTAL).lower()
)

print(f"[STEP 1] Resolved load_type={resolved_load_type}")

# Determine run mode for incremental loads (NEW vs RECOVERY)
run_mode_info: dict = get_next_run_mode(batch_id)
current_run_mode: str = run_mode_info.get("run_mode", RUN_MODE_NEW)
previous_batch_id: int = run_mode_info.get("batch_id", batch_id)

# In RECOVERY mode, check which Silver tables failed in the previous run
failed_silver_tables_to_retry: list[str] = []
if current_run_mode == RUN_MODE_RECOVERY and resolved_load_type == LOAD_TYPE_INCREMENTAL:
    print(f"[STEP 1] RECOVERY mode detected for batch_id={previous_batch_id}")
    failed_silver_tables_to_retry = get_failed_tables_from_previous_run(previous_batch_id)

# Core config values used across all subsequent steps
BRONZE_TABLE_NAME: str = config_row["bronze_table_name"]
SILVER_TABLE_NAME: str = config_row["silver_table_name"]
MAPPING_PATH: str = config_row["bronze_to_silver_mapping_path"]
PRIMARY_KEY_COLUMNS: list[str] = [
    pk.strip() for pk in config_row["primary_key"].split(",")
    if pk.strip()
]
WATERMARK_COLUMN: str = config_row.get("watermark_column") or ""
SILVER_TRANSFORM_NAME: str = config_row.get("silver_transform_name") or ""
QUARANTINE_TABLE_NAME: str = f"{QUARANTINE_SCHEMA}.{SILVER_TABLE_NAME.split('.')[-1]}"

print(
    f"[STEP 1] Config resolved:\n"
    f"  bronze_table      = {BRONZE_TABLE_NAME}\n"
    f"  silver_table      = {SILVER_TABLE_NAME}\n"
    f"  mapping_path      = {MAPPING_PATH}\n"
    f"  primary_key       = {PRIMARY_KEY_COLUMNS}\n"
    f"  watermark_column  = {WATERMARK_COLUMN}\n"
    f"  run_mode          = {current_run_mode}\n"
    f"  load_type         = {resolved_load_type}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Runtime tracking variables — populated during pipeline execution
# ---------------------------------------------------------------------------
source_row_count: int = 0
target_row_count: int = 0
inserted_row_count: int = 0
updated_row_count: int = 0
rejected_row_count: int = 0
pipeline_status: str = STATUS_FAILED
error_message: str | None = None
table_session_id: str | None = None
source_table_id = config_load_table["id"]
source_table = config_load_table["bronze_table_name"]
WATERMARK_TABLE:str = "cfg.watermark"
COLUMN_WATERMARK_VALUE:str = "watermark_value"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DUMP START SESSION PIPELINE LOG
# try:
#     session_id_dump = start_pipeline_session(
#         pipeline_name="TEST_SILVER_LAYER",
#         pipeline_run_id="PL_01",
#         batch_id=batch_id,
#         run_mode=current_run_mode,
#     )

#     print(f"[AUDIT] Started table layer session")
# except Exception as audit_start_error:
#     print(f"[AUDIT] Warning — Could not start audit session: {audit_start_error}")



session_id = p_session_id
pipeline_run_id = p_pipeline_run_id


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(p_session_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Audit: start table layer session

# CELL ********************

try:
    table_session_id = start_table_layer(
        session_id=session_id,
        source_table_id=source_table_id,
        source_table_name=BRONZE_TABLE_NAME,
        layer=LAYER_SILVER,
        batch_id=batch_id,
        load_type=resolved_load_type.upper(),
    )
    print(f"[AUDIT] Started table layer session | table_session_id={table_session_id}")
except Exception as audit_start_error:
    print(f"[AUDIT] Warning — Could not start audit session: {audit_start_error}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## MAIN PIPELINE 
# All steps run inside a try/except/finally to guarantee audit logging.

# CELL ********************

# ---------------------------------------------------------------------------
# MAIN PIPELINE EXECUTION
# ---------------------------------------------------------------------------
try:

    # -----------------------------------------------------------------------
    #— Load JSON Mapping File
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("Loading Bronze-to-Silver mapping JSON")
    print("=" * 70)

    mapping: dict = load_mapping_json(MAPPING_PATH)
    column_mappings: list[dict] = mapping["columns"]

    # -----------------------------------------------------------------------
    # STEP 3 — Read Bronze Source Table
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(" Reading Bronze source table")
    print("=" * 70)

    bronze_df: DataFrame = read_bronze_table(
        source_table_name=BRONZE_TABLE_NAME,
        load_type=resolved_load_type,
        batch_id=batch_id,
    )
    source_row_count = bronze_df.count()

    # -----------------------------------------------------------------------
    # STEP 4 — Apply Column Transformations
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(" Applying column transformations")
    print("=" * 70)

    mapped_silver_df: DataFrame = apply_column_transformations(
        bronze_df=bronze_df,
        column_mappings=column_mappings,
        source_table_name=BRONZE_TABLE_NAME,
    )

    # -----------------------------------------------------------------------
    # DEDUPLICATION — Hash-based within batch + against Silver layer
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("DEDUP  | Deduplicating records")
    print("=" * 70)

    # Compute row hash for change detection
    hashed_silver_df: DataFrame = compute_row_hash(
        input_df=mapped_silver_df,
        exclude_columns=[INGESTION_TIMESTAMP_COLUMN],
    )

    # Remove duplicates within the current batch (keep latest by _loaded_at)
    deduped_batch_df: DataFrame = deduplicate_within_batch(
        input_df=hashed_silver_df,
        primary_key_columns=PRIMARY_KEY_COLUMNS,
    )

    # Skip records already in Silver with the same hash (no data change)
    # Only applies to incremental loads — full load always overwrites
    if resolved_load_type == LOAD_TYPE_INCREMENTAL:
        deduped_final_df: DataFrame = deduplicate_against_silver(
            incoming_df=deduped_batch_df,
            silver_table_name=SILVER_TABLE_NAME,
            primary_key_columns=PRIMARY_KEY_COLUMNS,
        )
    else:
        deduped_final_df = deduped_batch_df

    # Drop the internal hash column before writing to Silver
    mapped_silver_df_final: DataFrame = deduped_final_df.drop("__row_hash")

    # -----------------------------------------------------------------------
    # STEP 5 — Data Quality (DQ) Validation
    # -----------------------------------------------------------------------IN
    print("\n" + "=" * 70)
    print(" Running Data Quality validation")
    print("=" * 70)

    # DQ rules are defined per table. In production these would be loaded
    # from a DQ config table or JSON file. For now, define a base set of
    # common rules — the 'not_null' check on every PK column.
    dq_rules: list[dict] = [
        {"column": pk_column, "rule": "not_null"}
        for pk_column in PRIMARY_KEY_COLUMNS
    ]

    # can be appended here or loaded from a separate DQ config source.

    df_valid, df_rejected, rejected_row_count = run_dq_validation(
        input_df=mapped_silver_df_final,
        column_mappings=column_mappings,
        source_table=BRONZE_TABLE_NAME,
        quarantine_table_name=QUARANTINE_TABLE_NAME,
    )

    if rejected_row_count > 0:
        log_invalid_batch_records(
            df_rejected, 
            table_session_id=table_session_id,
            layer=LAYER_SILVER,
            target_table=SILVER_TABLE_NAME,
            record_key_column=config_load_table["primary_key"]
        )

    # If all rows are rejected, skip the MERGE step and mark as WARNING
    if df_valid.count() == 0:
        print(
            f" All {rejected_row_count:,} row(s) rejected by DQ. "
            "Skipping MERGE. Status=WARNING."
        )
        pipeline_status = STATUS_WARNING
        target_row_count = 0

    else:
        # -------------------------------------------------------------------
        # STEP 6 — Write to Silver (FULL LOAD or MERGE)
        # -------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("| Writing to Silver layer")
        print("=" * 70)

        write_stats: dict

        if resolved_load_type == LOAD_TYPE_FULL:
            write_stats = write_silver_full_load(
                valid_df=df_valid,
                silver_table_name=SILVER_TABLE_NAME,
            )
        else:
            write_stats = write_silver_merge(
                valid_df=df_valid,
                silver_table_name=SILVER_TABLE_NAME,
                primary_key_columns=PRIMARY_KEY_COLUMNS,
            )

        inserted_row_count = write_stats.get("inserted_row", 0)
        updated_row_count = write_stats.get("updated_row", 0)
        target_row_count = inserted_row_count + updated_row_count
        pipeline_status = STATUS_SUCCESS

        print(
            f"Silver write complete: "
            f"inserted={inserted_row_count:,} | updated={updated_row_count:,}"
        )

except Exception as pipeline_error:
    # Capture the full traceback for audit logging
    error_message = (
        f"{type(pipeline_error).__name__}: {str(pipeline_error)}\n"
        f"{traceback.format_exc()}"
    )
    pipeline_status = STATUS_FAILED
    print(f"[PIPELINE ERROR]\n{error_message}")

finally:
    # -----------------------------------------------------------------------
    # STEP 7 — Write Audit Log (always runs, even on failure)
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(" Writing audit log")
    print("=" * 70)

    is_final_step: bool = True  # Silver is the last layer for this notebook

    try:
        if table_session_id:
            finish_table_layer(
                table_session_id=table_session_id,
                layer=LAYER_SILVER,
                status=pipeline_status,
                is_final_table_step=is_final_step,
                source_row_count=source_row_count,
                target_row_count=target_row_count,
                inserted_row=inserted_row_count,
                updated_row=updated_row_count,
                deleted_row=0,
                rejected_row=rejected_row_count,
                error_message=error_message,
                error_type=(
                    error_message.split(":")[0] if error_message else None
                ),
                is_retryable=True if pipeline_status == STATUS_FAILED else False,
                write_detail=True,
            )
            print(
                f"[AUDIT] Session closed | status={pipeline_status} "
                f"| table_session_id={table_session_id}"
            )
        else:
            print("[AUDIT] No table_session_id — skipping finish_table_layer call.")
    except Exception as audit_finish_error:
        # Audit failure must NOT raise — log only
        print(
            f"[AUDIT] Warning — finish_table_layer failed: {audit_finish_error}. "
            "Audit record may be incomplete."
        )

    # Re-raise pipeline error AFTER audit log is written
    if pipeline_status == STATUS_FAILED and error_message:
        raise RuntimeError(
            f"Bronze → Silver pipeline FAILED for '{SILVER_TABLE_NAME}'. "
            f"See audit table (table_session_id={table_session_id}) for details.\n"
            f"{error_message}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## SUMMARY LOG

# CELL ********************

# ---------------------------------------------------------------------------
# SUMMARY — print final execution summary for observability
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("PIPELINE SUMMARY")
print("=" * 70)
print(f"  Notebook          : nb_ingest_bronze_silver_dev")
print(f"  Environment       : {run_env}")
print(f"  Session ID        : {session_id}")
print(f"  Batch ID          : {batch_id}")
print(f"  Run mode          : {current_run_mode}")
print(f"  Load type         : {resolved_load_type}")
print(f"  Source (Bronze)   : {BRONZE_TABLE_NAME}")
print(f"  Target (Silver)   : {SILVER_TABLE_NAME}")
print(f"  Source rows read  : {source_row_count:,}")
print(f"  Rows inserted     : {inserted_row_count:,}")
print(f"  Rows updated      : {updated_row_count:,}")
print(f"  Rows rejected     : {rejected_row_count:,}")
print(f"  Final status      : {pipeline_status}")
if error_message:
    print(f"  Error             : {error_message[:200]}...")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = {
    "notebook": "nb_ingest_bronze_silver_dev",
    "session_id": session_id,
    "batch_id": batch_id,
    "run_mode": current_run_mode,
    "load_type": resolved_load_type,
    "source_table": BRONZE_TABLE_NAME,
    "target_table": SILVER_TABLE_NAME,
    "source_rows_read": source_row_count,
    "rows_inserted": inserted_row_count,
    "rows_updated": updated_row_count,
    "rows_rejected": rejected_row_count,
    "final_status": pipeline_status,
    "error_message": error_message,
}

notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
