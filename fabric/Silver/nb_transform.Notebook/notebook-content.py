# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
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

# Source configuration record identifier (maps to cfg.source_tables.id)
source_config_id: int = 1

# Batch identifier for this pipeline run
batch_id: int = 0

# Pipeline session identifier for audit tracing
session_id: str = ""

# Environment tag: 'dev' | 'staging' | 'prod'
run_env: str = "dev"

# Override load type at runtime: 'full' | 'incremental' | '' (use cfg value)
force_load_type: str = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Cell 2 — IMPORTS & SETUP

# CELL ********************

# ---------------------------------------------------------------------------
# IMPORTS & CONSTANTS
# ---------------------------------------------------------------------------
import json
import hashlib
import traceback
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

# Config / metadata tables
SOURCE_CONFIG_TABLE: str = "cfg.source_tables"
NEXT_RUN_MODE_TABLE: str = "cfg.next_run_mode"
AUDIT_TABLE_SESSION: str = "audit.audit_table_session"

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
try:
    spark  # noqa: F821  — already available in Fabric runtime
except NameError:
    spark = SparkSession.builder.appName("nb_ingest_bronze_silver_dev").getOrCreate()

# ---------------------------------------------------------------------------
# IMPORT AUDIT LOGGING HELPER
# On Microsoft Fabric, notebooks can be referenced with %run.
# Uncomment the line below when running inside Fabric:
#   %run /nb_audit_logging_helper_dev
# For local testing, the functions start_table_layer / finish_table_layer
# must be available from the imported notebook's scope.
# ---------------------------------------------------------------------------
# %run /nb_audit_logging_helper_dev

print(f"[SETUP] Bronze → Silver notebook initialised | env={run_env} | batch_id={batch_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Cell 3 — UTILITY FUNCTIONS
# Reusable functions used throughout the pipeline steps.

# CELL ********************

# ===========================================================================
# SECTION 3A — CONFIG & MAPPING UTILITIES
# ===========================================================================

def read_source_config(config_id: int) -> dict:
    """
    Read a single source configuration row from cfg.source_tables.

    Parameters
    ----------
    config_id : int
        The primary key (id) of the source configuration record.

    Returns
    -------
    dict
        A dictionary containing all columns of the matched configuration row.

    Raises
    ------
    ValueError
        If no active configuration row is found for the given id.
    """
    config_df = (
        spark.table(SOURCE_CONFIG_TABLE)
        .filter((F.col("id") == config_id) & (F.col("is_active") == True))  # noqa: E712
        .limit(1)
    )

    if config_df.count() == 0:
        raise ValueError(
            f"[CONFIG] No active configuration found in '{SOURCE_CONFIG_TABLE}' "
            f"for id={config_id}. Pipeline cannot continue."
        )

    config_row: dict = config_df.first().asDict()
    print(
        f"[CONFIG] Loaded config | source={config_row.get('source_name')} "
        f"| bronze={config_row.get('bronze_table_name')} "
        f"| silver={config_row.get('silver_table_name')} "
        f"| load_type={config_row.get('load_type')}"
    )
    return config_row


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

    try:
        with open(mapping_path, "r", encoding="utf-8") as mapping_file:
            mapping: dict = json.load(mapping_file)
    except FileNotFoundError:
        raise ValueError(f"[MAPPING] Mapping file not found: '{mapping_path}'")
    except json.JSONDecodeError as json_error:
        raise ValueError(
            f"[MAPPING] Mapping file is malformed JSON: '{mapping_path}' — {json_error}"
        )

    missing_keys = required_keys - mapping.keys()
    if missing_keys:
        raise ValueError(
            f"[MAPPING] Mapping file '{mapping_path}' is missing required keys: {missing_keys}"
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
    watermark_column: str | None = None,
    last_watermark_value: Any = None,
) -> DataFrame:
    """
    Read the Bronze Delta table as a Spark DataFrame.

    For FULL load, returns all rows.
    For INCREMENTAL load, filters rows where watermark_column > last_watermark_value.

    Parameters
    ----------
    source_table_name : str
        Fully qualified Bronze table name, e.g. 'bronze.agent'.
    load_type : str
        'full' or 'incremental'.
    watermark_column : str or None
        Column used for incremental extraction (e.g. 'updated_at').
    last_watermark_value : Any or None
        The previous high-water mark value. Rows with a value strictly greater
        than this are extracted.

    Returns
    -------
    DataFrame
        Raw Bronze DataFrame with no transformations applied.

    Raises
    ------
    AnalysisException
        Re-raised if the Bronze table does not exist.
    ValueError
        If incremental load is requested but watermark_column is not provided.
    """
    try:
        bronze_df: DataFrame = spark.table(source_table_name)
    except AnalysisException as analysis_error:
        raise AnalysisException(
            f"[BRONZE] Table '{source_table_name}' does not exist or is not accessible: "
            f"{analysis_error}"
        )

    if load_type.lower() == LOAD_TYPE_INCREMENTAL:
        if not watermark_column:
            raise ValueError(
                f"[BRONZE] Incremental load requires 'watermark_column' "
                f"but none was provided for table '{source_table_name}'."
            )
        if last_watermark_value is not None:
            bronze_df = bronze_df.filter(
                F.col(watermark_column) > F.lit(last_watermark_value)
            )
            print(
                f"[BRONZE] Incremental filter applied: {watermark_column} > {last_watermark_value}"
            )
        else:
            print(
                f"[BRONZE] Incremental mode selected but no watermark value found. "
                "Reading all rows (first run)."
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
    select_expressions.append(
        F.current_timestamp().alias(INGESTION_TIMESTAMP_COLUMN)
    )

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

# ===========================================================================
# SECTION 3E — DATA QUALITY VALIDATION ENGINE
# ===========================================================================

# ---------------------------------------------------------------------------
# Common (generic) validators
# ---------------------------------------------------------------------------

def _validate_is_not_null(column_name: str) -> F.Column:
    """Return a boolean Column that is True when the column value IS NULL."""
    return F.col(column_name).isNull()


def _validate_is_not_empty(column_name: str) -> F.Column:
    """Return True when a string column is NULL or an empty string."""
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


def _validate_not_in_set(column_name: str, allowed_values: list) -> F.Column:
    """Return True when the column value is NOT in the allowed value set."""
    return ~F.col(column_name).isin(allowed_values)


def _validate_is_negative(column_name: str) -> F.Column:
    """Return True when a numeric column holds a negative value."""
    return F.col(column_name) < 0


# ---------------------------------------------------------------------------
# Business (domain-specific) validators
# ---------------------------------------------------------------------------

def _validate_foreign_key_missing(
    column_name: str,
    reference_table: str,
    reference_column: str,
) -> F.Column:
    """
    Return True when a key value in column_name does not exist in the
    reference_table.reference_column (i.e. referential integrity violation).

    Parameters
    ----------
    column_name : str
        Column in the current DataFrame to check.
    reference_table : str
        Fully qualified reference table, e.g. 'silver.agent'.
    reference_column : str
        Column in the reference table containing valid key values.

    Returns
    -------
    pyspark.sql.Column
        Boolean column — True means key is MISSING (failing).
        Returns a literal False column if the reference table cannot be read.
    """
    try:
        ref_df: DataFrame = spark.table(reference_table).select(
            F.col(reference_column).alias("__ref_key")
        ).distinct()
        # Broadcast small reference tables for performance
        valid_keys = set(
            row["__ref_key"] for row in ref_df.collect()
        )
        return ~F.col(column_name).isin(list(valid_keys))
    except AnalysisException as analysis_error:
        print(
            f"[DQ] Warning — Could not read reference table '{reference_table}': "
            f"{analysis_error}. Skipping FK check for '{column_name}'."
        )
        return F.lit(False)  # Don't fail rows if reference table is unavailable


def _validate_date_order(
    start_column: str,
    end_column: str,
) -> F.Column:
    """
    Return True when start_date >= end_date (i.e. start must be BEFORE end).

    Parameters
    ----------
    start_column : str
        Column holding the start date/timestamp.
    end_column : str
        Column holding the end date/timestamp.

    Returns
    -------
    pyspark.sql.Column
        True when start_column >= end_column (violates the rule).
    """
    return (
        F.col(start_column).isNotNull()
        & F.col(end_column).isNotNull()
        & (F.col(start_column) >= F.col(end_column))
    )


# ---------------------------------------------------------------------------
# DQ Rule dispatcher
# ---------------------------------------------------------------------------

# Registry mapping rule type strings to validator factory functions.
# To add a new rule type, register it here — no other code needs to change.
_DQ_RULE_REGISTRY: dict[str, callable] = {
    "not_null": lambda rule: _validate_is_not_null(rule["column"]),
    "not_empty": lambda rule: _validate_is_not_empty(rule["column"]),
    "in_set": lambda rule: _validate_not_in_set(rule["column"], rule["values"]),
    "non_negative": lambda rule: _validate_is_negative(rule["column"]),
    "foreign_key": lambda rule: _validate_foreign_key_missing(
        rule["column"], rule["reference_table"], rule["reference_column"]
    ),
    "date_order": lambda rule: _validate_date_order(
        rule["start_column"], rule["end_column"]
    ),
}


def run_dq_validation(
    input_df: DataFrame,
    dq_rules: list[dict],
    quarantine_table_name: str,
) -> tuple[DataFrame, DataFrame, int]:
    """
    Apply all DQ rules to the mapped DataFrame.

    For each rule, a Boolean failure condition column is computed and rows
    that violate ANY rule are separated into df_rejected with a descriptive
    '__dq_failure_reason' column.

    DQ Rule structure examples
    --------------------------
    Common rules:
        {"column": "agent_id",   "rule": "not_null"}
        {"column": "status",     "rule": "in_set", "values": ["active", "inactive"]}
        {"column": "premium",    "rule": "non_negative"}

    Business rules:
        {"column": "agent_id",   "rule": "foreign_key",
         "reference_table": "silver.agent", "reference_column": "agent_id"}
        {"rule": "date_order", "start_column": "policy_start_date", "end_column": "policy_end_date"}

    Parameters
    ----------
    input_df : DataFrame
        Mapped Silver DataFrame (output of apply_column_transformations).
    dq_rules : list[dict]
        List of rule definitions.
    quarantine_table_name : str
        Fully qualified quarantine table where rejected rows will be written.

    Returns
    -------
    tuple[DataFrame, DataFrame, int]
        (df_valid, df_rejected, rejected_row_count)

    Raises
    ------
    ValueError
        If an unknown rule type is encountered.
    """
    if not dq_rules:
        print("[DQ] No DQ rules configured. Skipping validation.")
        return input_df, spark.createDataFrame([], input_df.schema), 0

    # Build per-rule failure condition columns and track reasons
    failure_condition_columns: list[F.Column] = []
    rule_reason_columns: list[F.Column] = []

    for dq_rule in dq_rules:
        rule_type: str = dq_rule.get("rule", "")

        if rule_type not in _DQ_RULE_REGISTRY:
            raise ValueError(
                f"[DQ] Unknown rule type '{rule_type}'. "
                f"Supported types: {list(_DQ_RULE_REGISTRY.keys())}"
            )

        failure_condition: F.Column = _DQ_RULE_REGISTRY[rule_type](dq_rule)
        failure_condition_columns.append(failure_condition)

        # Build human-readable reason string for this rule
        column_ref: str = dq_rule.get("column", dq_rule.get("start_column", "unknown"))
        rule_reason_columns.append(
            F.when(failure_condition, F.lit(f"{rule_type}:{column_ref}"))
        )

    # Combine all failure reasons into a single pipe-delimited string
    combined_failure_reason: F.Column = F.concat_ws(
        "|",
        *[F.coalesce(reason, F.lit("")) for reason in rule_reason_columns],
    )

    # Any rule failing marks the row as rejected
    is_any_rule_failing: F.Column = F.lit(False)
    for condition in failure_condition_columns:
        is_any_rule_failing = is_any_rule_failing | condition

    # Tag input with failure reason (empty string = row passed)
    tagged_df: DataFrame = input_df.withColumn(
        DQ_FAILURE_REASON_COLUMN,
        F.when(is_any_rule_failing, combined_failure_reason).otherwise(F.lit(None)),
    )

    df_valid: DataFrame = tagged_df.filter(
        F.col(DQ_FAILURE_REASON_COLUMN).isNull()
    ).drop(DQ_FAILURE_REASON_COLUMN)

    df_rejected: DataFrame = tagged_df.filter(
        F.col(DQ_FAILURE_REASON_COLUMN).isNotNull()
    )

    rejected_row_count: int = df_rejected.count()
    valid_row_count: int = df_valid.count()

    print(
        f"[DQ] Validation results: "
        f"valid={valid_row_count:,} | rejected={rejected_row_count:,}"
    )

    # Write rejected rows to quarantine table
    if rejected_row_count > 0:
        _write_to_quarantine(df_rejected, quarantine_table_name)

    return df_valid, df_rejected, rejected_row_count


def _write_to_quarantine(rejected_df: DataFrame, quarantine_table_name: str) -> None:
    """
    Write rejected rows to the quarantine Delta table.

    Adds a '__quarantined_at' timestamp before writing.
    Uses append mode so quarantine data accumulates over time.

    Parameters
    ----------
    rejected_df : DataFrame
        Rejected rows with '__dq_failure_reason' column.
    quarantine_table_name : str
        Target quarantine table name (e.g. 'silver_quarantine.agent').
    """
    quarantine_df: DataFrame = rejected_df.withColumn(
        "__quarantined_at", F.current_timestamp()
    )
    try:
        (
            quarantine_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_table_name)
        )
        print(
            f"[DQ] Wrote {rejected_df.count():,} rejected row(s) "
            f"to quarantine table '{quarantine_table_name}'."
        )
    except Exception as quarantine_error:
        # Quarantine write failure must NOT block the main pipeline
        print(
            f"[DQ] Warning — Failed to write to quarantine table "
            f"'{quarantine_table_name}': {quarantine_error}"
        )

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
# ## Cell 4 — STEP 1: Read Config & Determine Run Mode

# CELL ********************

# ---------------------------------------------------------------------------
# STEP 1 — Read source configuration and resolve run mode
# ---------------------------------------------------------------------------

print("=" * 70)
print(f"STEP 1 | Reading config for source_config_id={source_config_id}")
print("=" * 70)

# Read the configuration row for this pipeline invocation
config_row: dict = read_source_config(source_config_id)

# Resolve load type: notebook parameter overrides config table value
resolved_load_type: str = (
    force_load_type.lower()
    if force_load_type
    else config_row.get("load_type", LOAD_TYPE_FULL).lower()
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

# MARKDOWN ********************

# ---
# ## Cell 5 — MAIN PIPELINE (Steps 2–7)
# All steps run inside a try/except/finally to guarantee audit logging.

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

# ---------------------------------------------------------------------------
# Audit: start table layer session
# ---------------------------------------------------------------------------
try:
    table_session_id = start_table_layer(
        session_id=session_id,
        source_table_id=source_config_id,
        source_table_name=BRONZE_TABLE_NAME,
        layer=LAYER_SILVER,
        batch_id=batch_id,
        target_table_name=SILVER_TABLE_NAME,
        load_type=resolved_load_type.upper(),
    )
    print(f"[AUDIT] Started table layer session | table_session_id={table_session_id}")
except Exception as audit_start_error:
    print(f"[AUDIT] Warning — Could not start audit session: {audit_start_error}")

# ---------------------------------------------------------------------------
# MAIN PIPELINE EXECUTION
# ---------------------------------------------------------------------------
try:

    # -----------------------------------------------------------------------
    # STEP 2 — Load JSON Mapping File
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 2 | Loading Bronze-to-Silver mapping JSON")
    print("=" * 70)

    mapping: dict = load_mapping_json(MAPPING_PATH)
    column_mappings: list[dict] = mapping["columns"]

    # -----------------------------------------------------------------------
    # STEP 3 — Read Bronze Source Table
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 3 | Reading Bronze source table")
    print("=" * 70)

    # For incremental, retrieve last watermark from control table
    last_watermark_value = None
    if resolved_load_type == LOAD_TYPE_INCREMENTAL and WATERMARK_COLUMN:
        try:
            watermark_row = (
                spark.table(NEXT_RUN_MODE_TABLE)
                .select("last_watermark_value")
                .limit(1)
                .first()
            )
            last_watermark_value = watermark_row["last_watermark_value"] if watermark_row else None
        except Exception as watermark_error:
            print(f"[STEP 3] Warning — Could not read watermark: {watermark_error}. Reading all rows.")

    bronze_df: DataFrame = read_bronze_table(
        source_table_name=BRONZE_TABLE_NAME,
        load_type=resolved_load_type,
        watermark_column=WATERMARK_COLUMN or None,
        last_watermark_value=last_watermark_value,
    )
    source_row_count = bronze_df.count()

    # -----------------------------------------------------------------------
    # STEP 4 — Apply Column Transformations
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 4 | Applying column transformations")
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
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 5 | Running Data Quality validation")
    print("=" * 70)

    # DQ rules are defined per table. In production these would be loaded
    # from a DQ config table or JSON file. For now, define a base set of
    # common rules — the 'not_null' check on every PK column.
    dq_rules: list[dict] = [
        {"column": pk_column, "rule": "not_null"}
        for pk_column in PRIMARY_KEY_COLUMNS
    ]
    # NOTE: Table-specific business rules (foreign_key, date_order, in_set)
    # can be appended here or loaded from a separate DQ config source.

    df_valid, df_rejected, rejected_row_count = run_dq_validation(
        input_df=mapped_silver_df_final,
        dq_rules=dq_rules,
        quarantine_table_name=QUARANTINE_TABLE_NAME,
    )

    # If all rows are rejected, skip the MERGE step and mark as WARNING
    if df_valid.count() == 0:
        print(
            f"[STEP 5] All {rejected_row_count:,} row(s) rejected by DQ. "
            "Skipping MERGE. Status=WARNING."
        )
        pipeline_status = STATUS_WARNING
        target_row_count = 0

    else:
        # -------------------------------------------------------------------
        # STEP 6 — Write to Silver (FULL LOAD or MERGE)
        # -------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 6 | Writing to Silver layer")
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
            f"[STEP 6] Silver write complete: "
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
    print("STEP 7 | Writing audit log")
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
# ## Cell 6 — SUMMARY LOG

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
