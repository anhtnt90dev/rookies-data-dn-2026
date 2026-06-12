# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "126c09a8-79bf-4e16-9e56-5e7c93311e29",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "6358469d-5cd2-48a3-8d0f-c9583b40d1fa",
# META       "known_lakehouses": [
# META         {
# META           "id": "126c09a8-79bf-4e16-9e56-5e7c93311e29"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

source_table_id = ""
source_system = ""
source_type = ""
source_name = ""
source_location = ""
load_type = ""
watermark_column = ""
source_to_bronze_mapping_path = ""
bronze_table_name = ""
batch_id = ""
run_mode = ""
session_id = ""
source_format = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import uuid

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_id = int(batch_id)
source_table_id =int(source_table_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# Common helpers
# =========================

def escape_sql(value):
    if value is None:
        return None
    return str(value).replace("'", "''")


def align_to_target_schema(df, target_table: str):
    target_schema = spark.table(target_table).schema
    exprs = []

    for field in target_schema:
        if field.name in df.columns:
            exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    return df.select(*exprs)


def apply_load_type_filter(df, watermark_before):
    if load_type == "FULL":
        return df

    if load_type == "INCREMENTAL":
        if watermark_column is None or watermark_column.strip() == "":
            raise ValueError("watermark_column is required for INCREMENTAL load")

        if watermark_before is None:
            return df

        return df.where(
            F.to_timestamp(F.col(watermark_column)) > F.lit(watermark_before)
        )

    raise ValueError(f"Unsupported load_type: {load_type}")


# =========================
# Audit helpers
# =========================

def get_table_session_id(session_id: str, source_table_id: int):
    rows = (
        spark.table("log.audit_table_session")
        .where(
            (F.col("session_id") == F.lit(session_id))
            & (F.col("source_table_id") == F.lit(source_table_id))
        )
        .select("id", "bronze_status")
        .limit(1)
        .collect()
    )

    if not rows:
        raise Exception(
            f"No audit_table_session found for session_id={session_id}, "
            f"source_table_id={source_table_id}"
        )

    return rows[0]["id"], rows[0]["bronze_status"]


def update_bronze_running(table_session_id: str, watermark_before=None):
    watermark_sql = "NULL"

    if watermark_before is not None:
        watermark_sql = f"TIMESTAMP('{watermark_before}')"

    spark.sql(f"""
        UPDATE log.audit_table_session
        SET bronze_status = 'RUNNING',
            table_session_status = 'RUNNING',
            load_type = '{load_type}',
            watermark_before = {watermark_sql},
            bronze_started_at = current_timestamp(),
            updated_at = current_timestamp()
        WHERE id = '{table_session_id}'
    """)


def update_bronze_success(table_session_id: str, watermark_after=None):
    watermark_sql = "NULL"

    if watermark_after is not None:
        watermark_sql = f"TIMESTAMP('{watermark_after}')"

    spark.sql(f"""
        UPDATE log.audit_table_session
        SET bronze_status = 'SUCCESS',
            table_session_status = 'RUNNING',
            watermark_after = {watermark_sql},
            bronze_ended_at = current_timestamp(),
            error_code = NULL,
            error_message = NULL,
            updated_at = current_timestamp()
        WHERE id = '{table_session_id}'
    """)


def update_bronze_failed(table_session_id: str, error_message: str):
    safe_error = escape_sql(error_message)

    spark.sql(f"""
        UPDATE log.audit_table_session
        SET bronze_status = 'FAILED',
            table_session_status = 'FAILED',
            bronze_ended_at = current_timestamp(),
            error_code = 'BRONZE_LOAD_FAILED',
            error_message = '{safe_error}',
            updated_at = current_timestamp()
        WHERE id = '{table_session_id}'
    """)

def update_bronze_skipped(table_session_id: str):
    spark.sql(f"""
        UPDATE log.audit_table_session
        SET bronze_status = 'SKIPPED',
            table_session_status = 'RUNNING',
            bronze_started_at = current_timestamp(),
            bronze_ended_at = current_timestamp(),
            error_code = NULL,
            error_message = NULL,
            updated_at = current_timestamp()
        WHERE id = '{table_session_id}'
    """)

def insert_audit_detail(
    table_session_id: str,
    detail_status: str,
    source_row_count: int = 0,
    inserted_row: int = 0,
    updated_row: int = 0,
    deleted_row: int = 0,
    rejected_row: int = 0,
    layer: str = "BRONZE",
    error_message: str = None
):
    detail_id = str(uuid.uuid4())
    safe_error_sql = "NULL"

    if detail_status == "FAILED" and error_message is not None:
        safe_error_sql = f"'{escape_sql(error_message)}'"

    spark.sql(f"""
        INSERT INTO log.audit_detail (
            id,
            table_session_id,
            detail_status,
            source_row_count,
            inserted_row,
            updated_row,
            deleted_row,
            rejected_row,
            layer,
            error_message,
            created_at,
            updated_at
        )
        VALUES (
            '{detail_id}',
            '{table_session_id}',
            '{detail_status}',
            {int(source_row_count)},
            {int(inserted_row)},
            {int(updated_row)},
            {int(deleted_row)},
            {int(rejected_row)},
            '{layer}',
            {safe_error_sql},
            current_timestamp(),
            current_timestamp()
        )
    """)


def get_file_session_summary(table_session_id: str):
    summary = (
        spark.table("log.audit_file_session")
        .where(F.col("table_session_id") == F.lit(table_session_id))
        .agg(
            F.sum(F.coalesce(F.col("file_row_count"), F.lit(0))).alias("source_row_count"),
            F.sum(F.coalesce(F.col("processed_row_count"), F.lit(0))).alias("inserted_row"),
            F.sum(F.coalesce(F.col("rejected_row_count"), F.lit(0))).alias("rejected_row"),
            F.sum(F.when(F.col("file_status") == F.lit("FAILED"), 1).otherwise(0)).alias("failed_count")
        )
        .collect()[0]
    )

    failed_rows = (
        spark.table("log.audit_file_session")
        .where(
            (F.col("table_session_id") == F.lit(table_session_id))
            & (F.col("file_status") == F.lit("FAILED"))
        )
        .select("source_file", "error_message")
        .collect()
    )

    error_message = None

    if failed_rows:
        error_message = " | ".join(
            [f"{row['source_file']}: {row['error_message']}" for row in failed_rows]
        )

    failed_count = summary["failed_count"] or 0

    return {
        "status": "FAILED" if failed_count > 0 else "SUCCESS",
        "source_row_count": summary["source_row_count"] or 0,
        "inserted_row": summary["inserted_row"] or 0,
        "rejected_row": summary["rejected_row"] or 0,
        "error_message": error_message
    }


# =========================
# Mapping helpers
# =========================

def read_mapping(mapping_path: str) -> dict:
    full_path = f"/lakehouse/default/{mapping_path}"

    with open(full_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_source_schema(df, mapping: dict):
    required_columns = [
        col["expression"]
        for col in mapping["columns"]
        if col["expression"] is not None
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing source columns: {missing_columns}")


def apply_source_to_bronze_mapping(
    df,
    mapping: dict,
    batch_id: int,
    source_system: str,
    source_name: str,
    source_file: str = None
):
    select_exprs = []

    for col in mapping["columns"]:
        target = col["target"]
        expression = col["expression"]

        if expression is not None:
            select_exprs.append(F.col(expression).alias(target))

        elif target == "_batch_id":
            select_exprs.append(F.lit(str(batch_id)).alias(target))

        elif target == "_loaded_at":
            select_exprs.append(F.current_timestamp().alias(target))

        elif target == "_source_system":
            select_exprs.append(F.lit(source_system).alias(target))

        elif target == "_source_name":
            select_exprs.append(F.lit(source_name).alias(target))

        elif target == "_source_file":
            select_exprs.append(F.lit(source_file).alias(target))

        else:
            select_exprs.append(F.lit(None).alias(target))

    return df.select(*select_exprs)


# =========================
# Watermark helpers
# =========================

def read_watermark(source_table_id: int):
    rows = (
        spark.table("cfg.watermark")
        .where(F.col("source_table_id") == F.lit(source_table_id))
        .select("watermark_value")
        .limit(1)
        .collect()
    )

    if not rows:
        return None

    return rows[0]["watermark_value"]


def update_watermark(source_table_id: int, watermark_after):
    if watermark_after is None:
        return

    spark.sql(f"""
        MERGE INTO cfg.watermark AS target
        USING (
            SELECT
                CAST({source_table_id} AS BIGINT) AS source_table_id,
                TIMESTAMP('{watermark_after}') AS watermark_value
        ) AS source
        ON target.source_table_id = source.source_table_id

        WHEN MATCHED THEN
            UPDATE SET
                target.watermark_value = source.watermark_value,
                target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN
            INSERT (
                source_table_id,
                watermark_value,
                created_at,
                updated_at
            )
            VALUES (
                source.source_table_id,
                source.watermark_value,
                current_timestamp(),
                current_timestamp()
            )
    """)


def get_max_watermark(df):
    if watermark_column is None or watermark_column.strip() == "":
        return None

    if watermark_column not in df.columns:
        raise ValueError(
            f"watermark_column '{watermark_column}' not found in source columns: {df.columns}"
        )

    watermark_after = (
        df
        .agg(F.max(F.to_timestamp(F.col(watermark_column))).alias("watermark_after"))
        .collect()[0]["watermark_after"]
    )

    row_count = df.count()

    if row_count > 0 and watermark_after is None:
        raise ValueError(
            f"watermark_column '{watermark_column}' exists but all values are NULL or invalid timestamp"
        )

    return watermark_after


# =========================
# File helpers
# =========================

def get_relative_source_file(source_file: str) -> str:
    marker = "Files/"

    if marker in source_file:
        return marker + source_file.split(marker, 1)[1]

    return source_file

def read_dirty_json_file_or_folder(path: str, max_size_mb: int = 50):

    file_size_bytes = sum(
        file.size
        for file in notebookutils.fs.ls(path)
    )

    file_size_mb = file_size_bytes / 1024 / 1024

    if file_size_mb > max_size_mb:
        raise ValueError(
            f"Dirty JSON file exceeds the supported size limit "
            f"({file_size_mb:.2f} MB > {max_size_mb} MB). "
            f"Please provide a valid JSON file or preprocess the file before Bronze ingestion."
        )

    content = "".join(
        row["value"]
        for row in spark.read.text(path).collect()
    )

    start_idx = content.find("[")
    end_idx = content.rfind("]")

    if start_idx == -1 or end_idx == -1:
        raise ValueError(f"Cannot find JSON array in path: {path}")

    json_text = content[start_idx:end_idx + 1]
    records = json.loads(json_text)

    return spark.createDataFrame(records)


def list_files(path: str):
    return [
        file.path
        for file in notebookutils.fs.ls(path)
        if not file.isDir
    ]

def has_success_file_session(source_table_id: int, source_file: str) -> bool:
    rows = (
        spark.table("log.audit_file_session")
        .where(
            (F.col("source_table_id") == F.lit(source_table_id))
            & (F.col("source_file") == F.lit(source_file))
            & (F.col("file_status") == F.lit("SUCCESS"))
        )
        .limit(1)
        .collect()
    )

    return len(rows) > 0

def get_recovery_files(batch_id: int, source_table_id: int) -> list:
    rows = (
        spark.table("log.audit_file_session")
        .where(
            (F.col("batch_id") == F.lit(batch_id))
            & (F.col("source_table_id") == F.lit(source_table_id))
            & (F.col("file_status").isin("FAILED", "RUNNING", "NOT_RUN"))
        )
        .select("source_file")
        .distinct()
        .collect()
    )

    return [row["source_file"] for row in rows]


def register_file_sessions(table_session_id: str, files: list) -> list:
    files_to_process = []

    for source_file in files:
        if not has_success_file_session(source_table_id, source_file):
            files_to_process.append(source_file)

    if not files_to_process:
        return []

    rows = [
        (
            str(uuid.uuid4()),
            session_id,
            table_session_id,
            source_table_id,
            batch_id,
            source_file,
            "NOT_RUN"
        )
        for source_file in files_to_process
    ]

    df = spark.createDataFrame(
        rows,
        [
            "id",
            "session_id",
            "table_session_id",
            "source_table_id",
            "batch_id",
            "source_file",
            "file_status"
        ]
    )

    df = (
        df
        .withColumn("id", F.col("id").cast("string"))
        .withColumn("session_id", F.col("session_id").cast("string"))
        .withColumn("table_session_id", F.col("table_session_id").cast("string"))
        .withColumn("source_table_id", F.col("source_table_id").cast("bigint"))
        .withColumn("batch_id", F.col("batch_id").cast("bigint"))
        .withColumn("source_file", F.col("source_file").cast("string"))
        .withColumn("file_status", F.col("file_status").cast("string"))
        .withColumn("file_row_count", F.lit(None).cast("int"))
        .withColumn("processed_row_count", F.lit(None).cast("int"))
        .withColumn("rejected_row_count", F.lit(0).cast("int"))
        .withColumn("error_code", F.lit(None).cast("string"))
        .withColumn("error_message", F.lit(None).cast("string"))
        .withColumn("retry_count", F.lit(0).cast("int"))
        .withColumn("last_retry_at", F.lit(None).cast("timestamp"))
        .withColumn("started_at", F.lit(None).cast("timestamp"))
        .withColumn("completed_at", F.lit(None).cast("timestamp"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    df.write.format("delta").mode("append").saveAsTable("log.audit_file_session")

    return files_to_process


def update_file_sessions_running(table_session_id: str, files_to_process: list):
    if not files_to_process:
        return

    rows = [(source_file,) for source_file in files_to_process]

    df = spark.createDataFrame(rows, ["source_file"])
    df.createOrReplaceTempView("tmp_running_files")

    spark.sql(f"""
        MERGE INTO log.audit_file_session AS target
        USING (
            SELECT
                {batch_id} AS batch_id,
                {source_table_id} AS source_table_id,
                source_file
            FROM tmp_running_files
        ) AS source
        ON target.batch_id = source.batch_id
           AND target.source_table_id = source.source_table_id
           AND target.source_file = source.source_file

        WHEN MATCHED THEN
            UPDATE SET
                target.file_status = 'RUNNING',
                target.session_id = '{session_id}',
                target.table_session_id = '{table_session_id}',
                target.started_at = current_timestamp(),
                target.updated_at = current_timestamp()
    """)


def bulk_finish_file_sessions(table_session_id: str, file_results_list: list):
    if not file_results_list:
        return

    schema = StructType([
        StructField("source_file", StringType(), False),
        StructField("file_status", StringType(), False),
        StructField("file_row_count", LongType(), True),
        StructField("error_message", StringType(), True)
    ])

    rows = [
        Row(
            source_file=result["source_file"],
            file_status=result["status"],
            file_row_count=result.get("row_count"),
            error_message=result.get("error_message")
        )
        for result in file_results_list
    ]

    file_results_df = spark.createDataFrame(rows, schema)
    file_results_df.createOrReplaceTempView("tmp_file_results")

    spark.sql(f"""
        MERGE INTO log.audit_file_session AS target
        USING (
            SELECT
                '{session_id}' AS session_id,
                '{table_session_id}' AS table_session_id,
                {source_table_id} AS source_table_id,
                {batch_id} AS batch_id,
                source_file,
                file_status,
                file_row_count,
                CASE
                    WHEN file_status = 'SUCCESS' THEN file_row_count
                    ELSE NULL
                END AS processed_row_count,
                0 AS rejected_row_count,
                error_message,
                CASE
                    WHEN file_status = 'FAILED' THEN 'FILE_LOAD_FAILED'
                    ELSE NULL
                END AS error_code
            FROM tmp_file_results
        ) AS source
        ON target.batch_id = source.batch_id
           AND target.source_table_id = source.source_table_id
           AND target.source_file = source.source_file

        WHEN MATCHED THEN
            UPDATE SET
                target.session_id = source.session_id,
                target.table_session_id = source.table_session_id,
                target.file_status = source.file_status,
                target.file_row_count = source.file_row_count,
                target.processed_row_count = source.processed_row_count,
                target.rejected_row_count = source.rejected_row_count,
                target.error_code = source.error_code,
                target.error_message = source.error_message,
                target.completed_at = current_timestamp(),
                target.updated_at = current_timestamp()
    """)


# =========================
# Source processing
# =========================

def process_database_source(mapping: dict, watermark_before):

    source_df = spark.read.table(source_location)

    validate_source_schema(source_df, mapping)

    source_df = apply_load_type_filter(source_df, watermark_before)

    source_row_count = source_df.count()
    watermark_after = get_max_watermark(source_df)

    bronze_df = apply_source_to_bronze_mapping(
        df=source_df,
        mapping=mapping,
        batch_id=batch_id,
        source_system=source_system,
        source_name=source_name
    )

    bronze_df = align_to_target_schema(bronze_df, bronze_table_name)

    bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table_name)

    return {
        "watermark_after": watermark_after,
        "source_row_count": source_row_count,
        "inserted_row": source_row_count,
        "rejected_row": 0
    }


def process_file_source(mapping: dict, table_session_id: str, watermark_before):

    files = list_files(source_location)

    if not files:
        print(f"No files found in path: {source_location}")
        return {
            "watermark_after": None,
            "source_row_count": 0,
            "inserted_row": 0,
            "rejected_row": 0
        }

    if run_mode == "RECOVERY":
        files_to_process = get_recovery_files(batch_id, source_table_id)
    else:
        files_to_process = register_file_sessions(table_session_id, files)

    if not files_to_process:
        print(f"No files to process for source: {source_name}")
        return {
            "watermark_after": None,
            "source_row_count": 0,
            "inserted_row": 0,
            "rejected_row": 0
        }

    update_file_sessions_running(table_session_id, files_to_process)

    file_results = []
    failed_files = []
    total_watermark_max = None

    for source_file in files_to_process:
        current_file_log = {
            "source_file": source_file,
            "status": "RUNNING",
            "row_count": None,
            "error_message": None
        }

        try:
            if source_format.lower() == "json":
                source_df = read_dirty_json_file_or_folder(source_file, 50)

            elif source_format.lower() == "csv":
                source_df = spark.read.option("header", "true").csv(source_file)

            elif source_format.lower() == "parquet":
                source_df = spark.read.parquet(source_file)

            else:
                raise ValueError(f"Unsupported source_format: {source_format}")

            validate_source_schema(source_df, mapping)

            source_df = apply_load_type_filter(source_df, watermark_before)

            row_count = source_df.count()
            file_watermark_after = get_max_watermark(source_df)

            if file_watermark_after is not None:
                if total_watermark_max is None or file_watermark_after > total_watermark_max:
                    total_watermark_max = file_watermark_after

            relative_source_file = get_relative_source_file(source_file)

            bronze_df = apply_source_to_bronze_mapping(
                df=source_df,
                mapping=mapping,
                batch_id=batch_id,
                source_system=source_system,
                source_name=source_name,
                source_file=relative_source_file
            )

            bronze_df = align_to_target_schema(bronze_df, bronze_table_name)

            bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table_name)

            current_file_log["status"] = "SUCCESS"
            current_file_log["row_count"] = row_count

        except Exception as e:
            current_file_log["status"] = "FAILED"
            current_file_log["error_message"] = str(e)
            failed_files.append(source_file)

        file_results.append(current_file_log)

    bulk_finish_file_sessions(table_session_id, file_results)

    summary_metrics = get_file_session_summary(table_session_id)

    if failed_files:
        raise Exception(f"Failed files: {failed_files}")

    return {
        "watermark_after": total_watermark_max,
        "source_row_count": summary_metrics["source_row_count"],
        "inserted_row": summary_metrics["inserted_row"],
        "rejected_row": summary_metrics["rejected_row"]
    }


# =========================
# Main execution
# =========================

table_session_id, bronze_status = get_table_session_id(
    session_id=session_id,
    source_table_id=source_table_id
)

if run_mode == "RECOVERY" and bronze_status == "SKIPPED":
    update_bronze_skipped(table_session_id)
    insert_audit_detail(
        table_session_id=table_session_id,
        detail_status="SKIPPED",
        source_row_count=0,
        inserted_row=0,
        updated_row=0,
        deleted_row=0,
        rejected_row=0,
        layer="BRONZE",
        error_message=None
    )
    mssparkutils.notebook.exit("SKIPPED")

try:
    watermark_before = read_watermark(source_table_id)

    update_bronze_running(table_session_id, watermark_before)

    mapping = read_mapping(source_to_bronze_mapping_path)

    if source_type.lower() == "database":
        execution_result = process_database_source(mapping, watermark_before)

    elif source_type.lower() == "file":
        execution_result = process_file_source(mapping, table_session_id, watermark_before)

    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    watermark_after = execution_result["watermark_after"]

    if watermark_column and watermark_after is not None:
        update_watermark(source_table_id, watermark_after)

    update_bronze_success(table_session_id, watermark_after)

    insert_audit_detail(
        table_session_id=table_session_id,
        detail_status="SUCCESS",
        source_row_count=execution_result["source_row_count"],
        inserted_row=execution_result["inserted_row"],
        updated_row=0,
        deleted_row=0,
        rejected_row=execution_result["rejected_row"],
        layer="BRONZE",
        error_message=None
    )

except Exception as e:
    update_bronze_failed(table_session_id, str(e))

    if source_type.lower() == "file":
        summary = get_file_session_summary(table_session_id)

        insert_audit_detail(
            table_session_id=table_session_id,
            detail_status="FAILED",
            source_row_count=summary["source_row_count"],
            inserted_row=summary["inserted_row"],
            updated_row=0,
            deleted_row=0,
            rejected_row=summary["rejected_row"],
            layer="BRONZE",
            error_message=summary["error_message"] or str(e)
        )

    else:
        insert_audit_detail(
            table_session_id=table_session_id,
            detail_status="FAILED",
            source_row_count=0,
            inserted_row=0,
            updated_row=0,
            deleted_row=0,
            rejected_row=0,
            layer="BRONZE",
            error_message=str(e)
        )

    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
