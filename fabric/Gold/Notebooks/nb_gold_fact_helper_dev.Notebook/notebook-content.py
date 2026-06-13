# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b883e6d2-ee4b-4338-a694-4b81d338dd49",
# META       "default_lakehouse_name": "lh_insurance_dev",
# META       "default_lakehouse_workspace_id": "ddc0f61e-f221-421b-a87b-f80ffce2c8df",
# META       "known_lakehouses": [
# META         {
# META           "id": "b883e6d2-ee4b-4338-a694-4b81d338dd49"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run nb_audit_logging_helper_dev

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import uuid
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
CFG_SCHEMA = "cfg"
LOG_SCHEMA = "log"

UNKNOWN_KEY = -1
GOLD_LAYER = Layer.GOLD
DEFAULT_PIPELINE_NAME = "pl_gold_ingestion_dev"
DEFAULT_FACT_TABLE = "fact_policy"

CFG_DIM_FACT_TABLE = "cfg.dim_fact_table"

REQUIRED_AUDIT_TABLES = [
    AUDIT_SESSION_TABLE,
    AUDIT_TABLE_SESSION_TABLE,
    AUDIT_DETAIL_TABLE,
    INVALID_RECORD_TABLE,
]

FACT_SPECS = {
    "fact_policy": {
        "target_table": "gold.fact_policy",
        "source_table": "silver.policy",
        "context_tables": ["silver.quotation"],
        "cfg_dim_fact_table_id": 17,
        "upsert_key": "policy_id",
        "source_required_columns": [
            "policy_id",
            "quotation_id",
            "customer_id",
            "provider_code",
            "policy_number",
            "policy_start_date",
            "policy_end_date",
            "policy_status",
            "premium_amount",
            "operation_type",
            "is_deleted",
            "issued_at",
            "last_updated_at",
            "_batch_id",
            "_loaded_at",
            "_source_system",
        ],
        "context_required_columns": {
            "silver.quotation": [
                "quotation_id",
                "agent_id",
                "package_code",
                "quotation_at",
                "updated_at",
                "_batch_id",
                "_loaded_at",
            ],
        },
        "target_required_columns": [
            "policy_id",
            "policy_number",
            "quotation_id",
            "customer_id",
            "provider_code",
            "policy_key",
            "quotation_key",
            "customer_key",
            "provider_key",
            "agent_key",
            "package_key",
            "policy_status_key",
            "issued_date_key",
            "policy_start_date_key",
            "policy_end_date_key",
            "vehicle_key",
            "premium_amount",
            "created_at",
            "updated_at",
            "_batch_id",
            "_source_system",
            "pipeline_run_id",
            "is_deleted",
            "deleted_at",
            "delete_batch_id",
        ],
        "required_dimensions": {
            "gold.dim_date": {
                "key_column": "date_key",
                "required_columns": ["date_key", "full_date"],
                "requires_unknown": False,
                "scd2": False,
            },
            "gold.dim_policy": {
                "key_column": "policy_key",
                "business_column": "policy_id",
                "required_columns": ["policy_key", "policy_id"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_quotation": {
                "key_column": "quotation_key",
                "business_column": "quotation_id",
                "required_columns": ["quotation_key", "quotation_id"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_customer": {
                "key_column": "customer_key",
                "business_column": "customer_id",
                "required_columns": ["customer_key", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_provider": {
                "key_column": "provider_key",
                "business_column": "provider_code",
                "required_columns": ["provider_key", "provider_code", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_agent": {
                "key_column": "agent_key",
                "business_column": "agent_id",
                "required_columns": ["agent_key", "agent_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_package": {
                "key_column": "package_key",
                "business_column": "package_code",
                "required_columns": ["package_key", "package_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_policy_status": {
                "key_column": "policy_status_key",
                "business_column": "policy_status_code",
                "required_columns": ["policy_status_key", "policy_status_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_vehicle": {
                "key_column": "vehicle_key",
                "business_column": "customer_id",
                "required_columns": ["vehicle_key", "vehicle_id", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
        },
    },
    "fact_quotation": {
        "target_table": "gold.fact_quotation",
        "source_table": "silver.quotation",
        "cfg_dim_fact_table_id": 15,
        "upsert_key": "quotation_id",
        "source_required_columns": [
            "quotation_id",
            "customer_id",
            "agent_id",
            "provider_code",
            "package_code",
            "quotation_status",
            "premium_amount",
            "quotation_at",
            "quotation_expiry_at",
            "_batch_id",
            "_loaded_at",
            "_source_system",
        ],
        "target_required_columns": [
            "quotation_id",
            "customer_id",
            "agent_id",
            "provider_code",
            "quotation_key",
            "customer_key",
            "agent_key",
            "provider_key",
            "package_key",
            "quotation_status_key",
            "quotation_date_key",
            "quotation_expiry_date_key",
            "vehicle_key",
            "premium_amount",
            "converted_flag",
            "created_at",
            "updated_at",
            "_batch_id",
            "_source_system",
            "pipeline_run_id",
            "is_deleted",
            "deleted_at",
            "delete_batch_id",
        ],
        "required_dimensions": {
            "gold.dim_date": {
                "key_column": "date_key",
                "required_columns": ["date_key", "full_date"],
                "requires_unknown": False,
                "scd2": False,
            },
            "gold.dim_quotation": {
                "key_column": "quotation_key",
                "business_column": "quotation_id",
                "required_columns": ["quotation_key", "quotation_id"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_customer": {
                "key_column": "customer_key",
                "business_column": "customer_id",
                "required_columns": ["customer_key", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_agent": {
                "key_column": "agent_key",
                "business_column": "agent_id",
                "required_columns": ["agent_key", "agent_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_provider": {
                "key_column": "provider_key",
                "business_column": "provider_code",
                "required_columns": ["provider_key", "provider_code", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_package": {
                "key_column": "package_key",
                "business_column": "package_code",
                "required_columns": ["package_key", "package_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_quotation_status": {
                "key_column": "quotation_status_key",
                "business_column": "quotation_status_code",
                "required_columns": ["quotation_status_key", "quotation_status_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_vehicle": {
                "key_column": "vehicle_key",
                "business_column": "customer_id",
                "required_columns": ["vehicle_key", "vehicle_id", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
        },
    },
    "fact_quotation_item": {
        "target_table": "gold.fact_quotation_item",
        "source_table": "silver.quotation_item",
        "context_tables": ["silver.quotation"],
        "cfg_dim_fact_table_id": 16,
        "upsert_key": "quotation_item_id",
        "source_required_columns": [
            "quotation_item_id",
            "quotation_id",
            "coverage_type",
            "coverage_amount",
            "deductible_amount",
            "_batch_id",
            "_loaded_at",
            "_source_system",
        ],
        "context_required_columns": {
            "silver.quotation": [
                "quotation_id",
                "agent_id",
                "package_code",
                "quotation_status",
                "customer_id",
                "provider_code",
                "quotation_at",
                "updated_at",
                "_batch_id",
                "_loaded_at",
            ],
        },
        "target_required_columns": [
            "quotation_item_id",
            "quotation_id",
            "quotation_key",
            "quotation_date_key",
            "customer_key",
            "agent_key",
            "provider_key",
            "package_key",
            "quotation_status_key",
            "coverage_key",
            "vehicle_key",
            "coverage_amount",
            "deductible_amount",
            "created_at",
            "updated_at",
            "_batch_id",
            "_source_system",
            "pipeline_run_id",
            "is_deleted",
            "deleted_at",
            "delete_batch_id",
        ],
        "required_dimensions": {
            "gold.dim_date": {
                "key_column": "date_key",
                "required_columns": ["date_key", "full_date"],
                "requires_unknown": False,
                "scd2": False,
            },
            "gold.dim_quotation": {
                "key_column": "quotation_key",
                "business_column": "quotation_id",
                "required_columns": ["quotation_key", "quotation_id"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_customer": {
                "key_column": "customer_key",
                "business_column": "customer_id",
                "required_columns": ["customer_key", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_agent": {
                "key_column": "agent_key",
                "business_column": "agent_id",
                "required_columns": ["agent_key", "agent_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_provider": {
                "key_column": "provider_key",
                "business_column": "provider_code",
                "required_columns": ["provider_key", "provider_code", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_package": {
                "key_column": "package_key",
                "business_column": "package_code",
                "required_columns": ["package_key", "package_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_quotation_status": {
                "key_column": "quotation_status_key",
                "business_column": "quotation_status_code",
                "required_columns": ["quotation_status_key", "quotation_status_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_coverage": {
                "key_column": "coverage_key",
                "business_column": "coverage_type",
                "required_columns": ["coverage_key", "coverage_type"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_vehicle": {
                "key_column": "vehicle_key",
                "business_column": "customer_id",
                "required_columns": ["vehicle_key", "vehicle_id", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
        },
    },
    "fact_payment": {
        "target_table": "gold.fact_payment",
        "source_table": "silver.payment",
        "context_tables": ["silver.policy"],
        "cfg_dim_fact_table_id": 18,
        "upsert_key": "payment_id",
        "source_required_columns": [
            "payment_id",
            "policy_id",
            "transaction_reference",
            "payment_status",
            "payment_method",
            "payment_amount",
            "payment_at",
            "operation_type",
            "is_deleted",
            "_batch_id",
            "_loaded_at",
            "_source_system",
        ],
        "context_required_columns": {
            "silver.policy": [
                "policy_id",
                "customer_id",
                "provider_code",
                "issued_at",
                "last_updated_at",
                "_batch_id",
                "_loaded_at",
            ],
        },
        "target_required_columns": [
            "payment_id",
            "policy_id",
            "transaction_reference",
            "policy_key",
            "payment_status_key",
            "payment_method_key",
            "payment_date_key",
            "issued_date_key",
            "customer_key",
            "provider_key",
            "vehicle_key",
            "payment_amount",
            "created_at",
            "updated_at",
            "_batch_id",
            "_source_system",
            "pipeline_run_id",
            "is_deleted",
            "deleted_at",
            "delete_batch_id",
        ],
        "required_dimensions": {
            "gold.dim_date": {
                "key_column": "date_key",
                "required_columns": ["date_key", "full_date"],
                "requires_unknown": False,
                "scd2": False,
            },
            "gold.dim_policy": {
                "key_column": "policy_key",
                "business_column": "policy_id",
                "required_columns": ["policy_key", "policy_id"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_payment_status": {
                "key_column": "payment_status_key",
                "business_column": "payment_status_code",
                "required_columns": ["payment_status_key", "payment_status_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_payment_method": {
                "key_column": "payment_method_key",
                "business_column": "payment_method_code",
                "required_columns": ["payment_method_key", "payment_method_code"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_customer": {
                "key_column": "customer_key",
                "business_column": "customer_id",
                "required_columns": ["customer_key", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_provider": {
                "key_column": "provider_key",
                "business_column": "provider_code",
                "required_columns": ["provider_key", "provider_code", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_vehicle": {
                "key_column": "vehicle_key",
                "business_column": "customer_id",
                "required_columns": ["vehicle_key", "vehicle_id", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
        },
    },
    "fact_cancellation": {
        "target_table": "gold.fact_cancellation",
        "source_table": "silver.cancellation",
        "context_tables": ["silver.policy"],
        "cfg_dim_fact_table_id": 19,
        "upsert_key": "cancellation_id",
        "source_required_columns": [
            "cancellation_id",
            "policy_id",
            "cancellation_reason",
            "refund_amount",
            "cancellation_at",
            "operation_type",
            "is_deleted",
            "_batch_id",
            "_loaded_at",
            "_source_system",
        ],
        "context_required_columns": {
            "silver.policy": [
                "policy_id",
                "customer_id",
                "provider_code",
                "last_updated_at",
                "_batch_id",
                "_loaded_at",
            ],
        },
        "target_required_columns": [
            "cancellation_id",
            "policy_id",
            "policy_key",
            "cancellation_reason_key",
            "cancellation_date_key",
            "customer_key",
            "provider_key",
            "vehicle_key",
            "refund_amount",
            "created_at",
            "updated_at",
            "_batch_id",
            "_source_system",
            "pipeline_run_id",
            "is_deleted",
            "deleted_at",
            "delete_batch_id",
        ],
        "required_dimensions": {
            "gold.dim_date": {
                "key_column": "date_key",
                "required_columns": ["date_key", "full_date"],
                "requires_unknown": False,
                "scd2": False,
            },
            "gold.dim_policy": {
                "key_column": "policy_key",
                "business_column": "policy_id",
                "required_columns": ["policy_key", "policy_id"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_cancellation_reason": {
                "key_column": "cancellation_reason_key",
                "business_column": "cancellation_reason",
                "required_columns": ["cancellation_reason_key", "cancellation_reason"],
                "requires_unknown": True,
                "scd2": False,
            },
            "gold.dim_customer": {
                "key_column": "customer_key",
                "business_column": "customer_id",
                "required_columns": ["customer_key", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_provider": {
                "key_column": "provider_key",
                "business_column": "provider_code",
                "required_columns": ["provider_key", "provider_code", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
            "gold.dim_vehicle": {
                "key_column": "vehicle_key",
                "business_column": "customer_id",
                "required_columns": ["vehicle_key", "vehicle_id", "customer_id", "effective_from", "effective_to", "is_current"],
                "requires_unknown": True,
                "scd2": True,
            },
        },
    },
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def as_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def is_blank(value) -> bool:
    return value is None or str(value).strip() == ""


def normalize_fact_name(fact_table: str) -> str:
    if is_blank(fact_table) or str(fact_table).upper() == "ALL":
        return DEFAULT_FACT_TABLE
    return str(fact_table).split(".")[-1].strip()


def get_fact_spec(fact_table: str) -> Dict:
    fact_name = normalize_fact_name(fact_table)
    if fact_name not in FACT_SPECS:
        supported = ", ".join(sorted(FACT_SPECS.keys()))
        raise ValueError(f"Unsupported Gold fact table '{fact_table}'. Supported now: {supported}")
    return FACT_SPECS[fact_name]


def qualified(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


def table_exists(table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        try:
            spark.table(table_name).limit(0).count()
            return True
        except Exception:
            return False


def require_table(table_name: str) -> None:
    if not table_exists(table_name):
        raise Exception(f"Required table does not exist: {table_name}")


def get_table_columns(table_name: str) -> List[str]:
    require_table(table_name)
    return list(spark.table(table_name).columns)


def require_columns(table_name: str, required_columns: Iterable[str]) -> None:
    actual_columns = set(get_table_columns(table_name))
    missing_columns = [column for column in required_columns if column not in actual_columns]
    if missing_columns:
        raise Exception(f"{table_name} is missing required columns: {', '.join(missing_columns)}")


def require_table_has_rows(table_name: str, description: Optional[str] = None) -> None:
    require_table(table_name)
    if spark.table(table_name).limit(1).count() == 0:
        label = description or table_name
        raise Exception(f"Required table has no rows: {label}")


def require_unknown_member(table_name: str, key_column: str) -> None:
    require_columns(table_name, [key_column])
    unknown_count = spark.table(table_name).where(F.col(key_column) == F.lit(UNKNOWN_KEY)).limit(1).count()
    if unknown_count == 0:
        raise Exception(f"Missing Unknown member in {table_name}: {key_column} = {UNKNOWN_KEY}")


def require_scd2_window(table_name: str) -> None:
    require_columns(table_name, ["effective_from", "effective_to", "is_current"])
    invalid_count = (
        spark.table(table_name)
        .where(
            F.col("effective_from").isNull()
            | F.col("effective_to").isNull()
            | F.col("is_current").isNull()
            | (F.col("effective_from") > F.col("effective_to"))
        )
        .limit(1)
        .count()
    )
    if invalid_count > 0:
        raise Exception(f"SCD2 window check failed for {table_name}")


def require_cfg_fact_row(fact_table: str) -> int:
    spec = get_fact_spec(fact_table)
    require_columns(CFG_DIM_FACT_TABLE, ["id", "table_name", "table_type", "upsert_key", "is_active"])
    rows = (
        spark.table(CFG_DIM_FACT_TABLE)
        .where(
            (F.col("table_name") == F.lit(normalize_fact_name(fact_table)))
            & (F.col("table_type") == F.lit("FACT"))
            & (F.col("is_active") == F.lit(True))
        )
        .select("id", "upsert_key")
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise Exception(f"Expected exactly one active cfg.dim_fact_table row for {fact_table}, found {len(rows)}")
    if rows[0]["upsert_key"] != spec["upsert_key"]:
        raise Exception(
            f"cfg.dim_fact_table upsert_key mismatch for {fact_table}: "
            f"expected {spec['upsert_key']}, found {rows[0]['upsert_key']}"
        )
    return int(rows[0]["id"])


def get_table_dimensions_specs(fact_table: str) -> Dict:
    return get_fact_spec(fact_table)["required_dimensions"]


def get_cfg_fact_table_id(fact_table: str) -> int:
    if table_exists(CFG_DIM_FACT_TABLE):
        rows = (
            spark.table(CFG_DIM_FACT_TABLE)
            .where(F.col("table_name") == F.lit(normalize_fact_name(fact_table)))
            .select("id")
            .limit(1)
            .collect()
        )
        if rows:
            return int(rows[0]["id"])
    return int(get_fact_spec(fact_table)["cfg_dim_fact_table_id"])


def run_preflight_for_fact(fact_table: str = DEFAULT_FACT_TABLE, enable_audit: bool = True) -> Dict[str, List[str]]:
    spec = get_fact_spec(fact_table)
    checked = {"tables": [], "columns": [], "dimensions": [], "audit": []}

    require_table(spec["source_table"])
    require_columns(spec["source_table"], spec["source_required_columns"])
    checked["tables"].append(spec["source_table"])
    checked["columns"].append(spec["source_table"])

    for context_table in spec.get("context_tables", []):
        require_table(context_table)
        req_cols = spec["context_required_columns"][context_table]
        require_columns(context_table, req_cols)
        checked["tables"].append(context_table)
        checked["columns"].append(context_table)

    require_table(spec["target_table"])
    require_columns(spec["target_table"], spec["target_required_columns"])
    checked["tables"].append(spec["target_table"])
    checked["columns"].append(spec["target_table"])

    for dimension_table, dimension_spec in spec["required_dimensions"].items():
        require_table(dimension_table)
        # Dynamic required columns check
        dim_req = dimension_spec.get("required_columns")
        if not dim_req:
            dim_req = [dimension_spec["key_column"]]
            if dimension_spec.get("business_column"):
                dim_req.append(dimension_spec["business_column"])
            if dimension_spec.get("scd2"):
                dim_req.extend(["effective_from", "effective_to", "is_current"])
        require_columns(dimension_table, dim_req)
        require_table_has_rows(dimension_table, dimension_table)
        if dimension_spec.get("requires_unknown"):
            require_unknown_member(dimension_table, dimension_spec["key_column"])
        if dimension_spec.get("scd2"):
            require_scd2_window(dimension_table)
        checked["dimensions"].append(dimension_table)

    if as_bool(enable_audit, True):
        require_table(CFG_DIM_FACT_TABLE)
        cfg_id = require_cfg_fact_row(fact_table)
        checked["audit"].append(f"{CFG_DIM_FACT_TABLE}:{cfg_id}")
        for audit_table in REQUIRED_AUDIT_TABLES:
            require_table(audit_table)
            checked["audit"].append(audit_table)

    return checked


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def first_existing_column(df: DataFrame, candidate_columns: Iterable[str]) -> Optional[str]:
    available = set(df.columns)
    for column_name in candidate_columns:
        if column_name in available:
            return column_name
    return None


def filter_by_batch(df: DataFrame, batch_id) -> DataFrame:
    if is_blank(batch_id) or "_batch_id" not in df.columns:
        return df
    return df.where(F.col("_batch_id") == F.lit(str(batch_id)))


def date_key_expr(column_name: str):
    return F.date_format(F.to_date(F.col(column_name)), "yyyyMMdd").cast("int")


def dedupe_latest(df: DataFrame, key_columns: List[str], order_columns: List[str]) -> DataFrame:
    missing_keys = [column for column in key_columns if column not in df.columns]
    if missing_keys:
        raise Exception(f"Cannot dedupe; missing key columns: {', '.join(missing_keys)}")

    available_order_columns = [column for column in order_columns if column in df.columns]
    if available_order_columns:
        order_exprs = [F.col(column).desc_nulls_last() for column in available_order_columns]
    else:
        order_exprs = [F.lit(1)]

    window_spec = Window.partitionBy(*[F.col(column) for column in key_columns]).orderBy(*order_exprs)
    return df.withColumn("__gold_rn", F.row_number().over(window_spec)).where(F.col("__gold_rn") == 1).drop("__gold_rn")


def lookup_type1_key(
    df: DataFrame,
    dim_table: str,
    business_column: str,
    dim_business_column: str,
    key_column: str,
    output_column: str,
) -> DataFrame:
    require_columns(dim_table, [key_column, dim_business_column])
    if business_column not in df.columns:
        raise Exception(f"Missing lookup input column: {business_column}")

    base_df = df.drop(output_column) if output_column in df.columns else df
    dim_df = (
        spark.table(dim_table)
        .select(
            F.col(dim_business_column).alias("__dim_business_key"),
            F.col(key_column).cast("bigint").alias("__dim_key"),
        )
        .where(F.col("__dim_business_key").isNotNull())
        .dropDuplicates(["__dim_business_key"])
    )

    joined_df = base_df.join(dim_df, base_df[business_column] == dim_df["__dim_business_key"], "left")
    return (
        joined_df
        .drop("__dim_business_key")
        .withColumn(output_column, F.coalesce(F.col("__dim_key"), F.lit(UNKNOWN_KEY)).cast("bigint"))
        .drop("__dim_key")
    )


def lookup_scd2_key(
    df: DataFrame,
    dim_table: str,
    business_column: str,
    dim_business_column: str,
    event_timestamp_column: str,
    key_column: str,
    output_column: str,
) -> DataFrame:
    require_columns(dim_table, [key_column, dim_business_column, "effective_from", "effective_to"])
    for column_name in [business_column, event_timestamp_column]:
        if column_name not in df.columns:
            raise Exception(f"Missing SCD2 lookup input column: {column_name}")

    base_columns = [column for column in df.columns if column != output_column]
    row_id_column = f"__{output_column}_row_id"
    base_df = df.drop(output_column) if output_column in df.columns else df
    base_df = base_df.withColumn(row_id_column, F.monotonically_increasing_id())

    dim_df = spark.table(dim_table).select(
        F.col(dim_business_column).alias("__dim_business_key"),
        F.col(key_column).cast("bigint").alias("__dim_key"),
        F.col("effective_from").alias("__dim_effective_from"),
        F.col("effective_to").alias("__dim_effective_to"),
    )

    join_condition = (
        (base_df[business_column] == dim_df["__dim_business_key"])
        & (F.col(event_timestamp_column).cast("timestamp") >= F.col("__dim_effective_from"))
        & (F.col(event_timestamp_column).cast("timestamp") <= F.col("__dim_effective_to"))
    )

    ranked_df = (
        base_df
        .join(dim_df, join_condition, "left")
        .withColumn(
            "__scd2_rn",
            F.row_number().over(
                Window.partitionBy(row_id_column).orderBy(
                    F.col("__dim_effective_from").desc_nulls_last(),
                    F.col("__dim_key").desc_nulls_last(),
                )
            ),
        )
        .where(F.col("__scd2_rn") == 1)
    )

    return ranked_df.select(
        *[F.col(column) for column in base_columns],
        F.coalesce(F.col("__dim_key"), F.lit(UNKNOWN_KEY)).cast("bigint").alias(output_column),
    )


def count_missing_date_key_values(df: DataFrame, date_key_columns: Iterable[str], dim_date_table: str = "gold.dim_date") -> Dict[str, int]:
    require_columns(dim_date_table, ["date_key"])
    dim_dates_df = spark.table(dim_date_table).select(F.col("date_key").alias("__date_key")).dropDuplicates()
    missing_counts = {}

    for column_name in date_key_columns:
        if column_name not in df.columns:
            missing_counts[column_name] = -1
            continue

        null_count = df.where(F.col(column_name).isNull()).count()
        missing_ref_count = (
            df
            .where(F.col(column_name).isNotNull())
            .select(F.col(column_name).alias("__fact_date_key"))
            .dropDuplicates()
            .join(dim_dates_df, F.col("__fact_date_key") == F.col("__date_key"), "left_anti")
            .count()
        )
        missing_counts[column_name] = int(null_count + missing_ref_count)

    return missing_counts


def assert_fact_date_keys_exist(
    df: DataFrame,
    date_key_columns: Iterable[str],
    dim_date_table: str = "gold.dim_date",
) -> None:
    missing_counts = count_missing_date_key_values(df, date_key_columns, dim_date_table)
    failures = {column: count for column, count in missing_counts.items() if count != 0}
    if failures:
        detail = ", ".join([f"{column}={count}" for column, count in failures.items()])
        raise Exception(f"Fact date key validation failed against {dim_date_table}: {detail}")


def count_invalid_fk_values(df: DataFrame, key_column: str, dimension_table: str, dimension_key_column: str) -> int:
    require_columns(dimension_table, [dimension_key_column])
    if key_column not in df.columns:
        return -1

    dim_keys_df = spark.table(dimension_table).select(F.col(dimension_key_column).alias("__dimension_key")).dropDuplicates()
    invalid_count = (
        df
        .where(F.col(key_column).isNotNull() & (F.col(key_column) != F.lit(UNKNOWN_KEY)))
        .select(F.col(key_column).alias("__fact_key"))
        .dropDuplicates()
        .join(dim_keys_df, F.col("__fact_key") == F.col("__dimension_key"), "left_anti")
        .count()
    )
    null_count = df.where(F.col(key_column).isNull()).count()
    return int(invalid_count + null_count)


def count_unknown_keys(df: DataFrame, key_columns: Iterable[str]) -> Dict[str, int]:
    return {
        column_name: int(df.where(F.col(column_name) == F.lit(UNKNOWN_KEY)).count())
        for column_name in key_columns
        if column_name in df.columns
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_audit_batch_id(batch_id) -> int:
    if is_blank(batch_id):
        return 0
    try:
        return int(str(batch_id))
    except Exception as exc:
        raise ValueError(
            "Existing audit tables store batch_id as BIGINT. "
            "Use a numeric p_batch_id for audited Gold runs, or set p_enable_audit=False."
        ) from exc


def make_manual_pipeline_run_id(pipeline_name: str = DEFAULT_PIPELINE_NAME) -> str:
    return f"manual_{pipeline_name}_{int(time.time())}"


def start_gold_pipeline_audit(
    pipeline_name: str,
    pipeline_run_id: str,
    batch_id,
    run_mode: str = "NEW",
    enable_audit: bool = True,
) -> Optional[str]:
    if not as_bool(enable_audit, True):
        return None
    return start_pipeline_session(
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        batch_id=normalize_audit_batch_id(batch_id),
        run_mode=run_mode,
    )


def start_gold_table_audit(
    session_id: Optional[str],
    fact_table: str,
    batch_id,
    load_type: str = "FULL",
    enable_audit: bool = True,
) -> Optional[str]:
    if not as_bool(enable_audit, True) or is_blank(session_id):
        return None

    fact_name = normalize_fact_name(fact_table)
    return start_table_layer(
        session_id=str(session_id),
        source_table_id=get_cfg_fact_table_id(fact_name),
        source_table_name=f"gold.{fact_name}",
        layer=GOLD_LAYER,
        batch_id=normalize_audit_batch_id(batch_id),
        load_type=load_type,
    )


def finish_gold_table_audit(
    table_session_id: Optional[str],
    status: str,
    source_row_count: int = None,
    target_row_count: int = None,
    inserted_row: int = None,
    updated_row: int = None,
    deleted_row: int = None,
    rejected_row: int = None,
    error_code: str = None,
    error_message: str = None,
    error_type: str = None,
    is_retryable: bool = None,
    enable_audit: bool = True,
) -> None:
    if not as_bool(enable_audit, True) or is_blank(table_session_id):
        return

    finish_table_layer(
        table_session_id=str(table_session_id),
        layer=GOLD_LAYER,
        status=status,
        is_final_table_step=True,
        source_row_count=source_row_count,
        target_row_count=target_row_count,
        inserted_row=inserted_row,
        updated_row=updated_row,
        deleted_row=deleted_row,
        rejected_row=rejected_row,
        error_code=error_code,
        error_message=error_message,
        error_type=error_type,
        is_retryable=is_retryable,
        write_detail=True,
    )


def log_invalid_rows(
    df: DataFrame,
    table_session_id: Optional[str],
    target_table: str,
    record_key_column: str,
    error_column: str,
    error_reason: str,
    raw_columns: Iterable[str],
    max_rows: int = None,
) -> int:
    row_count = df.count()
    if row_count == 0 or is_blank(table_session_id):
        return int(row_count)

    selected_raw_columns = [column for column in raw_columns if column in df.columns]
    raw_json_expr = (
        F.to_json(F.struct(*[F.col(column) for column in selected_raw_columns]))
        if selected_raw_columns
        else F.lit(None).cast("string")
    )
    record_key_expr = F.col(record_key_column).cast("string") if record_key_column in df.columns else F.lit(None).cast("string")

    invalid_df = df.select(
        F.expr("uuid()").alias("id"),
        F.lit(str(table_session_id)).alias("table_session_id"),
        F.lit(None).cast("string").alias("file_session_id"),
        F.lit(GOLD_LAYER.value).alias("layer"),
        F.lit(target_table).alias("target_table"),
        record_key_expr.alias("record_key"),
        raw_json_expr.alias("raw_data"),
        F.lit(error_column).alias("error_column"),
        F.lit(error_reason).alias("error_reason"),
        F.lit(ErrorType.DATA.value).alias("error_type"),
        F.lit(False).alias("is_retryable"),
    ).withColumn("created_at", F.current_timestamp())

    if max_rows is not None:
        invalid_df = invalid_df.limit(int(max_rows))

    invalid_df.write.format("delta").mode("append").saveAsTable(INVALID_RECORD_TABLE)
    return int(row_count)


def log_unresolved_lookup_rows(
    df: DataFrame,
    table_session_id: Optional[str],
    target_table: str,
    record_key_column: str,
    lookup_key_columns: Iterable[str],
    raw_columns: Iterable[str],
) -> int:
    total_unresolved = 0
    for key_column in lookup_key_columns:
        if key_column not in df.columns:
            continue
        unresolved_df = df.where(F.col(key_column) == F.lit(UNKNOWN_KEY))
        total_unresolved += log_invalid_rows(
            df=unresolved_df,
            table_session_id=table_session_id,
            target_table=target_table,
            record_key_column=record_key_column,
            error_column=key_column,
            error_reason=f"Unresolved dimension lookup for {key_column}; assigned {UNKNOWN_KEY}",
            raw_columns=raw_columns,
        )
    return int(total_unresolved)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def empty_target_dataframe(target_table: str) -> DataFrame:
    require_table(target_table)
    return spark.createDataFrame([], spark.table(target_table).schema)


def select_target_columns(df: DataFrame, target_table: str) -> DataFrame:
    target_columns = get_table_columns(target_table)
    missing_columns = [column for column in target_columns if column not in df.columns]
    if missing_columns:
        raise Exception(f"Source dataframe is missing target columns for {target_table}: {', '.join(missing_columns)}")
    return df.select(*target_columns)


def merge_fact_table(source_df: DataFrame, target_table: str, upsert_key: str) -> Dict[str, int]:
    require_table(target_table)
    require_columns(target_table, [upsert_key])
    if upsert_key not in source_df.columns:
        raise Exception(f"Merge source is missing upsert key: {upsert_key}")

    source_df = select_target_columns(source_df, target_table).cache()
    source_count = int(source_df.count())

    if source_count == 0:
        source_df.unpersist()
        return {
            "source_row_count": 0,
            "target_row_count": int(spark.table(target_table).count()),
            "inserted_row": 0,
            "updated_row": 0,
            "deleted_row": 0,
        }

    target_keys_df = spark.table(target_table).select(upsert_key).dropDuplicates()
    source_keys_df = source_df.select(upsert_key).where(F.col(upsert_key).isNotNull()).dropDuplicates()
    inserted_count = int(source_keys_df.join(target_keys_df, upsert_key, "left_anti").count())
    updated_count = int(source_keys_df.join(target_keys_df, upsert_key, "inner").count())
    deleted_count = int(source_df.where(F.col("is_deleted") == F.lit(True)).count()) if "is_deleted" in source_df.columns else 0

    temp_view_name = f"_gold_fact_merge_{uuid.uuid4().hex}"
    source_df.createOrReplaceTempView(temp_view_name)

    target_columns = get_table_columns(target_table)
    update_columns = [column for column in target_columns if column not in {"created_at"}]
    update_clause = ",\n                ".join([f"target.`{column}` = source.`{column}`" for column in update_columns])
    insert_columns = ", ".join([f"`{column}`" for column in target_columns])
    insert_values = ", ".join([f"source.`{column}`" for column in target_columns])

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING {temp_view_name} AS source
        ON target.`{upsert_key}` = source.`{upsert_key}`
        WHEN MATCHED THEN UPDATE SET
                {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
    """)

    spark.catalog.dropTempView(temp_view_name)
    target_count = int(spark.table(target_table).count())
    source_df.unpersist()

    return {
        "source_row_count": source_count,
        "target_row_count": target_count,
        "inserted_row": inserted_count,
        "updated_row": updated_count,
        "deleted_row": deleted_count,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
