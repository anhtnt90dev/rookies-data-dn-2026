# 01 - Orchestration and Concurrency Specification

This document details the metadata-driven execution, concurrency throttling, and skip-resume recovery mechanisms implemented in the master orchestrator `nb_gold_master_load_dev`.

---

## 1. Metadata-Driven Orchestration

Rather than hardcoding the list of dimension and fact notebooks, the master notebook queries the Delta table `cfg.dim_fact_table` at runtime to dynamically fetch active configurations.

```mermaid
graph TD
    Master["Master Notebook:<br/>nb_gold_master_load_dev"] -->|Query active configurations| ConfigTable[("Table Configuration<br/>(cfg.dim_fact_table)")]
    ConfigTable -->|Parse Table Type| Stages{Split into Tasks}
    Stages -->|table_type = DIM| Stage1["Stage 1: Dimensions ThreadPool"]
    Stages -->|table_type = FACT| Stage2["Stage 2: Facts ThreadPool"]
```

### Schema of `cfg.dim_fact_table`
The master notebook relies on the following columns from the configuration table:
*   `id`: Table sequence ID.
*   `table_name`: Target Delta table name (e.g. `dim_customer`).
*   `table_type`: Type classification (`DIM` or `FACT`).
*   `gold_transform_name`: Target notebook path (e.g. `nb_gold_load_scd2_dimensions_dev`).
*   `is_active`: Boolean flag indicating if the table is loaded in the pipeline.

---

## 2. Two-Stage Parallel Execution Flow

To ensure referential integrity, ingestion is executed in two concurrent stages. Dimension loads must be fully completed before Fact loads begin so that point-in-time dimension key lookups resolve correctly. The validation suite runs subsequently as a dedicated sequential activity.

```mermaid
graph TD
    Start([Start Master Ingestion]) --> FetchConfig[Query cfg.dim_fact_table]
    FetchConfig --> InitContext[Initialize Run Mode & Batch ID]
    
    InitContext --> Stage1[Stage 1: Dimensions Parallel Load]
    subgraph "Stage 1: Dimensions Concurrency Pool (max_workers = 15)"
        D1[dim_date]
        D2[dim_customer]
        D3[dim_agent]
        D4[dim_provider]
        D5["SCD1: dim_package..."]
    end
    Stage1 --> D1 & D2 & D3 & D4 & D5
    
    D1 & D2 & D3 & D4 & D5 -->|All Succeeded| Stage2[Stage 2: Facts Parallel Load]
    D1 & D2 & D3 & D4 & D5 -->|Any Failed| Halt1[Halt and Abort immediately]
    
    subgraph "Stage 2: Facts Concurrency Pool (max_workers = 5)"
        F1[fact_quotation]
        F2[fact_quotation_item]
        F3[fact_policy]
        F4[fact_payment]
        F5[fact_cancellation]
    end
    Stage2 --> F1 & F2 & F3 & F4 & F5
    
    F1 & F2 & F3 & F4 & F5 -->|Any Failed| Halt2[Halt and Abort immediately]
    F1 & F2 & F3 & F4 & F5 -->|All Succeeded| Stage3[Stage 3: Post-Ingestion Audit & Reset run mode to NEW]
    
    Stage3 -->|nb_ingestion_gold Succeeded| Stage4[Pipeline Stage: Validation Suite]
    
    subgraph "Pipeline Stage: Validation Activity (Dedicated Sequential Activity)"
        V1[nb_gold_validate_reconciliation_dev]
    end
    Stage4 --> V1
    
    V1 -->|Pass| Success[Pipeline Succeeded]
    V1 -->|Fail| Halt3[Log anomalies to log.invalid_record & Pipeline Failed]
```

### End-to-End Execution Sequence

The execution path, parallel notebook runs, dynamic error interception, final metadata updates, and downstream validation are outlined below:

```mermaid
sequenceDiagram
    autonumber
    participant Pipeline as pl_master_etl_dev
    participant Master as nb_gold_master_load_dev
    participant Config as cfg.dim_fact_table
    participant Pool as ThreadPoolExecutor
    participant Child as Child Ingestion Notebooks
    participant Audit as log.audit_table_session
    participant Val as nb_gold_validate_reconciliation_dev
    
    Pipeline->>Master: Run nb_ingestion_gold (batch_id, session_id, run_mode)
    Master->>Config: Query active tables list
    Config-->>Master: Return list of active dimensions & facts
    
    Note over Master: STAGE 1: Load Dimensions
    Master->>Pool: Initialize ThreadPool (max_workers=15)
    loop For each dimension table in parallel
        Pool->>Child: mssparkutils.notebook.run(dim_notebook)
        Child-->>Pool: Return success / fail
    end
    Pool-->>Master: Return all results
    
    alt Any Dimension failed
        Master->>Audit: resolve_source_success() (Mark affected sources as FAILED)
        Master-->>Master: Raise Exception (Fail Fast & Abort)
        Master-->>Pipeline: Return Failure
    else All Dimensions succeeded
        Note over Master: STAGE 2: Load Facts
        Master->>Pool: Initialize ThreadPool (max_workers=5)
        loop For each fact table in parallel
            Pool->>Child: mssparkutils.notebook.run(fact_notebook)
            Child-->>Pool: Return success / fail
        end
        Pool-->>Master: Return all results
        
        alt Any Fact failed
            Master->>Audit: resolve_source_success() (Mark affected sources as FAILED)
            Master-->>Master: Raise Exception (Abort)
            Master-->>Pipeline: Return Failure
        else All Facts succeeded
            Note over Master: STAGE 3: Post-Ingestion Audit & Reset
            Master->>Audit: resolve_source_success() (Mark all sources as SUCCESS)
            Master->>Master: reset_next_run_mode() (Set next_run_mode = 'NEW')
            Master-->>Pipeline: Return Success (Exit)
            
            Note over Pipeline: Pipeline Activity: nb_validation_gold
            Pipeline->>Val: Run Validation (batch_id, session_id, run_mode)
            Val->>Val: Run validation checks sequentially
            Val->>Audit: finish_table_layer() (Write detail statuses to log.audit_detail)
            alt Any check fails
                Val-->>Pipeline: Return Validation Statuses (Anomalies logged)
            else All checks pass
                Val-->>Pipeline: Return Validation Statuses (All green)
            end
        end
    end
```


### Concurrency Pools and Workers
*   **Dimensions Pool (`max_workers = 15`)**:
    - Runs 14 dimension loading tasks concurrently (1 `dim_date`, 9 SCD1 loads, and 4 SCD2 loads).
    - Max workers is set to 15 to allow all tasks to trigger simultaneously, maximizing cluster resource utilization.
*   **Facts Pool (`max_workers = 5`)**:
    - Runs 5 fact loading tasks concurrently.
    - Max workers is throttled to 5 to avoid resource exhaustion since fact tables carry larger volumes and perform heavy joins.

> [!WARNING]
> **Microsoft Fabric Capacity Limits**
> Spawning 15 parallel Spark notebook jobs concurrently consumes a significant amount of workspace capacity. In smaller Fabric capacities (e.g., F2/F4), some notebook sessions might queue. Ensure the capacity is configured with sufficient vCores or warm pools to allow seamless concurrent notebook startup.

---

## 3. Skip-Resume Recovery Mechanics

If a pipeline run aborts, the subsequent recovery run executes in `RECOVERY` mode using the same logical `batch_id`.

While Bronze and Silver layers actively check `should_process_table_layer` and skip tables that previously succeeded, the **Gold Layer overrides this check to always return `True`** (via `nb_gold_audit_helper_dev`). Due to the strictly idempotent design of Gold's Delta loads (SCD1, SCD2, and Facts), re-running these tables is safe, ensures complete relational consistency against updated upstream layers, and avoids audit lookup latency inside parallel threads.

For comprehensive specifications on run mode transitions, skip-resume mechanics, and Gold orchestrator failure handling, see the dedicated document:
*   [05 - Ingestion Run Modes and Recovery Specifications](./05-pipeline-recovery-and-run-modes.md)
