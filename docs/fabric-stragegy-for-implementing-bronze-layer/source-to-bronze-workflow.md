# Master Pipeline - Source-to-Bronze Processing Flow

> **Source of Truth**
>
> This Mermaid diagram is the source of truth for the Source-to-Bronze pipeline execution flow. It supersedes any previous draw.io or image-based diagrams.

## Workflow Diagram

```mermaid
flowchart TD
    A[Start Master Pipeline] --> B[Lookup cfg.next_run_mode<br/>Get next_run_mode, batch_id, previous_session_id]

    B --> C{run_mode}

    C -->|NEW| D[Generate new batch_id]
    C -->|RECOVERY| E[Reuse existing batch_id<br/>and previous_session_id]

    D --> F[Generate session_id]
    E --> F

    F --> G[Set Pipeline Variables]
    G --> H[Insert audit_session]

    H --> I[Lookup active source_table config]
    I --> J[Insert audit_table_session for all active sources<br/>NEW = NOT_RUN<br/>RECOVERY = SUCCESS becomes SKIPPED]

    J --> K[ForEach source entity]
    K --> L[Process Source-to-Bronze Notebook]

    L --> M{bronze_status}

    M -->|SKIPPED in RECOVERY| O[Return SKIPPED result]
    M -->|NOT_RUN / RUNNING| P{source_type}

    P -->|Database| Q{load_type}
    P -->|File| R{load_type}

    Q -->|FULL| Q1[Read full DB source]
    Q -->|INCREMENTAL| Q2[Read DB source using watermark]

    R -->|FULL| R1[List files and process new files]
    R -->|INCREMENTAL| R2[List files<br/>skip SUCCESS files<br/>apply watermark/file tracking]

    Q1 --> S[Validate source schema]
    Q2 --> S
    R1 --> S
    R2 --> S

    S --> T[Apply source-to-bronze mapping]
    T --> U[Add metadata columns]
    U --> V[Write to Bronze Delta table]
    V --> W[Update watermark if applicable]
    W --> X[Return SUCCESS result]

    S -. Error .-> ERR[Catch Exception]
    T -. Error .-> ERR
    U -. Error .-> ERR
    V -. Error .-> ERR
    W -. Error .-> ERR

    ERR --> ERR1[Return FAILED result]

    O --> Z{Aggregate source results}
    X --> Z
    ERR1 --> Z

    Z -->|No| K
    Z -->|Yes| AA[Bulk update audit_table_session<br/>Bulk insert audit_detail]

    AA --> AB[Run Bronze Layer Gate]

    AB --> AC{All bronze_status<br/>SUCCESS/SKIPPED?}

    AC -->|No| AD[Update audit_session FAILED]
    AD --> AF[Update cfg.next_run_mode = RECOVERY]
    AF --> AG[Stop Pipeline]

    AC -->|Yes| AH{Total target_row_count > 0?}

    AH -->|Yes| AE[Continue to Silver]
    AH -->|No| AI[Update audit_session SUCCESS]
    AI --> AJ[Exit notebook with NO_DATA<br/>Stop before Silver]
```

## SQL Queries

### 1. Lookup Pipeline Run Mode

```sql
SELECT
    next_run_mode,
    batch_id,
    session_id
FROM cfg.next_run_mode;
```

**Purpose**
- Determine whether the pipeline runs in `NEW` or `RECOVERY` mode.
- Retrieve the latest `batch_id`.
- Retrieve the previous `session_id` for recovery processing.

### 2. Lookup Active Source Configurations

```sql
SELECT
    s.id,
    s.source_system,
    s.source_type,
    s.source_name,
    s.source_format,
    s.source_location,
    s.load_type,
    s.watermark_column,
    s.source_to_bronze_mapping_path,
    s.bronze_table_name,
    s.load_sequence,
    w.watermark_value
FROM cfg.source_table s
LEFT JOIN cfg.watermark w
    ON s.id = w.source_table_id
WHERE s.is_active = 1
ORDER BY s.load_sequence;
```

**Purpose**
- Retrieve all active source entities.
- Retrieve ingestion configurations and metadata.
- Retrieve watermark values for incremental loading.
- Ensure sources are processed according to `load_sequence`.