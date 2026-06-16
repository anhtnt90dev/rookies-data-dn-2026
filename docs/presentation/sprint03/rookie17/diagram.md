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
    I --> J[Insert audit_table_session for all active sources]

    J --> K[ForEach source entity]
    K --> L[Process Source-to-Bronze Notebook]

    L --> M{run_mode}

    M -->|NEW| N[Process normally]
    M -->|RECOVERY| O[Skip SUCCESS/SKIPPED sources<br/>and process failed sources]

    N --> P{source_type}
    O --> P

    P -->|Database| Q{load_type}
    P -->|File| R{load_type}

    Q -->|FULL| Q1[Read full DB source]
    Q -->|INCREMENTAL| Q2[Read DB source using watermark]

    R -->|FULL| R1[List and process files]
    R -->|INCREMENTAL| R2[List files and apply watermark/file tracking]

    Q1 --> S[Validate source schema]
    Q2 --> S
    R1 --> S
    R2 --> S

    S --> T[Apply source-to-bronze mapping]
    T --> U[Add metadata columns]
    U --> V[Write to Bronze Delta table]

    V --> W[Update watermark if applicable]
    W --> X[Update audit_table_session SUCCESS]
    X --> Y[Insert audit_detail SUCCESS]

    S -. Error .-> ERR[Catch Exception]
    T -. Error .-> ERR
    U -. Error .-> ERR
    V -. Error .-> ERR
    W -. Error .-> ERR

    ERR --> ERR1[Update audit_table_session FAILED]
    ERR1 --> ERR2[Insert audit_detail FAILED]
    ERR2 --> ERR3[Raise exception to Pipeline]

    Y --> Z{All sources processed?}
    ERR3 --> AB

    Z -->|No| K
    Z -->|Yes| AA[Run Bronze Validation Notebook]

    AA --> AB{All bronze_status<br/>SUCCESS/SKIPPED?}

    AB -->|No| AD[Update audit_session FAILED]
    AB -->|Yes| AC{All source entities have<br/>inserted_row > 0?}

    AC -->|Yes| AE[Continue to Silver]
    AC -->|No| AD

    AD --> AF[Update cfg.next_run_mode = RECOVERY]
    AF --> AG[Stop Pipeline]
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