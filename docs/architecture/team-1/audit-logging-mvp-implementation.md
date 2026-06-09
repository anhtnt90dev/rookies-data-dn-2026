# Audit Logging MVP Implementation

## Purpose

This document describes how the Microsoft Fabric audit logging MVP implements the logical workflow-control design in `data-workflow-control-strategy.md`.

## Canonical Audit and Control Mapping

| Canonical Purpose                       | Fabric Implementation                         | Responsibility                                                                                                 |
| --------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `Job_Config`                            | `cfg.source_table`                            | Stores configured source-table metadata. This table is designed but is not implemented by the audit MVP.       |
| `Watermark`                             | `cfg.watermark`                               | Stores Source-to-Bronze ingestion checkpoints. This table is designed but is not implemented by the audit MVP. |
| `Batch_Log`                             | `log.audit_session`                           | Stores one record for each pipeline execution session.                                                         |
| `Pipeline_Log`                          | `log.audit_session`, `log.audit_table_session`, `log.audit_file_session`, `log.audit_detail` | Stores run/session, table/layer, per-file, row-count, and reconciliation tracking.                             |
| `Pipeline_Error`                        | `log.invalid_record`, table/file error fields, `log.retry_log` | Stores rejected validation records, operational failure details, and retry attempt failures.                   |
| Retry support                           | `cfg.retry_policy`, `log.retry_log`           | Stores configurable retry policy and retry attempts for transient system failures.                             |
| Batch/session recovery context          | `cfg.next_run_mode`, `batch_id`, `session_id`, audit statuses | Stores the state needed to start NEW runs or resume failed batches in RECOVERY mode.                           |

## Identity and Update Rules

- `pipeline_run_id` is the idempotency key for `log.audit_session`. Repeating a start call with the same orchestration run ID reuses the existing session record.
- `(session_id, source_table_id)` is the idempotency key for `log.audit_table_session`.
- One `log.audit_table_session` record represents one configured source table during one pipeline execution. Bronze, Silver, and Gold update the status and timestamps on that same record.
- `(batch_id, source_table_id, source_file)` is the idempotency key for `log.audit_file_session`.
- One `log.audit_file_session` record represents one source file for one configured source table in one batch.
- `table_session_id` links `log.audit_file_session`, `log.audit_detail`, `log.retry_log`, and `log.invalid_record` to their parent table execution.
- `log.invalid_record` does not physically duplicate `batch_id`, `session_id`, or source entity. Those are traced by joining `log.invalid_record.table_session_id` to `log.audit_table_session.id`.
- `cfg.next_run_mode` stores the next run mode and failed `batch_id` for recovery. Its physical `session_id` column is `BIGINT`, while MVP audit session IDs are UUID strings, so the current framework treats `batch_id` as the durable recovery key until the session identifier type is resolved.
- `log.audit_detail` is append-only. Its generated `id` uniquely identifies each detail, while `(table_session_id, layer, attempt_no)` describes its execution attempt.
- The row-count MVP uses its `ErrorType` classification as the table-level `error_code` when a standardized downstream error code is not available.
- UUID string IDs are used by the physical MVP implementation. The logical design permits the ID generation strategy to be finalized during implementation.

## Physical MVP Extensions

The MVP retains the following physical columns in addition to the logical baseline:

| Table                     | Extension Columns                                                                                                                                                                  | Purpose                                                                         |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `log.audit_session`       | `duration_ms`, `sla_target_ms`, `sla_breached`                                                                                                                                     | Measures pipeline duration and SLA performance.                                 |
| `log.audit_table_session` | `batch_id`, `source_table_name`, `watermark_column`, `watermark_before`, `watermark_after`, `load_window_start`, `load_window_end`, `duration_ms`, `sla_target_ms`, `sla_breached` | Supports monitoring, lineage, incremental-load context, and SLA analysis.       |
| `log.audit_file_session`  | `source_file`, `file_status`, row-count fields, `retry_count`, `last_retry_at`, start/end timestamps, file-level error fields                                                       | Tracks per-file processing status, row counts, retry state, timing, and file-level errors. |
| `log.audit_detail`        | `attempt_no`, `target_row_count`, watermark/load-window fields, `error_type`, `is_retryable`, duration/SLA fields                                                                  | Supports reconciliation, attempt history, error classification, and monitoring. |
| `log.retry_log`           | `attempt_no`, `error_type`, `is_retryable`, `duration_ms`                                                                                                                          | Supports detailed retry analysis.                                               |
| `log.invalid_record`      | `error_type`, `is_retryable`                                                                                                                                                       | Supports consistent error classification.                                       |

The MVP does not use an `audit_key` column. Updates use the design-defined identifiers and relationships described above.

## Fabric Validation Procedure

The committed table-creation notebook is non-destructive. Before validating this schema change, manually recreate the audit objects in the disposable `audit_lakehouse_test` Lakehouse:

```sql
DROP VIEW IF EXISTS log.vw_etl_investigation_queue;
DROP VIEW IF EXISTS log.vw_etl_pipeline_run_summary;
DROP VIEW IF EXISTS log.vw_etl_table_layer_monitor;

DROP TABLE IF EXISTS log.invalid_record;
DROP TABLE IF EXISTS log.retry_log;
DROP TABLE IF EXISTS log.audit_detail;
DROP TABLE IF EXISTS log.audit_file_session;
DROP TABLE IF EXISTS log.audit_table_session;
DROP TABLE IF EXISTS log.audit_session;
```

After updating the Fabric workspace from Git, run `nb_audit_driver_flow_dev`. Confirm that:

- None of the recreated audit tables contain `audit_key`.
- Repeating a start call with one `pipeline_run_id` returns the same pipeline-session ID.
- Bronze, Silver, and Gold return the same table-session ID for one `(session_id, source_table_id)`.
- The final table-session row contains the expected Bronze, Silver, and Gold statuses.
- Detail rows remain separate and link to the table session through `table_session_id`.
- Failed layers populate table-level `error_code` and `error_message`.
- Monitoring views compile and expose table-level and detail-level errors separately.

## SLA Target Strategy

For the MVP, SLA tracking is optional and used for monitoring pipeline/table runtime.

- SLA values are stored in milliseconds in `sla_target_ms`.
- If `sla_target_ms` is provided, Audit compares the final runtime against it when the run finishes.
- `sla_breached = TRUE` when `duration_ms > sla_target_ms`.
- If `sla_target_ms` is NULL, SLA breach evaluation is disabled for that run.
- The default pipeline SLA is 30 minutes unless a runtime/config override is provided.
- Table/layer-level SLA can override the pipeline default when a specific table or layer needs a different threshold.
