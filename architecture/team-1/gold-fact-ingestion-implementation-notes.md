# Gold Fact Ingestion Implementation Notes

## Source-of-truth mismatch report

The repository is the source of truth for this implementation. Before coding, the following files were re-read:

- `docs/business-process/diagram/logical_star_schema_ver4.md`
- `docs/source-to-target-mapping/silver-to-gold-mapping.md`
- `sql/lakehouse/create_gold_tables.sql`
- `sql/lakehouse/create_silver_tables.sql`
- existing Gold, Silver, Bronze, Config, Pipeline, and Audit notebooks

Confirmed differences between the execution plan wording and the current repo:

- `pipeline_run_id`, `deleted_at`, and `delete_batch_id` are not present in the current Silver DDL. They are populated at Gold load time.
- `silver.policy`, `silver.payment`, and `silver.cancellation` contain `is_deleted`. `silver.quotation` and `silver.quotation_item` do not, so those sources must default to active until their DDL changes.
- `silver.policy` has `last_updated_at` and `_loaded_at`, but not `updated_at`. The `fact_policy` dedupe/order logic uses `last_updated_at`, then `_loaded_at`, then `issued_at`.
- The repo already has `gold.fact_policy` and supporting dimension table DDL, but it did not have Gold fact build, validation, or driver notebooks before this task.
- The existing audit helper names its numeric table identifier `source_table_id`. For Gold facts this implementation uses `cfg.dim_fact_table.id` as that identifier. For `fact_policy`, the confirmed id is `17`.
- Dimension ingestion is a dependency owned by other tasks. This implementation does not create fake production dimension rows. Preflight fails if required dimensions, SCD2 windows, or `-1` Unknown members are missing.
- Additional repo issue observed but not used by this fact build: `cfg.source_table` lists the insurance provider Silver table as `silver.insurance_provider`, while the current Silver DDL creates `silver.provider`. The `fact_policy` implementation resolves `provider_key` through `gold.dim_provider`, so this should be verified by the dimension ingestion task owner.

No contradiction was found for the documented `fact_policy` relationships. The implementation follows the repo mapping:

- primary source: `silver.policy`
- quotation context: `silver.policy.quotation_id -> silver.quotation.quotation_id`
- `agent_key`: lookup `gold.dim_agent` by quotation `agent_id` using quotation event date
- `package_key`: lookup `gold.dim_package` by quotation `package_code`
- `customer_key`, `provider_key`, `vehicle_key`: SCD2 lookups using policy event date
- date keys: `issued_at`, `policy_start_date`, and `policy_end_date` converted to `yyyyMMdd` and validated against `gold.dim_date`

## Implemented artifacts

- `fabric/Gold/Notebooks/nb_gold_fact_helper_dev.Notebook`
- `fabric/Gold/Notebooks/nb_gold_fact_build_dev.Notebook`
- `fabric/Gold/Notebooks/nb_gold_fact_validate_dev.Notebook`
- `fabric/Gold/Notebooks/nb_gold_driver_flow_dev.Notebook`

## Current implementation scope

Implemented now:

- Gold fact helper/preflight framework.
- Preflight dependency checks for `fact_policy`.
- Delta merge helper that preserves target `created_at` on matched rows.
- Type 1 and SCD2 lookup helpers.
- Date-key validation helper.
- Unresolved lookup logging to `log.invalid_record`.
- Gold audit wrappers around existing `log.audit_session`, `log.audit_table_session`, and `log.audit_detail`.
- `fact_policy` build logic.
- `fact_policy` validation logic.
- Driver flow for all currently implemented facts. At this checkpoint, `ALL` means `fact_policy` only.

Intentionally not implemented yet:

- `fact_quotation`
- `fact_quotation_item`
- `fact_payment`
- `fact_cancellation`

Those facts should be added only after `fact_policy` has passed controlled Fabric test runs.

## Required dependencies before production execution

The following must already exist and be populated:

- `gold.dim_date`
- `gold.dim_policy`
- `gold.dim_quotation`
- `gold.dim_customer`
- `gold.dim_provider`
- `gold.dim_agent`
- `gold.dim_package`
- `gold.dim_policy_status`
- `gold.dim_vehicle`
- required `-1` Unknown member rows for all non-date dimensions
- SCD2 windows in `gold.dim_customer`, `gold.dim_provider`, `gold.dim_agent`, and `gold.dim_vehicle`
- `cfg.dim_fact_table`
- `log.audit_session`
- `log.audit_table_session`
- `log.audit_detail`
- `log.invalid_record`

## Test strategy

Run these tests in Microsoft Fabric against controlled tables or a disposable test lakehouse:

| Test case | Input condition | Expected result |
| --- | --- | --- |
| Normal successful run | Valid `silver.policy`, matching quotation, populated dimensions, valid `dim_date` | `gold.fact_policy` rows merge successfully and validation passes |
| Empty source input | Selected batch has no `silver.policy` rows | Build finishes with zero inserts and validation passes when Gold also has zero rows for that batch |
| Duplicate source records | Two rows share `policy_id` with different `last_updated_at` or `_loaded_at` | Only latest row is merged; validation finds no duplicate `policy_id` |
| Null required key | `policy_id` is null or blank | Row is excluded, logged to `log.invalid_record`, and merge continues |
| Missing dimension lookup | Valid fact row but missing customer/provider/agent/package/status/vehicle dimension row | Non-date key becomes `-1` and invalid-record output traces the lookup column |
| Missing date key | Fact date converts to a key not present in `gold.dim_date`, or source date is null | Build fails before merge |
| SCD2 historical lookup | Multiple dimension versions exist for a business key | Fact uses the dimension row whose effective window contains the event date |
| Incremental batch filter | Pass `p_batch_id` | Only matching Silver batch rows are processed |
| Failed layer execution | Remove a required table or column in test | Preflight fails before fact write |
| Audit correctness | Run with `p_enable_audit=true` | Audit session, table session, detail row, and invalid-record rows exist as applicable |
| Row count correctness | Deduped valid source count equals target count for selected batch | Validation passes row count reconciliation |
| Rerun/idempotency | Run same batch twice | No duplicate `policy_id`; merge reports updates, not new duplicates |
| Schema mismatch | Drop or rename a required source/target column in test | Preflight fails before write |
| Soft delete | `silver.policy.is_deleted=true` or `operation_type='D'` | Target row remains, `is_deleted=true`, `deleted_at` and `delete_batch_id` populated |

## Execution order

1. Run dimension ingestion tasks and confirm required dimensions are populated.
2. Confirm `cfg` and `log` tables exist.
3. Run `nb_gold_driver_flow_dev` with `p_fact_table='fact_policy'`.
4. Review `nb_gold_fact_validate_dev` output.
5. Review `log.invalid_record` for any `-1` lookup assignments.
6. Rerun the same batch to confirm idempotency.
7. Add the next fact only after the current fact passes.
