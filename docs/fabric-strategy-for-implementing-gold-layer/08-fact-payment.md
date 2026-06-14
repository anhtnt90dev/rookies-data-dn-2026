# 08 - Fact Payment Ingestion Strategy (`nb_gold_load_fact_payment_dev`)

This document defines the ingestion specifications, conformed key lookups, and operational metadata tracking logic for the `gold.fact_payment` table. Context for columns and rules is aligned with the project specification in [silver-to-gold-mapping.md](../source-to-target-mapping/silver-to-gold-mapping.md) and [fact_payment.json](../source-to-target-mapping/jsons/silver-to-gold/fact_payment.json).

---

## 1. Objectives

*   Ingest payment transaction records from `silver.payment` into `gold.fact_payment`.
*   Link each payment to conformed payment status (`dim_payment_status`) and payment method (`dim_payment_method`) dimensions.
*   Inherit policy, customer, provider, and vehicle keys from the upstream `silver.policy` lineage context.
*   Apply PIT logic based on the transaction date to resolve the correct customer and vehicle versions.

---

## 2. Ingestion Logic Flow

```mermaid
graph TD
    Start([Start Ingestion]) --> ReadPayment[Read silver.payment]
    ReadPayment --> JoinPolicy[Join silver.policy on policy_id]
    JoinPolicy --> JoinSCD1[Join SCD1 Dimensions: dim_policy, dim_payment_status, dim_payment_method]
    JoinSCD1 --> JoinSCD2[Join SCD2 Dimensions: dim_customer, dim_provider, dim_vehicle]
    JoinSCD2 --> PIT_Join{PIT Join condition met?}
    
    PIT_Join -->|Yes| ResolveKeys[Assign Target Surrogate Keys]
    PIT_Join -->|No / Null| AssignDefault[Assign -1 Surrogate Key]
    
    ResolveKeys & AssignDefault --> FormatDates[Resolve Date Keys: payment_date_key, issued_date_key]
    FormatDates --> MergeFact{Delta Merge on payment_id}
    
    MergeFact -->|Key Match & Diff Detected| UpdateFact[Update Record In-Place]
    MergeFact -->|Key Match & No Diff| SkipWrite[Bypass Write / Do Nothing]
    MergeFact -->|No Key Match| InsertFact[Insert New Record]
    
    UpdateFact & SkipWrite & InsertFact --> End([Fact Completed])
```

---

## 3. Dimension Key Lookups Schema

The conformed keys for `gold.fact_payment` are resolved using the following specifications:

| Target Key Column | Source Field | Target Dimension Table | Join Type & Lookup Logic |
| :--- | :--- | :--- | :--- |
| `policy_key` | `policy_id` | `dim_policy` (SCD1) | Join `silver.payment.policy_id` = `dim_policy.policy_id`. Null fallback = `-1`. |
| `payment_date_key` | `payment_at` | `dim_date` | `CAST(DATE_FORMAT(payment_at, 'yyyyMMdd') AS INT)`. Null fallback = `-1`. |
| `issued_date_key` | `issued_at` (Parent) | `dim_date` | Join `silver.policy` to get `issued_at`, then resolve `CAST(DATE_FORMAT(policy.issued_at, 'yyyyMMdd') AS INT)`. Null fallback = `-1`. |
| `customer_key` | `customer_id` (Parent) | `dim_customer` (SCD2) | PIT Join: Join `silver.policy` to get `customer_id`, where `silver.payment.payment_at` BETWEEN `dim_customer.effective_from` AND `dim_customer.effective_to`. Null fallback = `-1`. |
| `provider_key` | `provider_code` (Parent) | `dim_provider` (SCD2) | PIT Join: Join `silver.policy` to get `provider_code`, where `silver.payment.payment_at` BETWEEN `dim_provider.effective_from` AND `dim_provider.effective_to`. Null fallback = `-1`. |
| `payment_status_key` | `payment_status` | `dim_payment_status` (SCD1) | Join `silver.payment.payment_status` = `dim_payment_status.payment_status_code`. Null fallback = `-1`. |
| `payment_method_key` | `payment_method` | `dim_payment_method` (SCD1) | Join `silver.payment.payment_method` = `dim_payment_method.payment_method_code`. Null fallback = `-1`. |
| `vehicle_key` | `customer_id` (Parent) | `dim_vehicle` (SCD2) | PIT Join: Join `silver.policy` to get `customer_id` and find active `vehicle_id` from `silver.vehicle` where `silver.payment.payment_at` BETWEEN `dim_vehicle.effective_from` AND `dim_vehicle.effective_to`. Null fallback = `-1`. |

### Measures and Degenerate Dimensions:
*   `payment_amount`: `COALESCE(payment_amount, 0)`
*   `payment_id` (STRING), `policy_id` (STRING), `transaction_reference` (STRING): Mapped directly as degenerate dimensions.

---

## 4. Soft Delete and Ingestion Rules

*   **Soft Deletes**: If a payment is soft-deleted in `silver.payment` (`is_deleted = true`), the target row is updated:
    *   `is_deleted = true`
    *   `deleted_at = current_timestamp()`
    *   `delete_batch_id = <current_batch_id>`
    *   `payment_amount = 0.00`

---

## 5. Idempotency & Rerun Behavior (No-Change Scenario)

*   **No Changes**: If payment records in the incoming Silver batch are identical to existing target records, the MERGE condition evaluates to `false`. **No update operations are performed, and no data is written to disk**, saving processing time.
*   **Batch Reruns**: In recovery runs, the MERGE statement matches on `payment_id` and only updates records with actual column changes. No duplicate payment transactions are created.


