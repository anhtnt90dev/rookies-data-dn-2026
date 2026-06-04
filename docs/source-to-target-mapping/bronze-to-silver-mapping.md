# Bronze to Silver Mapping

## Overview

This document defines the Bronze-to-Silver mapping for CRM database tables and JSON-based sources, based on the existing Source-to-Bronze mapping and the naming conventions. It standardizes column names, data types, and basic cleansing rules for the Silver layer in Microsoft Fabric Lakehouse.

## 1. Objective

Provide a consistent mapping reference from Bronze tables to Silver tables,data type casting, and common transformation rules.



## 2. Common Audit and Metadata Columns

| Bronze Column | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- |
| _batch_id | _batch_id | STRING | Pass-through |
| _loaded_at | _loaded_at | TIMESTAMP | Generate |
| _source_system | _source_system | STRING | Pass-through |
| _source_name | _source_name | STRING | Pass-through |

## 3. JSON Source to Silver Mapping

> **Note:** Bronze JSON columns are raw strings. Silver casts these to typed columns and aligns naming to the standard.

### 3.1. Cancellations

- **Source table:** bronze.cancellation
- **Target table:** silver.cancellation

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| cancellation_id | STRING | cancellation_id | STRING | Direct mapping |
| policy_id | STRING | policy_id | STRING | Direct mapping |
| cancellation_date | STRING | cancellation_at | TIMESTAMP | Cast to `TIMESTAMP` and and suffix `_at`|
| cancellation_reason | STRING | cancellation_reason | STRING | Direct mapping |
| refund_amount | STRING | refund_amount | DECIMAL(18,2) | Cast to `DECIMAL(18,2)` |
| last_updated | STRING | last_updated | TIMESTAMP | Cast to `TIMESTAMP` |
| operation_type | STRING | operation_type | STRING | Direct mapping |
| operation_type | STRING | is_deleted | BOOLEAN | Check Condition |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 3.2. Payments

- **Source table:** bronze.payment
- **Target table:** silver.payment

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| payment_id | STRING | payment_id | STRING | Direct mapping |
| policy_id | STRING | policy_id | STRING | Direct mapping |
| payment_date | STRING | payment_at | TIMESTAMP | Cast to `TIMESTAMP` and suffix `_at` |
| payment_method | STRING | payment_method | STRING | Direct mapping |
| payment_status | STRING | payment_status | STRING | Direct mapping |
| payment_amount | STRING | payment_amount | DECIMAL(18,2) | Cast to `DECIMAL(18,2)` |
| transaction_reference | STRING | transaction_reference | STRING | Direct mapping |
| last_updated | STRING | last_updated | TIMESTAMP | Cast to `TIMESTAMP` |
| operation_type | STRING | operation_type | STRING | Direct mapping |
| operation_type | STRING | is_delete | BOOLEAN | Check Condition |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 3.3. Policies

- **Source table:** bronze.policy
- **Target table:** silver.policy

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| policy_id | STRING | policy_id | STRING | Direct mapping |
| quotation_id | STRING | quotation_id | STRING | Direct mapping |
| customer_id | STRING | customer_id | STRING | Direct mapping |
| provider_code | STRING | provider_code | STRING | Direct mapping |
| policy_number | STRING | policy_number | STRING | Direct mapping |
| policy_start_date | STRING | policy_start_date | DATE | Cast to `DATE` |
| policy_end_date | STRING | policy_end_date | DATE | Cast to `DATE` |
| policy_status | STRING | policy_status | STRING | Direct mapping |
| premium_amount | STRING | premium_amount | DECIMAL(18,2) | Cast to `DECIMAL(18,2)` |
| operation_type | STRING | operation_type | STRING | Direct mapping |
| operation_type | STRING | is_delete | BOOLEAN | Check Condition |
| issued_date | STRING | issued_at | TIMESTAMP | Cast to `TIMESTAMP`, rename to `_at` |
| last_updated | STRING | last_updated | TIMESTAMP | Cast to `TIMESTAMP` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

## 4. CRM Source to Silver Mapping

> **Note:** CRM Bronze columns are already typed but names are standardized to `_at` for timestamps.

### 4.1. Customers

- **Source table:** bronze.customer
- **Target table:** silver.customer

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| customer_id | STRING | customer_id | STRING | Direct mapping |
| full_name | STRING | full_name | STRING | Direct mapping |
| gender | STRING | gender | STRING | Direct mapping |
| dob | DATE | dob | DATE | Direct mapping |
| phone_number | STRING | phone_number | STRING | Direct mapping |
| email | STRING | email | STRING | Direct mapping |
| city | STRING | city | STRING | Direct mapping |
| district | STRING | district | STRING | Direct mapping |
| created_date | TIMESTAMP | created_at | TIMESTAMP | Rename to `_at` |
| updated_date | TIMESTAMP | updated_at | TIMESTAMP | Rename to `_at` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 4.2. Agents

- **Source table:** bronze.agent
- **Target table:** silver.agent

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| agent_id | STRING | agent_id | STRING | Direct mapping |
| agent_name | STRING | agent_name | STRING | Direct mapping |
| region | STRING | region | STRING | Direct mapping |
| branch | STRING | branch | STRING | Direct mapping |
| manager_name | STRING | manager_name | STRING | Direct mapping |
| created_date | TIMESTAMP | created_at | TIMESTAMP | Rename to `_at` |
| updated_date | TIMESTAMP | updated_at | TIMESTAMP | Rename to `_at` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 4.3. Insurance Providers

- **Source table:** bronze.insurance_provider
- **Target table:** silver.provider

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| provider_code | STRING | provider_code | STRING | Direct mapping |
| provider_name | STRING | provider_name | STRING | Direct mapping |
| provider_group | STRING | provider_group | STRING | Direct mapping |
| active_flag | INT | is_active | BOOLEAN | Convert `1/0` to `true/false` |
| created_date | TIMESTAMP | created_at | TIMESTAMP | Rename to `_at` |
| updated_date | TIMESTAMP | updated_at | TIMESTAMP | Rename to `_at` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 4.4. Vehicles

- **Source table:** bronze.vehicle
- **Target table:** silver.vehicle

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| vehicle_id | STRING | vehicle_id | STRING | Direct mapping |
| customer_id | STRING | customer_id | STRING | Direct mapping |
| plate_number | STRING | plate_number | STRING | Direct mapping |
| vehicle_brand | STRING | vehicle_brand | STRING | Direct mapping |
| vehicle_model | STRING | vehicle_model | STRING | Direct mapping |
| manufacture_year | INT | manufacture_year | INT | Direct mapping |
| vehicle_value | DECIMAL(18,2) | vehicle_value | DECIMAL(18,2) | Direct mapping |
| created_date | TIMESTAMP | created_at | TIMESTAMP | Rename to `_at` |
| updated_date | TIMESTAMP | updated_at | TIMESTAMP | Rename to `_at` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 4.5. Quotations

- **Source table:** bronze.quotation
- **Target table:** silver.quotation

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| quotation_id | STRING | quotation_id | STRING | Direct mapping |
| customer_id | STRING | customer_id | STRING | Direct mapping |
| agent_id | STRING | agent_id | STRING | Direct mapping |
| provider_code | STRING | provider_code | STRING | Direct mapping |
| quotation_date | TIMESTAMP | quotation_at | TIMESTAMP | Rename to `_at` |
| quotation_status | STRING | quotation_status | STRING | Direct mapping |
| package_code | STRING | package_code | STRING | Direct mapping |
| premium_amount | DECIMAL(18,2) | premium_amount | DECIMAL(18,2) | Direct mapping |
| quotation_expiry_date | TIMESTAMP | quotation_expiry_at | TIMESTAMP | Rename to `_at` |
| created_date | TIMESTAMP | created_at | TIMESTAMP | Rename to `_at` |
| updated_date | TIMESTAMP | updated_at | TIMESTAMP | Rename to `_at` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |

### 4.6. Quotation Items

- **Source table:** bronze.quotation_item
- **Target table:** silver.quotation_item

| Bronze Column | Bronze Type | Silver Column | Silver Type | Rule |
| --- | --- | --- | --- | --- |
| quotation_item_id | STRING | quotation_item_id | STRING | Direct mapping |
| quotation_id | STRING | quotation_id | STRING | Direct mapping |
| coverage_type | STRING | coverage_type | STRING | Direct mapping |
| coverage_amount | DECIMAL(18,2) | coverage_amount | DECIMAL(18,2) | Direct mapping |
| deductible_amount | DECIMAL(18,2) | deductible_amount | DECIMAL(18,2) | Direct mapping |
| created_date | TIMESTAMP | created_at | TIMESTAMP | Rename to `_at` |
| updated_date | TIMESTAMP | updated_at | TIMESTAMP | Rename to `_at` |
| _batch_id | STRING | _batch_id | STRING | Pass-through |
| _loaded_at | TIMESTAMP | _loaded_at | TIMESTAMP | Generate |
| _source_system | STRING | _source_system | STRING | Pass-through |
| _source_name | STRING | _source_name | STRING | Pass-through |


