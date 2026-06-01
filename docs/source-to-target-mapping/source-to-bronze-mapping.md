# Source to Bronze Mapping

## Overview

This document defines the Source-to-Bronze mapping for CRM database tables and JSON-based sources, including field mappings, logical data types, Bronze metadata columns, and proposed type mappings for Microsoft Fabric Lakehouse.

```mermaid
flowchart LR
    subgraph CRM["CRM Database"]
        CUST["dbo.customers"]
        AGT["dbo.agents"]
        PROV["dbo.insurance_providers"]
        VEH["dbo.vehicle"]
        QUO["dbo.quotation"]
        QI["dbo.quotation_item"]
    end

    subgraph JSON["JSON File Sources"]
        CAN["cancellation_full_<yyyy-MM-dd>.json"]
        PAY["payment_full_<yyyy-MM-dd>.json"]
        POL["policy_full_<yyyy-MM-dd>.json"]
    end

    subgraph BRONZE["Bronze Delta Tables"]
        BCUST["bronze.customer"]
        BAGT["bronze.agent"]
        BPROV["bronze.insurance_provider"]
        BVEH["bronze.vehicle"]
        BQUO["bronze.quotation"]
        BQI["bronze.quotation_item"]
        BCAN["bronze.cancellation"]
        BPAY["bronze.payment"]
        BPOL["bronze.policy"]
    end

    CUST --> BCUST
    AGT --> BAGT
    PROV --> BPROV
    VEH --> BVEH
    QUO --> BQUO
    QI --> BQI
    CAN --> BCAN
    PAY --> BPAY
    POL --> BPOL
```

## Bronze Metadata Flow

```mermaid
flowchart LR
    SRC["Source Record"] --> RAW["Bronze Business Columns"]
    PIPE["Pipeline"] --> BID["_batch_id"]
    PIPE --> LAT["_loaded_at"]
    PIPE --> SS["_source_system"]
    PIPE --> SN["_source_name"]
    RAW --> BRZ["Bronze Delta Table"]
    BID --> BRZ
    LAT --> BRZ
    SS --> BRZ
    SN --> BRZ
```

## 1. Objective

Provide a standardized Source to Bronze mapping reference for CRM (database) and JSON-based sources, including field mappings, logical data types, metadata and audit columns for the Bronze layer in Microsoft Fabric.

## 2. Audit and Metadata Columns

| Column | Description |
| --- | --- |
| _batch_id | Unique identifier of the logical data batch. |
| _loaded_at | Timestamp when the record was loaded into Bronze. |
| _source_system | Source system where the record originated. |
| _source_name | Source table, file, or entity name. |

## 3. JSON Source to Bronze Mapping

> **Note:**

> - Bronze stores raw incremental JSON records using append-only ingestion. CDC handling based on operation_type will be implemented in the Silver layer.

> - The source file names are based on the provided samples. Despite the naming convention, the JSON sources are incremental (delta) extracts.

### 3.1. Cancellations

- **Source system:** policy_system.

- **Source file:** cancellation_full_<yyyy-MM-dd>.json.

- **Target table:** bronze.cancellation.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cancellation_id | STRING | cancellation_id | STRING | 20 | N/A | N/A | Direct Mapping |
| policy_id | STRING | policy_id | STRING | 20 | N/A | N/A | Direct Mapping |
| cancellation_date | STRING | cancellation_date | STRING | 23 | N/A | N/A | Direct Mapping |
| cancellation_reason | STRING | cancellation_reason | STRING | 255 | N/A | N/A | Direct Mapping |
| refund_amount | STRING | refund_amount | STRING | 30 | N/A | N/A | Direct Mapping |
| last_updated | STRING | last_updated | STRING | 23 | N/A | N/A | Direct Mapping |
| operation_type | STRING | operation_type | STRING | 1 | N/A | N/A | Direct Mapping |
| batch_date | STRING | batch_date | STRING | 10 | N/A | N/A | Direct Mapping |
| source_system | STRING | source_system | STRING | 50 | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 3.2. Payments

- **Source system:** payment_system.

- **Source file:** payment_full_<yyyy-MM-dd>.json.

- **Target table:** bronze.payment.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| payment_id | STRING | payment_id | STRING | 20 | N/A | N/A | Direct Mapping |
| policy_id | STRING | policy_id | STRING | 20 | N/A | N/A | Direct Mapping |
| payment_date | STRING | payment_date | STRING | 23 | N/A | N/A | Direct Mapping |
| payment_method | STRING | payment_method | STRING | 50 | N/A | N/A | Direct Mapping |
| payment_status | STRING | payment_status | STRING | 50 | N/A | N/A | Direct Mapping |
| payment_amount | STRING | payment_amount | STRING | 30 | N/A | N/A | Direct Mapping |
| transaction_reference | STRING | transaction_reference | STRING | 100 | N/A | N/A | Direct Mapping |
| last_updated | STRING | last_updated | STRING | 23 | N/A | N/A | Direct Mapping |
| operation_type | STRING | operation_type | STRING | 1 | N/A | N/A | Direct Mapping |
| batch_date | STRING | batch_date | STRING | 10 | N/A | N/A | Direct Mapping |
| source_system | STRING | source_system | STRING | 50 | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 3.3. Policies

- **Source system:** policy_system.

- **Source file:** policy_full_<yyyy-MM-dd>.json.

- **Target Table:** bronze.policy.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| policy_id | STRING | policy_id | STRING | 20 | N/A | N/A | Direct Mapping |
| quotation_id | STRING | quotation_id | STRING | 20 | N/A | N/A | Direct Mapping |
| customer_id | STRING | customer_id | STRING | 20 | N/A | N/A | Direct Mapping |
| provider_code | STRING | provider_code | STRING | 20 | N/A | N/A | Direct Mapping |
| policy_number | STRING | policy_number | STRING | 50 | N/A | N/A | Direct Mapping |
| policy_start_date | STRING | policy_start_date | STRING | 10 | N/A | N/A | Direct Mapping |
| policy_end_date | STRING | policy_end_date | STRING | 10 | N/A | N/A | Direct Mapping |
| policy_status | STRING | policy_status | STRING | 50 | N/A | N/A | Direct Mapping |
| premium_amount | STRING | premium_amount | STRING | 30 | N/A | N/A | Direct Mapping |
| issued_date | STRING | issued_date | STRING | 23 | N/A | N/A | Direct Mapping |
| last_updated | STRING | last_updated | STRING | 23 | N/A | N/A | Direct Mapping |
| operation_type | STRING | operation_type | STRING | 1 | N/A | N/A | Direct Mapping |
| batch_date | STRING | batch_date | STRING | 10 | N/A | N/A | Direct Mapping |
| source_system | STRING | source_system | STRING | 50 | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

## 4. CRM Source to Bronze Mapping

### 4.1. Customers

- **Source system:** crm_system.

- **Source Table:** dbo.customers.

- **Target Table:** bronze.customer.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| customer_id | VARCHAR(20) | customer_id | STRING | 20 | N/A | N/A | Direct Mapping |
| full_name | NVARCHAR(200) | full_name | STRING | 200 | N/A | N/A | Direct Mapping |
| gender | VARCHAR(10) | gender | STRING | 10 | N/A | N/A | Direct Mapping |
| dob | DATE | dob | DATE | N/A | N/A | N/A | Direct Mapping |
| phone_number | VARCHAR(20) | phone_number | STRING | 20 | N/A | N/A | Direct Mapping |
| email | VARCHAR(200) | email | STRING | 200 | N/A | N/A | Direct Mapping |
| city | NVARCHAR(100) | city | STRING | 100 | N/A | N/A | Direct Mapping |
| district | NVARCHAR(100) | district | STRING | 100 | N/A | N/A | Direct Mapping |
| created_date | DATETIME | created_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| updated_date | DATETIME2(3) | updated_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 4.2. Agents

- **Source system:** crm_system.

- **Source Table:** dbo.agents.

- **Target Table:** bronze.agent.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| agent_id | VARCHAR(20) | agent_id | STRING | 20 | N/A | N/A | Direct Mapping |
| agent_name | NVARCHAR(200) | agent_name | STRING | 200 | N/A | N/A | Direct Mapping |
| region | NVARCHAR(100) | region | STRING | 100 | N/A | N/A | Direct Mapping |
| branch | NVARCHAR(100) | branch | STRING | 100 | N/A | N/A | Direct Mapping |
| manager_name | NVARCHAR(200) | manager_name | STRING | 200 | N/A | N/A | Direct Mapping |
| created_date | DATETIME | created_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| updated_date | DATETIME2(3) | updated_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 4.3. Insurance providers

- **Source system:** crm_system.

- **Source Table:** dbo.insurance_providers.

- **Target Table:** bronze.insurance_provider.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| provider_code | VARCHAR(20) | provider_code | STRING | 20 | N/A | N/A | Direct Mapping |
| provider_name | NVARCHAR(200) | provider_name | STRING | 200 | N/A | N/A | Direct Mapping |
| provider_group | NVARCHAR(100) | provider_group | STRING | 100 | N/A | N/A | Direct Mapping |
| active_flag | INT | active_flag | INT | N/A | N/A | N/A | Direct Mapping |
| created_date | DATETIME2(3) | created_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| updated_date | DATETIME2(3) | updated_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 4.4. Vehicles

- **Source system:** crm_system.

- **Source Table:** dbo.vehicle.

- **Target Table:** bronze.vehicle.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| vehicle_id | VARCHAR(20) | vehicle_id | STRING | 20 | N/A | N/A | Direct Mapping |
| customer_id | VARCHAR(20) | customer_id | STRING | 20 | N/A | N/A | Direct Mapping |
| plate_number | VARCHAR(20) | plate_number | STRING | 20 | N/A | N/A | Direct Mapping |
| vehicle_brand | NVARCHAR(100) | vehicle_brand | STRING | 100 | N/A | N/A | Direct Mapping |
| vehicle_model | NVARCHAR(100) | vehicle_model | STRING | 100 | N/A | N/A | Direct Mapping |
| manufacture_year | INT | manufacture_year | INT | N/A | N/A | N/A | Direct Mapping |
| vehicle_value | DECIMAL(18,2) | vehicle_value | DECIMAL | N/A | 18 | 2 | Direct Mapping |
| created_date | DATETIME2(3) | created_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| updated_date | DATETIME2(3) | updated_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 4.5. Quotations

- **Source system:** crm_system.

- **Source Table:** dbo.quotation.

- **Target Table:** bronze.quotation.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| quotation_id | VARCHAR(20) | quotation_id | STRING | 20 | N/A | N/A | Direct Mapping |
| customer_id | VARCHAR(20) | customer_id | STRING | 20 | N/A | N/A | Direct Mapping |
| agent_id | VARCHAR(20) | agent_id | STRING | 20 | N/A | N/A | Direct Mapping |
| provider_code | VARCHAR(20) | provider_code | STRING | 20 | N/A | N/A | Direct Mapping |
| quotation_date | DATETIME | quotation_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| quotation_status | VARCHAR(50) | quotation_status | STRING | 50 | N/A | N/A | Direct Mapping |
| package_code | VARCHAR(50) | package_code | STRING | 50 | N/A | N/A | Direct Mapping |
| premium_amount | DECIMAL(18,2) | premium_amount | DECIMAL | N/A | 18 | 2 | Direct Mapping |
| quotation_expiry_date | DATETIME | quotation_expiry_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| created_date | DATETIME2(3) | created_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| updated_date | DATETIME2(3) | updated_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

### 4.6. Quotation items

- **Source system:** crm_system.

- **Source Table:** dbo.quotation_item.

- **Target Table:** bronze.quotation_item.

| Source Column | Source Type | Target Column | Target Type | Target Length | Target Precision | Target Scale | Rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| quotation_item_id | VARCHAR(20) | quotation_item_id | STRING | 20 | N/A | N/A | Direct Mapping |
| quotation_id | VARCHAR(20) | quotation_id | STRING | 20 | N/A | N/A | Direct Mapping |
| coverage_type | NVARCHAR(100) | coverage_type | STRING | 100 | N/A | N/A | Direct Mapping |
| coverage_amount | DECIMAL(18,2) | coverage_amount | DECIMAL | N/A | 18 | 2 | Direct Mapping |
| deductible_amount | DECIMAL(18,2) | deductible_amount | DECIMAL | N/A | 18 | 2 | Direct Mapping |
| created_date | DATETIME2(3) | created_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| updated_date | DATETIME2(3) | updated_date | TIMESTAMP | N/A | N/A | N/A | Direct Mapping |
| N/A | N/A | _batch_id | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _loaded_at | TIMESTAMP | N/A | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_system | STRING | 50 | N/A | N/A | Pipeline Generated |
| N/A | N/A | _source_name | STRING | 100 | N/A | N/A | Pipeline Generated |

## 5. Mapping Matrix

| Target Type | Target Length | Target Precision | Target Scale | Delta Lake Type |
| --- | --- | --- | --- | --- |
| STRING | Any | N/A | N/A | STRING |
| INT | N/A | N/A | N/A | INT |
| BIGINT | N/A | N/A | N/A | BIGINT |
| DECIMAL | N/A | p | s | DECIMAL(p,s) |
| DATE | N/A | N/A | N/A | DATE |
| TIMESTAMP | N/A | N/A | N/A | TIMESTAMP |
| BOOLEAN | N/A | N/A | N/A | BOOLEAN |
| DOUBLE | N/A | N/A | N/A | DOUBLE |

> **Note:**

> - The mappings, metadata columns, and type mappings defined in this document are proposed for the current solution design and may be refined during implementation based on technical constraints, source system changes, or project requirements.
