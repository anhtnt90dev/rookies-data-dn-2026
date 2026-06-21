# Fabric Pipeline Performance Optimization

## Overview

This document summarizes the performance analysis and optimization process for a Microsoft Fabric Medallion Data Pipeline processing **1,000 records**. It includes the execution performance before optimization, identified bottlenecks, optimization strategies, and the resulting performance improvements.

---

# Performance Before Optimization

## Execution Time

| Metric                 |             Value |
| ---------------------- | ----------------: |
| Actual Execution Time  |       **64m 37s** |
| Average Execution Time |   **~65 minutes** |
| Dataset Size           | **1,000 records** |

---

# Root Causes of Performance Issues

## 1. High Overhead from PySpark Notebook Activities

All activities related to table interactions, validation checks, logging, and processing logic were implemented using **PySpark Notebook Activities**.

Each notebook execution required:

* Spark Session initialization (~20 seconds)
* Notebook Activity startup (~20 seconds)

Because notebooks were executed frequently throughout the pipeline, the repeated initialization overhead significantly increased the overall execution time.

---

## 2. No Spark Session Reuse

Each notebook started a new Spark Session instead of reusing an existing one.

This resulted in:

* Repeated Spark initialization
* Higher compute resource consumption
* Longer pipeline execution time

---

## 3. Sequential Table Ingestion

Tables were ingested one after another.

Consequences:

* Each activity waited for the previous one to finish.
* Available compute resources remained underutilized.
* Overall throughput was significantly reduced.

---

## 4. Excessive Use of Invoke Pipeline Activities

Each Medallion layer (Bronze, Silver, Gold) was orchestrated using separate **Invoke Pipeline** activities.

This introduced additional overhead due to:

* Pipeline initialization
* Pipeline termination
* Context switching between pipelines

---

## 5. Excessive Notebook Usage for Lightweight Operations

Simple operations such as:

* Table existence checks
* Conditional logic
* Validation

were implemented using PySpark notebooks.

Launching Spark for lightweight logic created unnecessary execution overhead.

---

## 6. Inefficient Audit Logging

Audit logs were updated individually for every processed table.

Consequences:

* Excessive database write operations
* Increased execution latency
* Reduced overall pipeline efficiency

---

# Optimization Strategies

## 1. Enable Shared Spark Session

Enabled **Spark Session Sharing** to allow multiple notebook activities to reuse the same Spark session.

### Benefits

* Eliminated repeated Spark initialization
* Reduced notebook startup latency
* Improved compute resource utilization
* Lowered execution overhead

---

## 2. Parallelize Table Ingestion

Replaced sequential ingestion with parallel execution using **ThreadPoolExecutor**.

Multiple workers ingest multiple tables simultaneously.

### Benefits

* Maximized compute utilization
* Increased throughput
* Significantly reduced ingestion time

---

## 3. Remove Unnecessary Invoke Pipeline Activities

Merged pipeline orchestration into a more streamlined execution flow.

### Benefits

* Reduced pipeline startup overhead
* Eliminated unnecessary context switching
* Simplified orchestration logic

---

## 4. Replace Notebook Activities with Native Fabric Activities

For lightweight operations, PySpark notebooks were replaced by native Fabric Pipeline activities.

Examples include:

* Lookup Activity
* If Condition Activity

### Benefits

* Reduced notebook executions
* Eliminated unnecessary Spark initialization
* Improved pipeline responsiveness

---

## 5. Batch Audit Logging

Instead of updating audit logs after every table, execution results were accumulated and written in batches.

### Benefits

* Fewer database write operations
* Lower logging overhead
* Improved execution performance

---

# Performance After Optimization

## Execution Time

| Metric                 |             Value |
| ---------------------- | ----------------: |
| Average Execution Time |   **~16 minutes** |
| Dataset Size           | **1,000 records** |

---

# Pipeline Execution Comparison

| Pipeline Layer                                |     Version 1 (Before) | Version 2 (After) | Optimization Applied                              |
| --------------------------------------------- | ---------------------: | ----------------: | ------------------------------------------------- |
| Bronze                                        |                21m 03s |            5m 28s | Parallel Execution & Spark Session Sharing        |
| Silver                                        |                18m 00s |            2m 19s | Native Activities & Spark Session Sharing         |
| Gold *(includes Validation & Reconciliation)* |                25m 34s |            6m 16s | Parallel Execution & Reduced Notebook Invocations |
| Validation & Reconciliation                   | Integrated within Gold |            2m 28s | Dedicated Native Activities                       |
| **Total Pipeline**                            |        **~65 minutes** |   **~16 minutes** | **~75% Faster**                                   |

> **Note:** In Version 1, Validation & Reconciliation were executed inside the Gold notebook and therefore cannot be measured separately. In Version 2, they were refactored into dedicated native pipeline activities, making their execution time independently measurable.

---

# Optimization Results

## Overall Improvement

* Pipeline execution time reduced from **~65 minutes** to **~16 minutes**
* Approximately **75% reduction** in total execution time
* Approximately **49 minutes saved** per pipeline execution

---

## Technical Improvements

* Reduced repeated Spark initialization overhead
* Enabled Spark Session reuse across notebook activities
* Improved pipeline scalability through parallel processing
* Increased compute resource utilization
* Reduced notebook execution count
* Simplified pipeline orchestration
* Optimized audit logging with batch updates
* Refactored Validation & Reconciliation into dedicated native pipeline activities
* Improved pipeline maintainability and monitoring

---

# Key Takeaways

The optimization focused not only on reducing execution time but also on improving the overall architecture of the pipeline.

The key architectural improvements include:

* Shared Spark Session
* Parallel table ingestion
* Reduced notebook dependency
* Native Fabric activities for lightweight operations
* Batch audit logging
* Dedicated Validation & Reconciliation stage

These improvements resulted in a faster, more scalable, and easier-to-maintain Microsoft Fabric data pipeline capable of processing **1,000 records** in approximately **16 minutes**, representing an overall performance improvement of approximately **75%**.
