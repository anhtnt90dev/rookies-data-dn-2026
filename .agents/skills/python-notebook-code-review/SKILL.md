# PySpark Notebook Best Practices: Skill Review

This document serves as a skill review checklist and rubric for AI agents and human reviewers evaluating PySpark notebooks. It outlines the key criteria for performance, stability, and code quality.

## 1. Spark Session Management
- [ ] **Instantiation**: Uses `SparkSession.builder.getOrCreate()` rather than creating a new session unconditionally to avoid conflicts.
- [ ] **Configuration Optimization**: Checks if configurations like `spark.sql.shuffle.partitions` are explicitly set relative to the data volume, rather than relying on the default (200).
- [ ] **Resource Cleanup**: Concludes the notebook with `spark.stop()` to release resources, which is especially critical in shared cluster environments.

## 2. Transformation Efficiency (Catalyst Optimizer)
- [ ] **Native Functions vs. UDFs**: Prefers built-in `pyspark.sql.functions` over Python User-Defined Functions (UDFs). UDFs force data serialization between the JVM and Python, which breaks Catalyst optimization.
- [ ] **Minimizing `withColumn` in Loops**: Flags iterations that call `withColumn` repeatedly. Recommends using `select(*exprs)` to project all new columns in a single, optimized transformation plan.
- [ ] **Filter Early (Predicate Pushdown)**: Applies `filter()` or `where()` operations as early as possible in the pipeline before joins or wide transformations.

## 3. Action Control & Driver Memory (OOM Prevention)
- [ ] **`collect()` Guardrails**: Flags `df.collect()` or `df.toPandas()` unless preceded by aggressive filtering, sampling, or aggregation. This prevents Driver Out-of-Memory (OOM) crashes.
- [ ] **Displaying Data**: Prefers `display(df)` (if working in Databricks/Fabric) or `df.show(n, truncate=False)` over pulling data back to the driver just for inspection.

## 4. Shuffling & Partition Management
- [ ] **Join Optimization**: Leverages `broadcast(small_df)` for joining small reference tables with large fact tables to eliminate expensive network shuffles.
- [ ] **`coalesce()` vs `repartition()`**: 
    - Verifies the use of `coalesce()` to reduce partitions (since it avoids a full shuffle).
    - Verifies the use of `repartition()` to increase partitions or resolve data skew by partitioning on a specific column.

## 5. Caching Strategy
- [ ] **Strategic Caching**: Uses `.cache()` or `.persist()` *only* if the DataFrame is evaluated by multiple subsequent actions. Caching unused DataFrames wastes memory.
- [ ] **Unpersisting**: Explicitly calls `.unpersist()` when the cached DataFrame is no longer needed in the notebook lifecycle.

## 6. Idempotency & Notebook State
- [ ] **Idempotent Writes**: Save operations use `mode("overwrite")` where appropriate so re-running the notebook does not accidentally duplicate data.
- [ ] **Stateless Execution**: Code does not rely on variables lingering in memory from previously deleted cells or out-of-order execution (a common notebook anti-pattern).

## 7. Code Modularity & Style
- [ ] **Chaining**: Uses method chaining with line breaks for readability instead of reassigning the same variable repeatedly.
    ```python
    # Recommended Style
    df_transformed = (df
        .filter(col("status") == "active")
        .groupBy("department")
        .agg(count("*").alias("count"))
    )
    ```
- [ ] **Modular Functions**: Reusable DataFrame transformations are cleanly encapsulated in standard Python functions that take and return DataFrames.