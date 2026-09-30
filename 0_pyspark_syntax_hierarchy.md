# 0_pyspark_syntax_hierarchy.md
# PySpark Syntax Hierarchy: The 3 Levels of Distributed Control

PySpark syntax mirrors Pandas structurally, but it operates across a distributed cluster. Commands are grouped by where they execute: cluster-wide context, full relational tables, or isolated column vectors.

---

## Level 1: Global Context & Engine Functions (`spark.` & `F.`)
These commands interact with the master node. You use them to ingest data, configure the cluster environment, or call built-in SQL functions that execute across the entire Catalyst Optimizer.

* **SparkSession Initialization:** `spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()`
  * *Why:* The universal entry point for PySpark 2.0+ that configures the JVM, executors, and application name.
* **Reading Data (with strict schemas):** `df = spark.read.schema(strict_schema).option("mode", "PERMISSIVE").csv("path/")`
  * *Why:* Ingests data where `mode` controls corrupt record handling (`PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`).
* **Global SQL Functions:** `F.col('age')`, `F.lit(10)`
  * *Why:* Generating abstract expression objects that tell Spark how to process data before applying it to a specific table.
* **User Defined Functions (UDFs):** `@udf(returnType=StringType())`
  * *Why:* Executes custom Python logic against DataFrame columns, though it bypasses the Catalyst optimizer and is slower than native functions.

---

## Level 2: DataFrame / Table-Level Methods (`df.method()`)
These actions are called on an instantiated distributed DataFrame. They trigger shuffling, alter the table schema, filter row counts, or write the final output to disk.

* **Creating/Modifying Columns:** `df.withColumn("new_col", col("old_col") * 10)`
  * *Why:* Appends a new column or overwrites an existing one using vectorized expressions.
* **Filtering Rows:** `df.filter((col("age") >= 18) & col("name").isNotNull())`
  * *Why:* Drops rows that do not match the boolean condition.
* **Grouped Aggregations:** `df.groupBy("region").agg(sum("sales").alias("total"))`
  * *Why:* Shuffles data across the cluster to compute summary statistics per categorical group.
* **Distributed Joins:** `df_fact.join(broadcast(df_dim), on="user_id", how="left")`
  * *Why:* Merges distributed datasets, wrapping small lookup tables in `broadcast()` to prevent expensive network shuffles.
* **Writing Data (Delta Lake):** `df.write.format("delta").mode("overwrite").partitionBy("date").save("s3://bucket/table")`
  * *Why:* Serializes DataFrames to distributed storage, physically dividing folders by partition columns for faster read queries.
* **Delta Lake Upserts (MERGE):** `dt.alias("target").merge(df.alias("src"), "target.id = src.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`
  * *Why:* Performs ACID-compliant row-level updates and inserts on massive Data Lakes.
* **Delta Lake Vacuum & Optimize:** `dt.optimize().executeCompaction()` and `dt.vacuum(retentionHours=168)`
  * *Why:* `optimize()` solves the small-file problem by combining files, while `vacuum()` deletes historical time-travel data to save storage costs.
* **Pandas API on Spark:** `ps_df = df.pandas_api()`
  * *Why:* Allows Data Scientists to write native Pandas syntax that executes distributed across the PySpark cluster.

---

## Level 3: Column / Vector Expressions (`col().method()`)
These operations are chained directly onto a Column object (`F.col()`). They dictate exactly how individual elements within that specific vector should be cast, renamed, or windowed.

* **Conditional Logic:** `when(col("code") == 200, "OK").otherwise("ERROR")`
  * *Why:* The PySpark equivalent of a SQL `CASE WHEN` statement for element-wise logic.
* **Window Functions:** `lag("action", 1).over(Window.partitionBy("user_id").orderBy("timestamp"))`
  * *Why:* Computes running totals, ranking, or lead/lag row metrics within isolated partitions.
* **Type Casting:** `F.col('price_str').cast('double')`
  * *Why:* Modifying the underlying JVM data type of a single vector.
