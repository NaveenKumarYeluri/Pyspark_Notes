# 0_pyspark_syntax_cheatsheet.md
# PySpark Core Architecture Syntax Cheatsheet

This single-file document serves as a high-density reference sheet for PySpark syntax mapping, distributed operations, and data lake engineering patterns.

---

## 1. Core Architecture, I/O & Session Management

### SparkSession Initialization
* **Syntax:** `SparkSession.builder.appName('name').getOrCreate()`
* **Explanation:** The universal entry point for PySpark 2.0+. Configures the JVM, connects to the cluster manager, and sets the application name.
* **Example:** `spark = SparkSession.builder.appName("ETL_Pipeline").master("local[*]").getOrCreate()`

### Strict Schema Ingestion (`spark.read`)
* **Syntax:** `spark.read.schema(schema_obj).option('mode', 'PERMISSIVE').csv('path')`
* **Explanation:** Ingests external files into distributed memory using predefined schemas to catch corrupt records early.
* **Example:** `df = spark.read.schema(user_schema).option("header", True).csv("s3://bucket/raw_data/")`

### Delta Lake Partitioned Writes (`.write`)
* **Syntax:** `df.write.format('delta').mode('overwrite').partitionBy('col').save('path')`
* **Explanation:** Serializes DataFrames to distributed cloud storage, dividing files physically by partition columns for faster analytical reads.
* **Example:** `df.write.format("delta").mode("append").partitionBy("region").save("s3://bucket/trusted_users")`

### Pandas API on Spark
* **Syntax:** `df.pandas_api()`
* **Explanation:** Converts a PySpark DataFrame into a pandas-on-Spark DataFrame, allowing Data Scientists to execute native Pandas syntax across a distributed cluster.
* **Example:** `ps_df = df.pandas_api(); final_df = ps_df.groupby('city').mean().to_spark()`

---

## 2. Structural Matrix Transformations & Selection

### Column Mutation & Creation (`.withColumn()`)
* **Syntax:** `df.withColumn('new_col_name', expression)`
* **Explanation:** Appends a new column or safely overwrites an existing one across all partitions using vectorized expressions.
* **Example:** `df = df.withColumn("total_cost", F.col("price") * F.col("qty"))`

### Horizontal Pruning (`.select()` & `.drop()`)
* **Syntax:** `df.select('col1', 'col2')` | `df.drop('col3')`
* **Explanation:** Subsets the vertical column vectors. `.select()` is inclusive, `.drop()` is exclusive.
* **Example:** `df_clean = df.select("user_id", "email").drop("internal_token")`

### Vertical Row Filtering (`.filter()` / `.where()`)
* **Syntax:** `df.filter(boolean_condition)`
* **Explanation:** Prunes rows dynamically across the cluster that do not meet the specified logical condition.
* **Example:** `df = df.filter((F.col("age") >= 18) & F.col("email").isNotNull())`

### Renaming Columns (`.withColumnRenamed()`)
* **Syntax:** `df.withColumnRenamed('old_name', 'new_name')`
* **Explanation:** Alters the schema metadata to rename a specific column without mutating the underlying data.
* **Example:** `df = df.withColumnRenamed("tx_id", "transaction_id")`

---

## 3. Vectorized Element Logic & Conditionals

### Logical Branching (`when().otherwise()`)
* **Syntax:** `F.when(condition, true_val).otherwise(false_val)`
* **Explanation:** The distributed PySpark equivalent of a SQL `CASE WHEN` statement for element-wise conditional logic.
* **Example:** `df = df.withColumn("status", F.when(F.col("code") == 200, "OK").otherwise("ERROR"))`

### Type Casting (`.cast()`)
* **Syntax:** `F.col('column').cast('datatype')`
* **Explanation:** Re-allocates the underlying JVM data type of a single column vector.
* **Example:** `df = df.withColumn("price", F.col("price_str").cast("double"))`

### Regex Extraction (`regexp_extract()`)
* **Syntax:** `F.regexp_extract(F.col('target'), r'pattern', group_index)`
* **Explanation:** Mines unstructured string vectors using regex capture groups.
* **Example:** `df = df.withColumn("error_code", F.regexp_extract(F.col("log"), r'ERROR:(\d+)', 1))`

### Regex Replacement (`regexp_replace()`)
* **Syntax:** `F.regexp_replace(F.col('target'), r'pattern', 'replacement')`
* **Explanation:** Dynamically matches complex text patterns and replaces them concurrently across the cluster.
* **Example:** `df = df.withColumn("clean_tag", F.regexp_replace(F.col("raw_tag"), r'\s\[v\d+\]', ''))`

---

## 4. Aggregations, Relational Lookups & Windows

### Grouped Metric Aggregation (`.groupBy().agg()`)
* **Syntax:** `df.groupBy('col').agg(function('col').alias('name'))`
* **Explanation:** Shuffles data across cluster nodes to compute dimensional summary statistics.
* **Example:** `df.groupBy("region").agg(F.sum("sales").alias("total_sales"), F.avg("latency").alias("avg_ping"))`

### Distributed Relational Joins (`.join()`)
* **Syntax:** `df_left.join(F.broadcast(df_right), on='shared_key', how='left')`
* **Explanation:** Merges distributed datasets. Wrapping small lookup tables in `F.broadcast()` prevents catastrophic network shuffles.
* **Example:** `df_enriched = df_tx.join(F.broadcast(df_dim), on="store_id", how="inner")`

### Isolated Window Analytics (`Window.partitionBy()`)
* **Syntax:** `Window.partitionBy('group_col').orderBy('sort_col')`
* **Explanation:** Creates logical boundaries to compute running totals, rankings, or lead/lag metrics without collapsing the original rows.
* **Example:** `w = Window.partitionBy("user_id").orderBy("timestamp"); df = df.withColumn("prev_login", F.lag("timestamp", 1).over(w))`

---

## 5. ACID Transactions & Data Lake Maintenance

### Delta Lake Upserting / Merging (`.merge()`)
* **Syntax:** `dt.alias('t').merge(df.alias('s'), 't.id = s.id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`
* **Explanation:** Performs ACID-compliant row-level updates and inserts on massive Data Lakes without rewriting the entire table.
* **Example:** `target_table.alias("target").merge(updates_df.alias("src"), "target.uuid = src.uuid").whenMatchedUpdateAll().execute()`

### Storage Optimization (`.optimize()` & `.vacuum()`)
* **Syntax:** `dt.optimize().executeCompaction()` | `dt.vacuum(retentionHours)`
* **Explanation:** `.optimize()` physically packs small files into large Parquet files. `.vacuum()` deletes stale historical data to save cloud storage costs.
* **Example:** `dt.optimize().executeCompaction(); dt.vacuum(168)`
