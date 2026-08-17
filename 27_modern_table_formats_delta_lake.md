# PySpark Learning Log: Part 25 - Modern Table Formats (Delta Lake)

Standard PySpark writes data to disk as Parquet or CSV files. These files are **immutable**—once written, they cannot be changed. If a customer deletes their account or updates their address, you cannot simply `UPDATE` a Parquet file; you have to rewrite the entire folder!

**Delta Lake** solves this. It is an open-source storage layer built *on top* of Parquet. It maintains a transaction log (`_delta_log`) alongside your data files. This enables true database features on massive Data Lakes.

## 1. Setting up Delta Lake

Because Delta is an external package, you must configure your `SparkSession` to download the required JAR files and use the Delta SQL extensions.

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("Delta_Intro") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# This automatically downloads the Delta packages to your local machine
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

## 2. Writing and Reading Delta Tables

Instead of `.csv()` or `.parquet()`, you use `.format("delta")`.

```python
# Write data to a Delta table
df.write.format("delta").mode("overwrite").save("/tmp/my_delta_table")

# Read data from a Delta table
delta_df = spark.read.format("delta").load("/tmp/my_delta_table")
```

## 3. Time Travel (Data Versioning)

Every time you write, overwrite, or update a Delta table, it creates a new "Version". Delta keeps the old Parquet files around until you explicitly vacuum them. This means you can query exactly what the data looked like yesterday!

```python
# Read Version 0 (the very first time the table was created)
v0_df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/my_delta_table")

# Read Version 1 (after the first update/overwrite)
v1_df = spark.read.format("delta").option("versionAsOf", 1).load("/tmp/my_delta_table")
```
