# PySpark Learning Log: Bonus Part 32 - The Pandas API on Spark

Data Engineers love the PySpark DataFrame API, but Data Scientists generally prefer the standard Python `pandas` library. Historically, if a Data Scientist wanted to process 500GB of data, their `pandas` script would crash with an Out-of-Memory (OOM) error because `pandas` only runs on a single machine.

To bridge this gap, Databricks created an open-source project called "Koalas", which was officially integrated into Spark 3.2+ as the **Pandas API on Spark**.

It allows you to write almost exact `pandas` code, but under the hood, Spark's Catalyst Optimizer translates it into distributed PySpark execution plans!

## 1. Importing and Converting

You import the API using `pyspark.pandas`. You can seamlessly convert standard PySpark DataFrames into Spark Pandas DataFrames, and vice versa.

```python
import pyspark.pandas as ps

# Scenario 1: Create a Spark Pandas DataFrame directly
ps_df = ps.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

# Scenario 2: Convert an existing PySpark DataFrame to a Spark Pandas DataFrame
# (This does NOT pull the data into local memory; it remains distributed)
ps_df = spark_df.pandas_api()

# Scenario 3: Convert it back to a PySpark DataFrame
spark_df_again = ps_df.to_spark()
```

## 2. Using Pandas Syntax

Once it is a Spark Pandas DataFrame, you can stop using PySpark functions like `.withColumn()` or `col()` and use standard Pandas syntax instead.

```python
# Create a new column using standard Pandas brackets
ps_df['C'] = ps_df['A'] + ps_df['B']

# Standard Pandas filtering
filtered_ps_df = ps_df[ps_df['A'] > 1]

# Standard Pandas aggregations
summary_ps_df = ps_df.groupby('A').mean()

# View the head (instead of .show())
print(summary_ps_df.head())
```

**Why not use this all the time?**

While it is incredibly convenient, the native PySpark DataFrame API is still slightly faster and offers more robust Big Data controls (like specific broadcast joins, complex window bounding, and structured streaming). However, this API is perfect for migrating legacy Python scripts to Big Data!
