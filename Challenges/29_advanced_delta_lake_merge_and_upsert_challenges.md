# PySpark Challenge 30: The Daily Upsert (Post-Graduate Boss Level)

**The Scenario:**

You manage the core `Dim_Customers` table for an enterprise. 

You have just received today's CDC batch from the software engineering team. It contains two records:

1.  Customer `C1` has updated their email address.
2.  Customer `C3` is a brand new signup.

You need to Upsert this daily batch into the existing Delta table so that `C1` gets updated and `C3` gets added, all without destroying `C2`'s existing data!

**The Setup:**

Run this code to initialize the `SparkSession`, create the historical Delta table, and load today's updates into a DataFrame.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import shutil

# 1. Delta-enabled SparkSession
builder = SparkSession.builder.appName("Challenge_30") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

table_path = "mock_customers_delta"
if os.path.exists(table_path):
    shutil.rmtree(table_path)

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# --- HISTORICAL DATA (What currently exists in the Data Lake) ---
historical_data = [
    ("C1", "Alice", "alice@old_email.com"),
    ("C2", "Bob", "bob@email.com")
]
hist_df = spark.createDataFrame(historical_data, schema)
hist_df.write.format("delta").mode("overwrite").save(table_path)

# --- TODAY'S CDC BATCH (What you need to Upsert) ---
daily_updates = [
    ("C1", "Alice", "alice@NEW_EMAIL.com"), # UPDATE
    ("C3", "Charlie", "charlie@email.com")  # INSERT
]
daily_updates_df = spark.createDataFrame(daily_updates, schema)

print("Historical Data:")
spark.read.format("delta").load(table_path).show()
print("Today's CDC Batch:")
daily_updates_df.show()
```

## Challenge 30 Task:

Write a PySpark script to execute the Delta Lake MERGE.

**Requirements:**

1. Create a `DeltaTable` object named `target_delta_table` that points to `table_path`.
2. Execute a `.merge()` command. Alias the target table as `"t"` and the daily updates DataFrame as `"u"`.
3. Join them on the condition that `t.customer_id == u.customer_id`.
4. Apply the standard `.whenMatchedUpdateAll()` and `.whenNotMatchedInsertAll()` methods, and don't forget to call `.execute()`!
5. Read the final Delta table back into a DataFrame and `.show()` it, ordered by `customer_id`.

**Expected Output (`final_df.show()`):**

```text
+-----------+-------+-------------------+
|customer_id|   name|              email|
+-----------+-------+-------------------+
|         C1|  Alice|alice@NEW_EMAIL.com|
|         C2|    Bob|      bob@email.com|
|         C3|Charlie|  charlie@email.com|
+-----------+-------+-------------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import shutil

# 1. Delta-enabled SparkSession
builder = SparkSession.builder.appName("Challenge_29") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

table_path = "mock_customers_delta"
if os.path.exists(table_path):
    shutil.rmtree(table_path)

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# --- HISTORICAL DATA (What currently exists in the Data Lake) ---
historical_data = [
    ("C1", "Alice", "alice@old_email.com"),
    ("C2", "Bob", "bob@email.com")
]
hist_df = spark.createDataFrame(historical_data, schema)
hist_df.write.format("delta").mode("overwrite").save(table_path)

# --- TODAY'S CDC BATCH (What you need to Upsert) ---
daily_updates = [
    ("C1", "Alice", "alice@NEW_EMAIL.com"), # UPDATE
    ("C3", "Charlie", "charlie@email.com")  # INSERT
]
daily_updates_df = spark.createDataFrame(daily_updates, schema)

print("Historical Data:")
spark.read.format("delta").load(table_path).show()
print("Today's CDC Batch:")
daily_updates_df.show()

target_delta_table = DeltaTable.forPath(spark, table_path)

target_delta_table.alias("t") \
    .merge(
        daily_updates_df.alias("u"),
        "u.customer_id == t.customer_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

target_df = spark.read.format("delta").load(table_path)
target_df = target_df.orderBy("customer_id")
print("Updated DF:")
target_df.orderBy("customer_id").show()
```

### My Output Verification:

```
+-----------+-----+-------------------+
|customer_id| name|              email|
+-----------+-----+-------------------+
|         C1|Alice|alice@old_email.com|
|         C2|  Bob|      bob@email.com|
+-----------+-----+-------------------+

Today's CDC Batch:
+-----------+-------+-------------------+
|customer_id|   name|              email|
+-----------+-------+-------------------+
|         C1|  Alice|alice@NEW_EMAIL.com|
|         C3|Charlie|  charlie@email.com|
+-----------+-------+-------------------+

                                                                                
Updated DF:
                                                                                
+-----------+-------+-------------------+
|customer_id|   name|              email|
+-----------+-------+-------------------+
|         C1|  Alice|alice@NEW_EMAIL.com|
|         C2|    Bob|      bob@email.com|
|         C3|Charlie|  charlie@email.com|
+-----------+-------+-------------------+
```
