# PySpark Challenge 25: The E-Commerce Price Correction (Delta Basics)

**The Scenario:**

You are managing an inventory Data Lake. The pricing team uploaded the new catalog to a Delta table. However, they just realized they forgot to apply a mandatory 10% inflation markup to all "Electronics" items.

You need to read the Delta table, apply the markup, and overwrite the table. The Data Science team then needs you to output *both* the original prices (Version 0) and the corrected prices (Version 1) side-by-side so they can audit the change.

**The Setup:**

Run this code to initialize a Delta-enabled SparkSession and write the initial Version 0 data to disk. *(Note: The first time you run this, it will take a few moments to download the Delta Lake packages from Maven).*

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta import configure_spark_with_delta_pip
import os
import shutil

# 1. Delta-enabled SparkSession
builder = SparkSession.builder.appName("Challenge_25") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Clean up previous runs
table_path = "mock_delta_catalog"
if os.path.exists(table_path):
    shutil.rmtree(table_path)

# 2. Version 0 Data
schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

data = [
    ("I_01", "Electronics", 1000.0),
    ("I_02", "Apparel", 50.0),
    ("I_03", "Electronics", 400.0)
]
initial_df = spark.createDataFrame(data, schema)

# Write Version 0 to disk as Delta
initial_df.write.format("delta").mode("overwrite").save(table_path)
print("Version 0 Written successfully.")
```

## Challenge 25 Task:

Write a PySpark script to update the table and audit the versions.

**Requirements:**

1. Read the current Delta table from `table_path` into a DataFrame.
2. Use conditional logic to increase the `price` of all items where `category == "Electronics"` by **10%** (multiply by 1.10). Leave other categories unchanged.
3. Write this modified DataFrame back to `table_path` using `.format("delta").mode("overwrite")`. (This creates Version 1).
4. Use Delta's "Time Travel" feature to read **Version 0** into a DataFrame called `audit_v0_df`, and read **Version 1** into a DataFrame called `audit_v1_df`.
5. Join `audit_v0_df` and `audit_v1_df` together on `item_id`.
6. Select the `item_id`, the `category` (from either dataframe), the old price (aliased as `original_price`), and the new price (aliased as `corrected_price`). Order by `item_id`.

**Expected Output (`audit_report_df.show()`):**

```text
+-------+-----------+--------------+---------------+
|item_id|   category|original_price|corrected_price|
+-------+-----------+--------------+---------------+
|   I_01|Electronics|        1000.0|         1100.0|
|   I_02|    Apparel|          50.0|           50.0|
|   I_03|Electronics|         400.0|          440.0|
+-------+-----------+--------------+---------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import when, col, round
import os
import shutil

# 1. Delta-enabled SparkSession (Manual Package Resolution)
builder = SparkSession.builder.appName("Challenge_24") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")

# Bypass the delta pip utility and build directly
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Clean up previous runs
table_path = "mock_delta_catalog"
if os.path.exists(table_path):
    shutil.rmtree(table_path)

# 2. Version 0 Data
schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

data = [
    ("I_01", "Electronics", 1000.0),
    ("I_02", "Apparel", 50.0),
    ("I_03", "Electronics", 400.0)
]
initial_df = spark.createDataFrame(data, schema)

# Write Version 0 to disk as Delta
initial_df.write.format("delta").mode("overwrite").save(table_path)
print("Version 0 Written successfully.")

delta_df = spark.read.format("delta").load(table_path)

delta_df = (
    delta_df
    .withColumn("price", 
        when(col("category") == "Electronics", round((col("price") * 1.10), 2))
        .otherwise(col("price"))
    )
)
delta_df.write.format("delta").mode("overwrite").save(table_path)

# Read Version 0 (the very first time the table was created)
audit_v0_df = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
# Read Version 1 (after the first update/overwrite)
audit_v1_df = spark.read.format("delta").option("versionAsOf", 1).load(table_path)

item_id = (
    audit_v0_df
    .join(audit_v1_df, on="item_id", how="inner")
    .select(
        "item_id", 
        audit_v0_df["category"],
        audit_v0_df["price"].alias("original_price"),
        audit_v1_df["price"].alias("corrected_price")
    )
    .orderBy("item_id")
)
item_id.show()
```

### My Output Verification:

```
+-------+-----------+--------------+---------------+
|item_id|   category|original_price|corrected_price|
+-------+-----------+--------------+---------------+
|   I_01|Electronics|        1000.0|         1100.0|
|   I_02|    Apparel|          50.0|           50.0|
|   I_03|Electronics|         400.0|          440.0|
+-------+-----------+--------------+---------------+
```
