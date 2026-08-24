# PySpark Challenge 31: The Janitor (The Final Maintenance)

**The Scenario:**

Your automated CDC pipeline from Challenge 30 has been running flawlessly for months. However, the Cloud Infrastructure team just sent you a warning: the `mock_logs_delta` table contains way too many tiny files, and the historical storage costs are way over budget.

You need to compact the table to improve query speed, and then aggressively vacuum the table to delete all historical versions (0 hours retention) to save money.

**The Setup:**

Run this code to simulate the problem. It will loop 5 times, appending a tiny 1-row DataFrame to the Delta table each time. This simulates the "Small File Problem" and creates 5 distinct historical versions.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
import os
import shutil

builder = SparkSession.builder.appName("Challenge_31") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

table_path = "mock_logs_delta"
if os.path.exists(table_path):
    shutil.rmtree(table_path)

schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("message", StringType(), True)
])

# Simulate 5 separate tiny batch appends
print("Generating fragmented data...")
for i in range(5):
    tiny_df = spark.createDataFrame([(f"L{i}", f"Message {i}")], schema)
    tiny_df.write.format("delta").mode("append").save(table_path)

dt = DeltaTable.forPath(spark, table_path)
print("Before Maintenance:")
dt.history().select("version", "operation").show()
```

## Challenge 31 Task:

Write a script to clean up the `mock_logs_delta` table.

**Requirements:**

1. Create a `DeltaTable` object named `maintenance_table` pointing to `table_path`.
2. Run the `.optimize().executeCompaction()` command on the table.
3. Because we want to delete history immediately for this test, disable the retention safety check by running: `spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")`
4. Run the `.vacuum(0)` command on the table to permanently delete all old versions.
5. Print the table's history one last time using `maintenance_table.history().select("version", "operation").show()`. You will see that `OPTIMIZE` actually gets recorded as its own version in the log!

*(Note: Vacuuming deletes the physical files, but the text log of what happened remains in the `.history()` output so you always have an audit trail).*


### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
import os
import shutil

builder = SparkSession.builder.appName("Challenge_30") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

table_path = "mock_logs_delta"
if os.path.exists(table_path):
    shutil.rmtree(table_path)

schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("message", StringType(), True)
])

# Simulate 5 separate tiny batch appends
print("Generating fragmented data...")
for i in range(5):
    tiny_df = spark.createDataFrame([(f"L{i}", f"Message {i}")], schema)
    tiny_df.write.format("delta").mode("append").save(table_path)

dt = DeltaTable.forPath(spark, table_path)
print("Before Maintenance:")
dt.history().select("version", "operation").show()

maintenance_table = DeltaTable.forPath(spark, table_path)
maintenance_table.optimize().executeCompaction()
# We want to delete hist immediately
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
maintenance_table.vacuum(0)
maintenance_table.history().select("version", "operation").show()
```

### My Output Verification:

```
Before Maintenance:
+-------+---------+
|version|operation|
+-------+---------+
|      4|    WRITE|
|      3|    WRITE|
|      2|    WRITE|
|      1|    WRITE|
|      0|    WRITE|
+-------+---------+

+-------+---------+
|version|operation|
+-------+---------+
|      4|    WRITE|
|      3|    WRITE|
|      2|    WRITE|
|      1|    WRITE|
|      0|    WRITE|
+-------+---------+

                                                                                
Deleted 10 files and directories in a total of 1 directories.
+-------+------------+
|version|   operation|
+-------+------------+
|      7|  VACUUM END|
|      6|VACUUM START|
|      5|    OPTIMIZE|
|      4|       WRITE|
|      3|       WRITE|
|      2|       WRITE|
|      1|       WRITE|
|      0|       WRITE|
+-------+------------+
```
