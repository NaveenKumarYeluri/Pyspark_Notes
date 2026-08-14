# PySpark Challenge 23: The Real-Time Fraud Monitor (Boss Level)

**The Scenario:**

You are building a real-time transaction monitoring pipeline for a FinTech company. The backend systems are continuously dumping new credit card transaction CSVs into a secure directory. 

You need to write a streaming job that listens to this directory, evaluates the live transactions using conditional logic to flag potential fraud, and writes the stream to a live in-memory table so analysts can monitor it via SQL.

**The Setup:**

Run this code first. It will create a temporary directory on your machine and generate three mock CSV files to simulate a data stream.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import time

spark = SparkSession.builder.appName("Challenge_23").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create a temporary directory for our stream
stream_dir = "mock_transaction_stream"
os.makedirs(stream_dir, exist_ok=True)

# Generate Mock CSV Data (simulating data arriving over time)
csv1 = "txn_id,account_id,location,amount\nT101,A_1,Domestic,50.00\nT102,A_2,International,5500.00\n"
csv2 = "txn_id,account_id,location,amount\nT103,A_1,Domestic,12000.00\nT104,A_3,Domestic,5.00\n"
csv3 = "txn_id,account_id,location,amount\nT105,A_2,International,150.00\nT106,A_4,Domestic,200.00\n"

for i, data in enumerate([csv1, csv2, csv3]):
    with open(f"{stream_dir}/batch_{i}.csv", "w") as f:
        f.write(data)
    time.sleep(1) # simulate slight delay

# Schema required for the streaming read
txn_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("amount", DoubleType(), True)
])
```

## Challenge 23 Task:

Write a PySpark pipeline to ingest, transform, and output this stream. 

**Business Goals & Expected Output:**

1.  Use `spark.readStream` with the provided `txn_schema` to read the CSV files from the `"mock_transaction_stream"` directory (make sure to set `header=True`).
2.  Filter the stream to completely drop any transactions where the `amount` is less than `$10.00`.
3.  Add a conditional `fraud_alert` column:
    *   If the `amount` is strictly greater than `10000.00`, flag it as `"HIGH_RISK"`.
    *   If the `location` is `"International"` AND the `amount` is strictly greater than `5000.00`, also flag it as `"HIGH_RISK"`.
    *   Otherwise, flag it as `"CLEAN"`.
4.  Write the transformed stream using `.writeStream`. 
    *   Format: `"memory"`
    *   Query Name: `"fraud_monitor"`
    *   Output Mode: `"append"`
    *   Trigger: `.start()`
    *   *Assign the resulting StreamingQuery object to a variable named `streaming_query`.*
5.  Wait `3` seconds (`time.sleep(3)`) to let the micro-batches process, then use `spark.sql()` to select everything from the `fraud_monitor` table and show the results, ordered by `txn_id`.
6.  Finally, call `streaming_query.stop()` to kill the active stream.

**Expected Output (`spark.sql(...).show()`):**

```text
+------+----------+-------------+-------+-----------+
|txn_id|account_id|     location| amount|fraud_alert|
+------+----------+-------------+-------+-----------+
|  T101|       A_1|     Domestic|   50.0|      CLEAN|
|  T102|       A_2|International| 5500.0|  HIGH_RISK|
|  T103|       A_1|     Domestic|12000.0|  HIGH_RISK|
|  T105|       A_2|International|  150.0|      CLEAN|
|  T106|       A_4|     Domestic|  200.0|      CLEAN|
+------+----------+-------------+-------+-----------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, when
import os
import time

spark = SparkSession.builder.appName("Challenge_21").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# # Find and stop the active query by name
# for query in spark.streams.active:
#     if query.name == "live_events":
#         query.stop()

# Create a temporary directory for our stream
stream_dir = "mock_transaction_stream"
os.makedirs(stream_dir, exist_ok=True)

# Generate Mock CSV Data (simulating data arriving over time)
csv1 = "txn_id,account_id,location,amount\nT101,A_1,Domestic,50.00\nT102,A_2,International,5500.00\n"
csv2 = "txn_id,account_id,location,amount\nT103,A_1,Domestic,12000.00\nT104,A_3,Domestic,5.00\n"
csv3 = "txn_id,account_id,location,amount\nT105,A_2,International,150.00\nT106,A_4,Domestic,200.00\n"

for i, data in enumerate([csv1, csv2, csv3]):
    with open(f"{stream_dir}/batch_{i}.csv", "w") as f:
        f.write(data)
    time.sleep(1) # simulate slight delay

# Schema required for the streaming read
txn_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("amount", DoubleType(), True)
])

streaming_df = (
    spark
    .readStream
    .schema(txn_schema)
    .csv(stream_dir, header=True)
)

clean_stream_df = (
    streaming_df
    .filter(col("amount") >= 10)
    .withColumn("fraud_alert", 
        when(col("amount") > 10000.00, "HIGH_RISK")
        .when((col("location") == "International") & (col("amount") > 5000.00), "HIGH_RISK")
        .otherwise("CLEAN")
    )
)

streaming_query = (
    clean_stream_df
    .writeStream
    .format("memory")
    .queryName("fraud_monitor")
    .outputMode("append")
    .start()
)
time.sleep(3)

spark.sql("SELECT * FROM fraud_monitor ORDER BY txn_id").show()
streaming_query.stop()
```

### My Output Verification:

```
+------+----------+-------------+-------+-----------+
|txn_id|account_id|     location| amount|fraud_alert|
+------+----------+-------------+-------+-----------+
|  T101|       A_1|     Domestic|   50.0|      CLEAN|
|  T102|       A_2|International| 5500.0|  HIGH_RISK|
|  T103|       A_1|     Domestic|12000.0|  HIGH_RISK|
|  T105|       A_2|International|  150.0|      CLEAN|
|  T106|       A_4|     Domestic|  200.0|      CLEAN|
+------+----------+-------------+-------+-----------+
```
