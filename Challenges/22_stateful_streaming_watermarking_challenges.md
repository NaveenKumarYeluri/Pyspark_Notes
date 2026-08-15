# PySpark Challenge 24: The Real-Time Traffic Window (Boss Level)

**The Scenario:**

You are monitoring web traffic for an e-commerce platform. The backend servers are streaming user click events into a directory. Because of network lag, some mobile users' events are arriving slightly late.

The operations team needs a live, rolling count of how many "checkout" actions occur every 5 minutes. They are willing to tolerate data that arrives up to 10 minutes late.

**The Setup:**

Run this code to generate the mock timestamped stream.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import time

spark = SparkSession.builder.appName("Challenge_24").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

stream_dir = "mock_traffic_stream"
os.makedirs(stream_dir, exist_ok=True)

# Format: event_id, event_time, action
# Batch 1 (Arrives at 10:00)
csv1 = "e1,2026-10-15 10:01:00,checkout\ne2,2026-10-15 10:03:00,view_item\ne3,2026-10-15 10:04:00,checkout\n"
# Batch 2 (Arrives at 10:05 - includes a late event 'e4' from 10:02)
csv2 = "e4,2026-10-15 10:02:00,checkout\ne5,2026-10-15 10:06:00,checkout\n"
# Batch 3 (Arrives at 10:10)
csv3 = "e6,2026-10-15 10:11:00,checkout\ne7,2026-10-15 10:12:00,view_item\n"

for i, data in enumerate([csv1, csv2, csv3]):
    with open(f"{stream_dir}/traffic_{i}.csv", "w") as f:
        f.write(data)
    time.sleep(1)

traffic_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("action", StringType(), True)
])
```

## Challenge 24 Task:

Write a PySpark streaming pipeline to aggregate the live traffic.

**Business Goals & Expected Output:**

1. Read the stream from the `"mock_traffic_stream"` directory using `traffic_schema`.
2. Filter the stream to only include events where the `action` is `"checkout"`.
3. Apply a Watermark of `"10 minutes"` to the `event_time` column.
4. Aggregate the stream: Group by a `"5 minute"` tumbling window on `event_time`. Calculate the `.count()` of checkouts in each window. Alias the count column to `checkout_count`.
5. Start the `.writeStream`:
   * Write to a `"memory"` sink named `"traffic_monitor"`.
   * Use `"update"` output mode.
   * Save the query object to a variable named `traffic_query`.
6. Wait `3` seconds, then query the `"traffic_monitor"` table using SQL and show the results ordered by the window start time. Finally, stop the stream.

**Expected Output (`spark.sql(...).show(truncate=False)`):**

```text
+------------------------------------------+--------------+
|window                                    |checkout_count|
+------------------------------------------+--------------+
|{2026-10-15 10:00:00, 2026-10-15 10:05:00}|3             |
|{2026-10-15 10:05:00, 2026-10-15 10:10:00}|1             |
|{2026-10-15 10:10:00, 2026-10-15 10:15:00}|1             |
+------------------------------------------+--------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, window
import os
import time

spark = SparkSession.builder.appName("Challenge_22").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for query in spark.streams.active:
    query.stop()

stream_dir = "mock_traffic_stream"
os.makedirs(stream_dir, exist_ok=True)

# Format: event_id, event_time, action
# Batch 1 (Arrives at 10:00)
csv1 = "e1,2026-10-15 10:01:00,checkout\ne2,2026-10-15 10:03:00,view_item\ne3,2026-10-15 10:04:00,checkout\n"
# Batch 2 (Arrives at 10:05 - includes a late event 'e4' from 10:02)
csv2 = "e4,2026-10-15 10:02:00,checkout\ne5,2026-10-15 10:06:00,checkout\n"
# Batch 3 (Arrives at 10:10)
csv3 = "e6,2026-10-15 10:11:00,checkout\ne7,2026-10-15 10:12:00,view_item\n"

for i, data in enumerate([csv1, csv2, csv3]):
    with open(f"{stream_dir}/traffic_{i}.csv", "w") as f:
        f.write(data)
    time.sleep(1)

traffic_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("action", StringType(), True)
])

streaming_df = (
    spark.readStream
    .schema(traffic_schema)
    .csv(stream_dir, header=True)
)

clean_stream_df = (
    streaming_df
    .filter(col("action") == "checkout")
)

watermarked_df = clean_stream_df.withWatermark("event_time", "10 minutes")

final_df = (
    watermarked_df
    .groupBy(window(col("event_time"), "5 minutes"))
    .count()
    .withColumnRenamed("count", "checkout_count")
)

traffic_query = (
    final_df
    .writeStream
    .format("memory")
    .queryName("traffic_monitor")
    .outputMode("update")
    # checkpointLocation: forces Spark to safely write local state files to a dedicated folder on local disk
    # rather than relying on unmanaged temp memory spaces.
    .option("checkpointLocation", f"checkpoint_{int(time.time())}")
    .start()
)
# Tell Spark to wait until all current data is fully processed
traffic_query.processAllAvailable()

spark.sql("SELECT * FROM traffic_monitor").show(truncate=False)
traffic_query.stop()
```

### My Output Verification:

```
+------------------------------------------+--------------+
|window                                    |checkout_count|
+------------------------------------------+--------------+
|{2026-10-15 10:05:00, 2026-10-15 10:10:00}|1             |
|{2026-10-15 10:00:00, 2026-10-15 10:05:00}|1             |
+------------------------------------------+--------------+
```
