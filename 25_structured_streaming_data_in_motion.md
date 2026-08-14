# PySpark Learning Log: Part 23 - Structured Streaming (Data in Motion)

Up to this point, we have been using PySpark for **Batch Processing** (reading a fixed, static file, transforming it, and stopping). However, modern data engineering often requires real-time insights from continuous data feeds (like Kafka, IoT sensors, or active database logs). 

PySpark handles this using **Structured Streaming**. It treats a live data stream as a table that is being continuously appended.

## 1. Reading Streams (`readStream`)

Unlike `spark.read`, which reads data once, `spark.readStream` keeps a constant connection open, pulling in new data in "micro-batches" as it arrives. 

**Crucial Difference:** When streaming, PySpark *cannot* infer the schema automatically because it doesn't have the full dataset to look at. You **must** define a strict schema beforehand.

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Define the schema explicitly
stream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# 2. Open the continuous stream
# (e.g., listening to a folder for new CSV files)
streaming_df = (
    spark.readStream
    .schema(stream_schema)
    .csv("/path/to/listening/folder", header=True)
)
```

## 2. Transforming Streams

The beauty of Structured Streaming is that the DataFrame API is exactly the same! You can use `.filter()`, `.withColumn()`, `.join()`, and `.when()` exactly as you learned in batch processing. PySpark's Catalyst Optimizer automatically translates your batch logic into streaming logic.

```python
from pyspark.sql.functions import col

# Transform the live stream exactly like a batch DataFrame
clean_stream_df = streaming_df.filter(col("amount") > 0)
```

## 3. Writing Streams (`writeStream`)

You cannot use `.show()` or `.write` on a streaming DataFrame because the data never stops! Instead, you must use `.writeStream` to direct the continuous flow into a "Sink" (a destination).

You must also define an **Output Mode**:

*   `"append"`: Only new rows are added to the sink (standard for logs/events).
*   `"complete"`: The entire transformed table is rewritten to the sink every time an update occurs (requires an aggregation).
*   `"update"`: Only rows that were updated since the last batch are written.

```python
# Start the stream and write the output into a live, temporary in-memory table
query = (
    clean_stream_df.writeStream
    .format("memory")          # The Sink: memory (great for Notebook testing)
    .queryName("live_events")  # The name of the temporary SQL table
    .outputMode("append")      # The Mode: just add new rows
    .start()                   # ACTION: This actually starts the engine!
)

# In a notebook, you would query the live memory sink using Spark SQL:
# spark.sql("SELECT * FROM live_events").show()

# To stop the stream manually:
# query.stop()
```
