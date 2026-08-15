# PySpark Learning Log: Part 24 - Stateful Streaming & Watermarking

In the last lesson, we processed a stream row-by-row (using `filter` and `when`). This is called **Stateless Streaming**, and it requires very little memory. 

However, if you want to calculate the total sales per hour on a live stream, PySpark has to remember previous rows to keep a running total. This is called **Stateful Streaming**. Because a stream never stops, PySpark's memory (its "State") will grow infinitely until the cluster crashes.

To fix this, we use **Windows** and **Watermarks**.

## 1. Time-Based Windows

Instead of just grouping by a category (e.g., `store_id`), we group by a specific window of time using the `window()` function. 

*   **Tumbling Window:** A fixed, non-overlapping chunk of time (e.g., exactly 10:00 to 10:15).
*   **Sliding Window:** A chunk of time that overlaps (e.g., a 10-minute window that updates every 5 minutes).

```python
from pyspark.sql.functions import window, col, sum

# Group by 10-minute chunks based on the 'event_time' column
windowed_df = streaming_df.groupBy(
    window(col("event_time"), "10 minutes"), 
    col("store_id")
).agg(sum("sales"))
```

## 2. Watermarking (Preventing OOM Crashes)

Even with windows, PySpark doesn't know when it is "safe" to throw away old data, because a late sensor reading from yesterday could technically arrive today.

A **Watermark** tells PySpark: *"If data arrives X minutes later than the maximum event time we have seen so far, ignore it and drop the old state from memory."*

You **must** define a watermark before a stateful aggregation to keep your streaming pipeline stable!

```python
# 1. Define the Watermark FIRST
# "Keep the state open for 15 minutes to allow for late data"
watermarked_df = streaming_df.withWatermark("event_time", "15 minutes")

# 2. Then apply the Windowed Aggregation
final_df = watermarked_df.groupBy(
    window(col("event_time"), "10 minutes")
).count()
```

## 3. Output Modes for Stateful Streams

When outputting stateful aggregations using `.writeStream`, you cannot use `"append"` mode right away (because the total is still updating). You must use:

*   `"update"`: Only outputs rows to the sink if their aggregated total has changed since the last micro-batch.
*   `"complete"`: Rewrites the *entire* aggregated table to the sink every time a new micro-batch processes.

