# Recap: Time-Based Windows and Watermarks

First, take a look at the aggregation step from your successful script:

```python
final_df = (
    watermarked_df
    # HERE IS THE WINDOW!
    .groupBy(window(col("event_time"), "5 minutes")) 
    .count()
    .withColumnRenamed("count", "checkout_count")
)
```

By passing `window(...)` into the `groupBy()`, you told PySpark to group the data into 5-minute buckets based on the `event_time`, rather than grouping by a static category (like `store_id`).

## When to use Time-Based Windows?

**Use them whenever the business asks for "rolling" or "interval" metrics on a continuous stream.**

If you just run a standard `.groupBy("store_id").count()`, PySpark will give you an ever-increasing, all-time grand total of checkouts since the stream started. 

But in the real world, dashboards usually want to see:
*   "Sales *per hour*"
*   "Website errors *per 5 minutes*"
*   "Average temperature *per 10-second window*"

You use `window()` to slice that never-ending stream into readable chunks of time.

## When to use Watermarking?

**Use them ALWAYS when doing a time-based window aggregation on a streaming DataFrame.**

In a continuous stream, PySpark keeps the totals for your windows in its RAM. A watermark (`.withWatermark("event_time", "10 minutes")`) tells PySpark:
> *"If 10 minutes have passed since the end of a 5-minute window, you are officially allowed to drop that window's total from your memory. We will ignore any data that arrives later than that."*

---

# What if we don't want to discard late data?

This is a fantastic Data Engineering question. If a mobile user loses internet, their checkout event might arrive 3 hours late. If you set a 10-minute watermark, PySpark throws that data in the trash. 

Here are the 3 ways Data Engineers handle this in the real world:

### Option 1: Increase the Watermark (The RAM tradeoff)

If you want to accept data that is up to 3 days late, you can set `.withWatermark("event_time", "3 days")`. The downside? PySpark now has to hold 3 days' worth of 5-minute windows in its active RAM. This is expensive and can still lead to cluster crashes if traffic spikes.

### Option 2: Stateless Streaming (The typical real-world approach)

Instead of aggregating the data in real-time, you just stream the raw events directly to a Data Lake (like Databricks Delta Lake or AWS S3) using `"append"` mode. Because you aren't using `groupBy()`, you don't need a watermark, and PySpark uses almost zero RAM.

Then, you schedule a standard **Batch Job** to run every hour. The batch job reads all the raw data (including late arrivals) and calculates the `groupBy()` perfectly accurately. 

### Option 3: Delta Lake MERGE (Advanced)

Modern data architectures use tools like **Apache Hudi** or **Delta Lake**. You can stream aggregations with a short watermark for a "fast, mostly accurate" live dashboard. Meanwhile, late-arriving data is intercepted by a separate process that uses a SQL `MERGE` or `UPSERT` command to patch the historical tables retroactively.
