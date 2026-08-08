# PySpark Learning Log: Part 17 - Advanced Window Analytics (Lead, Lag, and Bounds)

In Part 9, we learned the basics of Window functions for ranking and simple aggregations (`rank`, `sum`, `max`). However, Window functions are also the secret to solving time-series problems, such as comparing a current row to a previous row or calculating moving averages.

## 1. Offsets: Comparing Rows (`lead` and `lag`)

If you want to calculate the time difference between user logins or the daily change in a stock price, you need to access data from the *previous* row or the *next* row.

* `lag(column, offset)`: Looks *backwards* by the offset number of rows.
* `lead(column, offset)`: Looks *forwards* by the offset number of rows.

```python
from pyspark.sql.functions import col, lag, lead
from pyspark.sql.window import Window

# Scenario: We want to see a user's previous purchase amount next to their current purchase
window_spec = Window.partitionBy("user_id").orderBy("purchase_date")

df_offsets = df.withColumn(
    "previous_purchase", 
    lag("amount", 1).over(window_spec)
)
```

## 2. Bounding Windows: Running Totals and Moving Averages

By default, an `orderBy` inside a Window specification creates a bounded window from the "beginning of time" up to the "current row." This is why it works perfectly for running totals.

However, you can manually define the boundaries of your window using `.rowsBetween(start, end)`.

* `Window.unboundedPreceding`: The very first row in the partition.
* `Window.currentRow`: The current row being evaluated.
* Numeric integers: Represent physical row offsets (e.g., `-2` means 2 rows ago, `1` means 1 row ahead).

```python
from pyspark.sql.functions import sum, avg
from pyspark.sql.window import Window

# Scenario 1: A standard Running Total (Cumulative Sum)
# (Implicitly uses rowsBetween(Window.unboundedPreceding, Window.currentRow))
running_total_window = Window.partitionBy("store").orderBy("date")
df.withColumn("running_total", sum("sales").over(running_total_window))

# Scenario 2: A 3-Day Moving Average
# Looks at the current row AND the 2 rows immediately preceding it
moving_avg_window = (
    Window.partitionBy("store")
    .orderBy("date")
    .rowsBetween(-2, Window.currentRow)
)
df.withColumn("3_day_moving_avg", avg("sales").over(moving_avg_window))
```

By defining boundaries, Data Engineers can calculate complex sliding metrics across massive time-series datasets without writing custom loops.
