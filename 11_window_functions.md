# PySpark Learning Log: Part 9 - Window Functions

Up until now, when we wanted to calculate an aggregate (like a sum or average), we used `.groupBy()`. However, `groupBy()` collapses all the rows into a single summary row. 

What if you want to calculate the average ticket price for an airline, but you *also* want to keep all the original flight rows intact to compare each specific flight against that average? 

This is where **Window Functions** come in. They allow you to perform calculations across a specific "window" of rows related to the current row, without collapsing the dataset.

## 1. The Anatomy of a Window

To use a Window function, you must first define the "Window Specification." This tells PySpark how to group and order the rows for your calculation. You do this using the `Window` class.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Defining a Window Specification
# 1. partitionBy: How to group the data (like the GROUP BY clause)
# 2. orderBy: How to sort the data within that group
window_spec = Window.partitionBy("airline").orderBy(col("ticket_price").desc())
```

## 2. Using Window Functions (Ranking)

A very common use case is ranking items within a category. For example, ranking flights from most expensive to least expensive, *per airline*.

```python
from pyspark.sql.functions import rank

# Assuming we have a 'flights_df'
# We use .withColumn to add our new calculated rank
ranked_df = flights_df.withColumn(
    "price_rank", 
    rank().over(window_spec)  # The .over() method applies the window spec!
)

ranked_df.show()
```

## 3. Using Aggregate Functions over a Window

You can also use standard aggregate functions (like `sum`, `avg`, `max`) over a window. 

If we want to find the maximum ticket price for each airline and put that value on every single row for comparison:

```python
from pyspark.sql.functions import max

# Notice we don't need 'orderBy' if we just want the max for the whole partition
agg_window_spec = Window.partitionBy("airline")

compared_df = flights_df.withColumn(
    "max_airline_price", 
    max("ticket_price").over(agg_window_spec)
)

compared_df.show()
```

## 4. Why are Window Functions Important?

Window functions are essential for complex analytics:

1. **Running Totals:** Calculating cumulative sums over time.
2. **Moving Averages:** Smoothing out time-series data (e.g., 7-day moving average).
3. **Lead/Lag:** Comparing a row's value to the previous row or next row (e.g., finding the time difference between consecutive orders).
