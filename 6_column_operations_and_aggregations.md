# PySpark Learning Log: Part 4 - Column Operations and Aggregations

Once you know how to filter and select data, the next step is learning how to modify columns and summarize your data. 

## 1. Modifying and Creating Columns (`withColumn`)

In PySpark, you cannot modify a DataFrame in place. Instead, you create a new DataFrame with the added or modified column using the `withColumn()` method. It takes two arguments: the name of the column as a string, and the transformation logic.

```python
from pyspark.sql.functions import col, round

# 1. Create a NEW column (e.g., adding a $50 baggage fee to the ticket price)
df_with_fee = df.withColumn("total_price", col("ticket_price") + 50)
df_with_fee.show()

# 2. OVERWRITE an existing column (e.g., rounding the ticket price)
df_rounded = df.withColumn("ticket_price", round(col("ticket_price"), 1))
df_rounded.show()
```

## 2. Renaming Columns (`withColumnRenamed`)

If you just need to change the name of a column without altering its data, use `withColumnRenamed()`.

```python
# Rename 'departure_code' to 'airport_code'
df_renamed = df.withColumnRenamed("departure_code", "airport_code")
df_renamed.show()
```

## 3. Changing Data Types (`cast`)

Often, data gets loaded in as the wrong type (e.g., numbers loaded as strings). You can use `.cast()` inside a `.withColumn()` to change a column's data type.

```python
from pyspark.sql.types import IntegerType

# Cast 'ticket_price' from Double to Integer (which removes the decimal)
df_int_price = df.withColumn("ticket_price", col("ticket_price").cast(IntegerType()))
df_int_price.show()
```

## 4. Grouping and Aggregating (`groupBy` and `agg`)

Just like in SQL or Pandas, you often need to aggregate data to find totals, averages, or counts. In PySpark, we chain a `.groupBy()` with an `.agg()` function.

First, you need to import the aggregation functions you want to use.

```python
from pyspark.sql.functions import sum, avg, count

# Example: How many flights and the average price per departure code?
flight_summary = df.groupBy("departure_code").agg(
    count("record_id").alias("total_flights"),
    avg("ticket_price").alias("average_price")
)
flight_summary.show()
```
*Note: `.alias()` is a handy way to instantly rename the resulting aggregated column so it doesn't look like `count(record_id)` in your final output.*