# PySpark Learning Log: Part 16 - Higher-Order Functions (Advanced Arrays)

In Part 12, we learned how to use `explode()` to break arrays into separate rows. While `explode()` is useful, it multiplies the size of your dataset, which can cause severe performance bottlenecks (Out of Memory errors) on massive datasets.

Introduced in newer versions of PySpark, **Higher-Order Functions** allow you to iterate over arrays and manipulate their contents *while keeping them in a single row*.

## 1. `transform` (Modify every element)

The `transform()` function loops through an array and applies an expression to every single element (similar to Python's `map()` or list comprehensions). You use a lambda function `x -> expression` to represent the element being processed.

```python
from pyspark.sql.functions import col, transform

# Scenario: We have an array of prices, and we want to add $5 to each price.
# 'x' represents the individual element inside the array during the loop.
df_tax = df.withColumn(
    "adjusted_prices", 
    transform(col("price_array"), lambda x: x + 5)
)
```

## 2. `filter` (Keep specific elements)

The `filter()` function loops through an array and keeps only the elements that evaluate to `True` based on a condition you define.

```python
from pyspark.sql.functions import filter

# Scenario: We have an array of ages, and we want to remove anyone under 18.
df_adults = df.withColumn(
    "adult_ages_only",
    filter(col("age_array"), lambda x: x >= 18)
)
```

## 3. `aggregate` (Reduce an array to a single value)

The `aggregate()` function loops through an array and keeps a running "accumulator" to reduce the entire array down to a single value (like calculating a sum or a custom mathematical product).

It takes three arguments:

1. The array column.
2. The initial starting value of the accumulator.
3. The lambda function defining how to merge the element into the accumulator.

```python
from pyspark.sql.functions import aggregate, lit

# Scenario: Summing all the numbers in an array.
# 'acc' is the running total, 'x' is the current element. We start 'acc' at 0.
df_sum = df.withColumn(
    "total_sum",
    aggregate(col("number_array"), lit(0), lambda acc, x: acc + x)
)
```

By utilizing `transform`, `filter`, and `aggregate`, you can perform extremely complex mathematical operations on arrays without ever triggering an expensive `explode()` shuffle!
