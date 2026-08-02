# PySpark Learning Log: Part 11 - String and Date Functions

So far, we have done a lot of math and conditional checks. However, in the real world, much of a Data Engineer's job involves parsing messy text and standardizing timestamps. PySpark has a massive library of built-in functions just for this.

## 1. String Functions

PySpark's `pyspark.sql.functions` module includes dozens of methods to clean and slice text. 

*   `upper()` / `lower()`: Changes case.
*   `trim()`: Removes leading and trailing whitespace.
*   `substring()`: Extracts a portion of a string.
*   `concat()` / `concat_ws()`: Joins strings together.

```python
from pyspark.sql.functions import col, upper, trim, substring, concat_ws

# 1. Clean messy text (uppercase and remove spaces)
clean_df = df.withColumn("clean_name", upper(trim(col("messy_name"))))

# 2. Extract the first 3 letters of a code
# Note: substring takes the column, the starting position (1-based), and the length.
code_df = df.withColumn("short_code", substring(col("full_code"), 1, 3))

# 3. Join columns together with a separator (e.g., "City, State")
location_df = df.withColumn("location", concat_ws(", ", col("city"), col("state")))
```

## 2. Date and Timestamp Functions

Working with time is notoriously tricky. PySpark provides robust functions to extract parts of a date or format it exactly how you need it.

*   `current_date()`: Returns the current date.
*   `year()`, `month()`, `dayofmonth()`: Extracts the specific integer component from a date column.
*   `date_format()`: Converts a date to a specific string format (e.g., "MM-dd-yyyy").
*   `datediff()`: Calculates the number of days between two dates.

```python
from pyspark.sql.functions import col, current_date, year, month, datediff

# 1. Extract the year from a transaction date
year_df = df.withColumn("transaction_year", year(col("transaction_date")))

# 2. Calculate how many days an invoice is overdue
# datediff(end_date, start_date)
overdue_df = df.withColumn(
    "days_overdue", 
    datediff(current_date(), col("due_date"))
)
```

## 3. Combining Concepts: The Real Power

The true power of PySpark is chaining these functions together with logic, groupings, and filters you already know. 

For example, you could extract the `month()` from a date, filter out specific months, and then `groupBy` the month to find the `sum` of sales!
