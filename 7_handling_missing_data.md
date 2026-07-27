# PySpark Learning Log: Part 5 - Handling Missing Data

In real-world data engineering, data is rarely perfectly clean. You will constantly encounter missing values (represented as `null` in PySpark). PySpark provides a dedicated `.na` (Not Available) module attached to DataFrames to handle these effectively.

## 1. Finding Null Values (`isNull` and `isNotNull`)

To filter your data based on whether a column is empty or not, you use the `.isNull()` and `.isNotNull()` methods directly on the column object.

```python
from pyspark.sql.functions import col

# Find all flights where the departure code is missing
missing_codes_df = df.filter(col("departure_code").isNull())
missing_codes_df.show()

# Find all valid flights (where we have a departure code)
valid_flights_df = df.filter(col("departure_code").isNotNull())
valid_flights_df.show()
```

## 2. Dropping Missing Data (`na.drop`)

Sometimes, if a row is missing critical information, you just want to throw the entire row away. You use `df.na.drop()` (which is also aliased as `df.dropna()`).

```python
# 1. Drop ANY row that contains a null value in ANY column (Aggressive)
clean_df = df.na.drop() 

# 2. Drop rows ONLY if a specific column is null (Targeted and recommended)
target_drop_df = df.na.drop(subset=["record_id", "departure_code"])
```

## 3. Filling Missing Data (`na.fill`)

Instead of dropping rows, it is often better to replace the missing values with a default value (like `0` for numbers or `"Unknown"` for text). You use `df.na.fill()` (also aliased as `df.fillna()`).

```python
# 1. Fill ALL null numeric columns with 0
filled_numbers_df = df.na.fill(0)

# 2. Fill ALL null string columns with "Unknown"
filled_strings_df = df.na.fill("Unknown")

# 3. Fill specific columns using a Python Dictionary (Best Practice)
# This allows you to set different defaults for different data types at the same time!
smart_fill_df = df.na.fill({
    "departure_code": "UNKNOWN_PORT",
    "ticket_price": 0.0,
    "is_international": False
})
```
