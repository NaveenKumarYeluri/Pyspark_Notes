# PySpark Learning Log: Part 12 - Complex Data Types (Arrays & Explode)

In traditional relational databases (SQL), every cell holds a single value. In Big Data, however, we frequently deal with JSON or unstructured logs where a single column might contain an entire list (array) of values. 

PySpark has a specific set of functions to handle `ArrayType` columns.

## 1. Creating Arrays (`split`)

Often, you will receive data as a single comma-separated string (e.g., `"apple,banana,orange"`). You can convert this string into a true PySpark Array using the `split()` function.

```python
from pyspark.sql.functions import col, split

# Converts a string like "Python,SQL,Spark" into an array: ["Python", "SQL", "Spark"]
df_arrays = df.withColumn("skills_array", split(col("skills_string"), ","))
```

## 2. Array Utility Functions (`size`, `array_contains`)

Once your column is an Array, you can use built-in collection functions to analyze the list without breaking it apart.

*   `size()`: Returns the number of elements in the array.
*   `array_contains()`: Checks if a specific value exists inside the array (returns True/False).

```python
from pyspark.sql.functions import size, array_contains

# 1. Count how many skills the user has
df_size = df_arrays.withColumn("skill_count", size(col("skills_array")))

# 2. Flag users who know "Spark"
df_spark = df_arrays.withColumn("knows_spark", array_contains(col("skills_array"), "Spark"))
```

## 3. The Magic of `explode`

This is arguably one of the most important functions in PySpark. 

If you have a row with an array of 3 items, and you want to analyze those items individually, you need to flatten the array. `explode()` takes an array column and **creates a new row for every element in the array**, copying all the other column data alongside it.

```python
from pyspark.sql.functions import explode

# Original Row: | emp_id: 1 | skills: ["Python", "SQL"] |

# Explode the array
exploded_df = df_arrays.withColumn("single_skill", explode(col("skills_array")))

# New DataFrame:
# | emp_id: 1 | skills: ["Python", "SQL"] | single_skill: "Python" |
# | emp_id: 1 | skills: ["Python", "SQL"] | single_skill: "SQL"    |
```
*Notice how `emp_id` 1 was duplicated so that each skill gets its own dedicated row! This allows you to easily `groupBy` or `join` on the individual items.*
