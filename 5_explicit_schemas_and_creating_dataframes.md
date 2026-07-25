# PySpark Learning Log: Part 3 - Explicit Schemas and Creating DataFrames

## 1. StructType and StructField

In the previous challenge, you identified the correct data types for a dataset. To apply these types in PySpark, we use two structural tools:

* `StructField`: Defines a single column (Name, Data Type, and whether it can contain Null/Empty values).
* `StructType`: A collection (list) of `StructField`s that defines the entire schema for a DataFrame.

Let's translate your answer from Challenge 2 into actual PySpark code.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, BooleanType

# Define the schema explicitly
flight_schema = StructType([
    StructField("record_id", LongType(), True),
    StructField("departure_code", StringType(), True),
    StructField("ticket_price", DoubleType(), True),
    StructField("is_international", BooleanType(), True)
])
```

* Note: The `True` at the end of each StructField means the column is nullable (it is allowed to have missing data).*

## 2. Creating a DataFrame from Scratch

When learning, testing, or building mock data for unit tests, you will often want to create DataFrames manually. We do this by creating a standard Python list of tuples (where each tuple is a row) and feeding it into `spark.createDataFrame()`.

```python
# 1. Create the raw data using standard Python lists and tuples
mock_data = [
    (84729481, "LAX", 450.75, False),
    (84729482, "JFK", 1250.00, True),
    (84729483, "ORD", 299.99, False),
    (84729484, "LHR", 890.50, True)
]

# 2. Combine the data and the schema we built earlier
df = spark.createDataFrame(data=mock_data, schema=flight_schema)

# 3. Action: View the DataFrame and its Schema
df.printSchema()
df.show()
```

## 3. Basic Transformations: Select and Filter

Now that we have a DataFrame, let's look at the two most fundamental **Transformations**.

To work with columns dynamically, it is highly recommended to import the `col` function. It allows you to reference columns safely as objects rather than just strings.

```python
from pyspark.sql.functions import col

# --- SELECT ---
# Used to pick specific columns, just like SQL.
selected_df = df.select(col("departure_code"), col("ticket_price"))
selected_df.show()

# --- FILTER ---
# Used to restrict rows based on a condition (you can also use .where(), they are identical)
expensive_flights_df = df.filter(col("ticket_price") > 500)
expensive_flights_df.show()

# --- CHAINING ---
# Because transformations are lazy, you can chain them together elegantly:
final_df = df.select(col("departure_code"), col("is_international")) \
             .filter(col("is_international") == True)
             
final_df.show()
```
