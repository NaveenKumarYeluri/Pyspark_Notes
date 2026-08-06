# PySpark Learning Log: Part 15 - Nested Data and JSON Parsing

In real-world data engineering, data is rarely flat. When ingesting data from NoSQL databases, REST APIs, or cloud infrastructure logs, you will frequently encounter deeply nested structures and stringified JSON. PySpark handles this using `StructType`, `ArrayType`, and `MapType`, along with specialized JSON functions.

## 1. Accessing Nested Structs

When a column contains a nested `StructType` (similar to a JSON object/dictionary), you can access its internal fields using standard dot notation.

```python
# Assuming a column 'user_info' with nested fields 'address' -> 'city'
df.select(col("user_info.name"), col("user_info.address.city"))
```

## 2. Parsing Stringified JSON (`from_json`)

Often, upstream systems write JSON payloads as plain strings inside a single CSV or Parquet column. To query the data inside, you must parse the string into a PySpark Struct using `from_json()`.

To use `from_json()`, you *must* provide a PySpark schema that defines the structure of the JSON string.

```python
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Define the schema of the JSON string
json_schema = StructType([
    StructField("browser", StringType(), True),
    StructField("session_time", IntegerType(), True)
])

# 2. Parse the string column into a Struct column
parsed_df = df.withColumn("parsed_data", from_json(col("raw_json_string"), json_schema))

# 3. Extract the flattened fields
flat_df = parsed_df.select(
    col("id"),
    col("parsed_data.browser").alias("browser_type"),
    col("parsed_data.session_time")
)
```

## 3. Creating JSON Strings (`to_json`)

Conversely, if you need to export data to an API or a message queue (like Kafka) that expects a JSON payload, you can pack multiple PySpark columns into a single JSON string using `to_json()` and `struct()`.

```python
from pyspark.sql.functions import to_json, struct

# Pack 'name' and 'age' columns into a single JSON string column
json_out_df = df.withColumn("payload", to_json(struct(col("name"), col("age"))))
```