# PySpark Learning Log: Part 2 - Syntax, Variables, and Data Types

## 1. The Anatomy of a PySpark Script

Every PySpark script generally follows a standard, four-step lifecycle. Whether you are running a quick test locally or deploying a massive data ingestion pipeline to the cloud, the structure remains the same.

1.  **Initialization:** Create the `SparkSession`.
2.  **Ingestion (Read):** Load data into a DataFrame from a source (CSV, JSON, a database, or AWS S3).
3.  **Transformation:** Apply logic to modify the data (filter rows, cast data types, create new columns). *Remember: These are lazy!*
4.  **Action (Write/Output):** Execute the plan by displaying the data (`show()`) or writing it to a destination (`write()`).

---

## 2. Variables in PySpark vs. Native Python

In standard Python, a variable usually holds a single value or a basic list in memory. 

```python
# Standard Python variables
flight_number = 1042
is_delayed = True
```

In PySpark, you will often use Python variables to store **DataFrames** or **Column objects**, rather than single values. A PySpark variable represents an operation applied to millions of rows simultaneously across a cluster.

```python
from pyspark.sql.functions import col

# 'df' is a variable holding the entire DataFrame (Lazy)
df = spark.read.csv("flights.csv", header=True)

# 'delay_col' is a variable holding a Column object, not a single number
delay_col = col("delay_minutes")
```

---

## 3. PySpark Data Types

When building data models, you must define the exact data types for your columns. Unlike Pandas, which uses NumPy data types under the hood, PySpark has its own specific types defined in the `pyspark.sql.types` module. 

Here are the most common ones you will use:

| PySpark Type | Description | Example Data |
| :--- | :--- | :--- |
| `StringType()` | Text and characters | `"Delta"`, `"JFK"` |
| `IntegerType()` | Whole numbers (32-bit) | `1042`, `-5` |
| `LongType()` | Large whole numbers (64-bit) | `9876543210` |
| `DoubleType()` | Floating-point numbers (decimals) | `199.99`, `3.1415` |
| `BooleanType()` | True or False logic | `True`, `False` |
| `TimestampType()` | Date and exact time | `2026-07-13 19:47:00` |

To use these, you have to import them at the top of your script:

```python
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
```

---

## 4. Challenge 2: Schema Design

You are building an ingestion pipeline and need to define the schema for an incoming dataset. 

**The Data Sample:**

* `record_id`: 84729481 (A standard sequential ID)
* `departure_code`: "LAX"
* `ticket_price`: 450.75
* `is_international`: False

**Your Task:**

Write out the four specific PySpark Data Types (from the table above) that you would assign to these four columns, in order.

* `record_id`: LongType()
* `departure_code`: StringType()
* `ticket_price`: DoubleType()
* `is_international`: BooleanType()
