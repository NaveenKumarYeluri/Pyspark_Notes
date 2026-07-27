# PySpark Challenge 5: Handling Missing Data

**The Setup:**

Copy the following code into your notebook. This will create a new mock DataFrame that has some intentional missing data (`None` in Python translates to `null` in PySpark).

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, BooleanType

# Schema
flight_schema = StructType([
    StructField("record_id", LongType(), True),
    StructField("departure_code", StringType(), True),
    StructField("ticket_price", DoubleType(), True),
    StructField("is_international", BooleanType(), True)
])

# Messy Data with Nulls (None)
messy_data = [
    (84729481, "LAX", 450.75, False),
    (84729482, None, 1250.00, True),     # Missing departure code
    (None, "ORD", None, False),          # Missing ID and price
    (84729484, "LHR", 890.50, None)      # Missing international flag
]

messy_df = spark.createDataFrame(data=messy_data, schema=flight_schema)
messy_df.show()
```

---

## Challenge 5a: The Easy Filter

**Task:**

Write a single PySpark command to filter `messy_df` so it ONLY shows rows where the `ticket_price` is currently `null`. 
*(Hint: Use `filter` and `isNull()`)*

## Challenge 5b: Drop and Fill (Medium)

**Task:**

Write a single chained PySpark command that does the following in order:

1. Drops any rows where the `record_id` is missing (using `na.drop` and the `subset` parameter).
2. Fills any remaining missing text in the `departure_code` column with the string `"TBD"`.
3. Shows the resulting DataFrame.

## Challenge 5c: The Dictionary Fill (Complex)

**Task:**

Write a single chained PySpark command that does the following:

1. Uses a Python dictionary to perform a multi-column fill via `na.fill()`.
   * Fill null `ticket_price` values with `0.0`.
   * Fill null `is_international` values with `False`.
2. Selects only the `departure_code` and `ticket_price` columns.
3. Shows the resulting DataFrame.

### My Solution:

```python
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType, DoubleType

spark = SparkSession.builder.appName("challenge_3").master("local[*]").getOrCreate()

# Schema
flight_schema = StructType([
    StructField("record_id", LongType(), True),
    StructField("departure_code", StringType(), True),
    StructField("ticket_price", DoubleType(), True),
    StructField("is_international", BooleanType(), True)
])

# Messy Data with Nulls (None)
messy_data = [
    (84729481, "LAX", 450.75, False),
    (84729482, None, 1250.00, True),     # Missing departure code
    (None, "ORD", None, False),          # Missing ID and price
    (84729484, "LHR", 890.50, None)      # Missing international flag
]

messy_df = spark.createDataFrame(data=messy_data, schema=flight_schema)
messy_df.show()

chall_5a = messy_df.filter(col("ticket_price").isNull())
chall_5a.show()

chall_5b = (
    messy_df
    .na.drop(subset=["record_id"])
    .na.fill({"departure_code":"TBD"})
)
chall_5b.show()

chall_5c = (
    messy_df
    .na.fill({
        "ticket_price":0.0,
        "is_international":False,
    }).select(col("departure_code"), col("ticket_price"))
)
chall_5c.show()
```

### My Output Verification:

```
+---------+--------------+------------+----------------+
|record_id|departure_code|ticket_price|is_international|
+---------+--------------+------------+----------------+
| 84729481|           LAX|      450.75|           false|
| 84729482|          NULL|      1250.0|            true|
|     NULL|           ORD|        NULL|           false|
| 84729484|           LHR|       890.5|            NULL|
+---------+--------------+------------+----------------+

+---------+--------------+------------+----------------+
|record_id|departure_code|ticket_price|is_international|
+---------+--------------+------------+----------------+
|     NULL|           ORD|        NULL|           false|
+---------+--------------+------------+----------------+

+---------+--------------+------------+----------------+
|record_id|departure_code|ticket_price|is_international|
+---------+--------------+------------+----------------+
| 84729481|           LAX|      450.75|           false|
| 84729482|           TBD|      1250.0|            true|
| 84729484|           LHR|       890.5|            NULL|
+---------+--------------+------------+----------------+

+--------------+------------+
|departure_code|ticket_price|
+--------------+------------+
|           LAX|      450.75|
|          NULL|      1250.0|
|           ORD|         0.0|
|           LHR|       890.5|
+--------------+------------+
```
