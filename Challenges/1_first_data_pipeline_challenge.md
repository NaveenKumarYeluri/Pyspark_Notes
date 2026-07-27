## 4. Challenge 3: Your First Data Pipeline

Using the `mock_data` and `flight_schema`, write a single chained PySpark command (similar to the chaining example) that does the following:

1. Filters the DataFrame to only show domestic flights (where `is_international` is `False`).
2. Selects only the `record_id` and `departure_code` columns.
3. Executes the transformation to display the results on the screen.

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
      StructType,
      StructField,
      LongType,
      StringType,
      BooleanType,
      DoubleType,
)
from pyspark.sql.functions import col

flight_schema = StructType(
    [
        StructField("record_id", LongType(), True),
        StructField("departure_code", StringType(), True),
        StructField("ticket_price", DoubleType(), True),
        StructField("is_international", BooleanType(), True),
    ]
)

mock_data = [
    (84729481, "LAX", 450.75, False),
    (84729482, "JFK", 1250.00, True),
    (84729483, "ORD", 299.99, False),
    (84729484, "LHR", 890.50, True),
]

spark = SparkSession.builder.appName("Challenges").master("local[*]").getOrCreate()
# Prints appName
app_name = spark.sparkContext.appName
print(f"Application Name: {app_name}")
df = spark.createDataFrame(data=mock_data, schema=flight_schema)
df.show()
modified_df = df.filter(col("is_international")==False).select(col("record_id"), col("departure_code"))
modified_df.show()
```

### My Output Verification:

```
Application Name: Challenges
+---------+--------------+------------+----------------+
|record_id|departure_code|ticket_price|is_international|
+---------+--------------+------------+----------------+
| 84729481|           LAX|      450.75|           false|
| 84729482|           JFK|      1250.0|            true|
| 84729483|           ORD|      299.99|           false|
| 84729484|           LHR|       890.5|            true|
+---------+--------------+------------+----------------+

+---------+--------------+
|record_id|departure_code|
+---------+--------------+
| 84729481|           LAX|
| 84729483|           ORD|
+---------+--------------+
```
