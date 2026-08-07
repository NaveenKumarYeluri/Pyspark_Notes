# PySpark Challenge 16: The IoT Sensor Array Pipeline (Boss Level)

**The Scenario:**

You are processing batched telemetry data from an IoT network. Devices transmit an array of temperature readings (in Fahrenheit) every hour.

Your goal is to clean out anomalous sensor errors, convert the valid readings to Celsius, calculate the average temperature per device, and join the results with the active facility metadata. All array operations must be done *in-place* using Higher-Order functions (no exploding allowed!).

**The Setup:**

Copy this code to generate the mock DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType
from datetime import date

spark = SparkSession.builder.appName("Challenge_16").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. IoT Sensor Logs (Temperatures in Fahrenheit)
log_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("batch_date", DateType(), True),
    StructField("temp_f_readings", ArrayType(IntegerType()), True)
])

log_data = [
    ("D_001", date(2026, 8, 1), [32, 50, 68, -99, 250]), # -99 and 250 are sensor errors
    ("D_002", date(2026, 8, 1), [77, 86, -10]),          # -10 is an error
    ("D_003", date(2026, 8, 2), [10, 20]),               # Valid, but device is retired
    ("D_004", date(2026, 8, 2), [-5, 300])               # All errors
]
logs_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Facility Metadata
meta_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("lifecycle", StringType(), True)
])

meta_data = [
    ("D_001", "Zone_A", "Active"),
    ("D_002", "Zone_A", "Active"),
    ("D_003", "Zone_B", "Retired"), 
    ("D_004", "Zone_B", "Active")
]
meta_df = spark.createDataFrame(data=meta_data, schema=meta_schema)
```

## Challenge 16 Task:

Write a single, chained PySpark pipeline to create `iot_report_df`.

**Requirements:**

1. Combine the `logs_df` with the `meta_df`.
2. Keep ONLY devices where the `lifecycle` is `"Active"`.
3. Create a new column `valid_f_array`. Use a Higher-Order function to keep ONLY temperatures that are `>= 0` AND `<= 120` from `temp_f_readings`.
4. Create a new column `valid_c_array`. Use a Higher-Order function to convert the elements inside `valid_f_array` from Fahrenheit to Celsius. *(Formula: `(F - 32) * 5 / 9`)*.
5. Create a new column `avg_temp_c`. Calculate this by finding the sum of `valid_c_array` (using a Higher-Order function) and dividing it by the size of the array (`size()`). Wrap the final calculation in `round()` to round it to one decimal place.
6. Select only the `device_id`, `facility`, `valid_c_array`, and `avg_temp_c` columns.
7. Order the final output by `device_id`.

**Expected Output (`iot_report_df.show(truncate=False)`):**

```text
+---------+--------+------------------+----------+
|device_id|facility|valid_c_array     |avg_temp_c|
+---------+--------+------------------+----------+
|D_001    |Zone_A  |[0.0, 10.0, 20.0] |10.0      |
|D_002    |Zone_A  |[25.0, 30.0]      |27.5      |
|D_004    |Zone_B  |[]                |NULL      |
+---------+--------+------------------+----------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType, DoubleType
from pyspark.sql.functions import col, filter, transform, aggregate, lit, size, round, try_divide
from datetime import date

spark = SparkSession.builder.appName("Challenge_16").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. IoT Sensor Logs (Temperatures in Fahrenheit)
log_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("batch_date", DateType(), True),
    StructField("temp_f_readings", ArrayType(IntegerType()), True)
])

log_data = [
    ("D_001", date(2026, 8, 1), [32, 50, 68, -99, 250]), # -99 and 250 are sensor errors
    ("D_002", date(2026, 8, 1), [77, 86, -10]),          # -10 is an error
    ("D_003", date(2026, 8, 2), [10, 20]),               # Valid, but device is retired
    ("D_004", date(2026, 8, 2), [-5, 300])               # All errors
]
logs_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Facility Metadata
meta_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("lifecycle", StringType(), True)
])

meta_data = [
    ("D_001", "Zone_A", "Active"),
    ("D_002", "Zone_A", "Active"),
    ("D_003", "Zone_B", "Retired"), 
    ("D_004", "Zone_B", "Active")
]
meta_df = spark.createDataFrame(data=meta_data, schema=meta_schema)

iot_report_df = (
    logs_df
    .join(meta_df, on="device_id", how="inner")
    .filter(col("lifecycle") == "Active")
    .withColumn("valid_f_array", filter(col("temp_f_readings"), lambda x: (x >= 0) & (x <= 120)))
    .withColumn("valid_c_array", transform(col("valid_f_array"), lambda x: ((x-32)*5/9)))
    .withColumn("avg_tmp", aggregate(col("valid_c_array"), lit(0.0), lambda acc, x: acc + x))
    .withColumn("avg_temp_c",
        round(
            try_divide(col("avg_tmp"), size(col("valid_c_array")))
        , 2)
    )
    .select("device_id", "facility", "valid_c_array", "avg_temp_c")
    .orderBy(col("device_id"))
)
iot_report_df.show(truncate=False)
```

### My Output Verification:

```
+---------+--------+-----------------+----------+
|device_id|facility|valid_c_array    |avg_temp_c|
+---------+--------+-----------------+----------+
|D_001    |Zone_A  |[0.0, 10.0, 20.0]|10.0      |
|D_002    |Zone_A  |[25.0, 30.0]     |27.5      |
|D_004    |Zone_B  |[]               |NULL      |
+---------+--------+-----------------+----------+
```
