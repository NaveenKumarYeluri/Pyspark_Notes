# PySpark Challenge 20: The Skewed Network Attack (Boss Level)

**The Scenario:**

You are processing massive network traffic logs. The dataset is heavily skewed because an attacker IP (`10.0.0.99`) is flooding the network. 

You need to join these logs with an ISP dimension table to calculate the total bandwidth consumed per ISP over time, as well as a running cumulative total. Because of the skew, a standard join will crash the cluster. You **must** implement a Salting strategy.

**The Setup:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date
from pyspark.sql.functions import col, lit, rand, round, concat, array, explode, sequence, sum, year, month
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Challenge_20").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Skewed Fact Table (Logs)
log_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("bandwidth_mb", IntegerType(), True),
    StructField("event_date", DateType(), True)
])

log_data = [
    ("L1", "192.168.1.1", 50, date(2026, 1, 15)),
    ("L2", "192.168.1.2", 120, date(2026, 1, 16)),
    # SKEW: The 10.0.0.99 IP has massive volume
    ("L3", "10.0.0.99", 5000, date(2026, 1, 15)),
    ("L4", "10.0.0.99", 6000, date(2026, 1, 15)),
    ("L5", "10.0.0.99", 8000, date(2026, 2, 10)),
    ("L6", "10.0.0.99", 7500, date(2026, 2, 12))
]
logs_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Dimension Table (ISP Lookup)
isp_schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("isp_name", StringType(), True)
])

isp_data = [
    ("192.168.1.1", "Comcast"),
    ("192.168.1.2", "Verizon"),
    ("10.0.0.99", "AWS_Cloud")
]
isp_df = spark.createDataFrame(data=isp_data, schema=isp_schema)
```

## Challenge 20 Task:

Write a PySpark pipeline to generate the `isp_bandwidth_report_df`. 

**Business Goals & Expected Output:**

1. Implement a Salting strategy (using a salt range of `0` to `3`) to safely join `logs_df` and `isp_df` without hitting OOM errors.
2. The final output must group the data to show the total `bandwidth_mb` per `isp_name`, per `year`, and per `month` (extracted from the `event_date`).
3. The final output must include a `cumulative_bandwidth` column that calculates the running total of bandwidth for each ISP over time (chronologically by year and month).
4. The final DataFrame must be ordered by `isp_name` (ascending), `year` (ascending), and `month` (ascending).

**Expected Output (`isp_bandwidth_report_df.show()`):**

```text
+---------+----+-----+-----------------+--------------------+
| isp_name|year|month|monthly_bandwidth|cumulative_bandwidth|
+---------+----+-----+-----------------+--------------------+
|AWS_Cloud|2026|    1|            11000|               11000|
|AWS_Cloud|2026|    2|            15500|               26500|
|  Comcast|2026|    1|               50|                  50|
|  Verizon|2026|    1|              120|                 120|
+---------+----+-----+-----------------+--------------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date
from pyspark.sql.functions import col, lit, rand, round, concat, array, explode, sequence, sum, year, month
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Challenge_18").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Skewed Fact Table (Logs)
log_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("bandwidth_mb", IntegerType(), True),
    StructField("event_date", DateType(), True)
])

log_data = [
    ("L1", "192.168.1.1", 50, date(2026, 1, 15)),
    ("L2", "192.168.1.2", 120, date(2026, 1, 16)),
    # SKEW: The 10.0.0.99 IP has massive volume
    ("L3", "10.0.0.99", 5000, date(2026, 1, 15)),
    ("L4", "10.0.0.99", 6000, date(2026, 1, 15)),
    ("L5", "10.0.0.99", 8000, date(2026, 2, 10)),
    ("L6", "10.0.0.99", 7500, date(2026, 2, 12))
]
logs_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Dimension Table (ISP Lookup)
isp_schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("isp_name", StringType(), True)
])

isp_data = [
    ("192.168.1.1", "Comcast"),
    ("192.168.1.2", "Verizon"),
    ("10.0.0.99", "AWS_Cloud")
]
isp_df = spark.createDataFrame(data=isp_data, schema=isp_schema)

running_total_win = (
    Window
    .partitionBy("isp_name")
    .orderBy("year", "month")
)

isp_bandwidth_report_df = (
    logs_df
    .withColumn("salted_key", concat(col("ip_address"), lit("_"), round(rand() * 3).cast("int")))
    .join(isp_df
        .withColumn("salt_array", sequence(lit(0), lit(3)))
        .withColumn("salt_val", explode(col("salt_array")))
        .withColumn("salted_key", concat(col("ip_address"), lit("_"), col("salt_val")))
        , on="salted_key"
        , how="inner"
    )
    .groupBy("isp_name", year(col("event_date")).alias("year"), month(col("event_date")).alias("month"))
    .agg(sum("bandwidth_mb").alias("monthly_bandwidth"))
    .withColumn("cumulative_bandwidth", sum("monthly_bandwidth").over(running_total_win))
    .orderBy(col("isp_name"), col("year"), col("month"))
)
isp_bandwidth_report_df.show()
```

### My Output Verification:

```
+---------+----+-----+-----------------+--------------------+
| isp_name|year|month|monthly_bandwidth|cumulative_bandwidth|
+---------+----+-----+-----------------+--------------------+
|AWS_Cloud|2026|    1|            11000|               11000|
|AWS_Cloud|2026|    2|            15500|               26500|
|  Comcast|2026|    1|               50|                  50|
|  Verizon|2026|    1|              120|                 120|
+---------+----+-----+-----------------+--------------------+
```
