# PySpark Challenge 14: The Threat Intelligence Pipeline

**The Scenario:**

You are a Data Engineer for a cybersecurity team. You receive a daily feed of raw, unformatted network logs and a separate table containing known malicious IP addresses. 

Your manager needs a **Threat Analysis Report** that extracts actionable data from the messy logs, flags known threats, and ranks the most targeted servers.

**The Setup:**

Copy this code into your environment to generate the mock DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, udf, split, explode, sum, desc, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Challenge_14").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Raw Network Logs
log_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("server_name", StringType(), True),
    StructField("raw_log_payload", StringType(), True)
])

log_data = [
    (1, "Server_Alpha", "SRC: 192.168.1.50 | STATUS: FAIL | PKT_SIZE: 1042"),
    (2, "Server_Beta", "SRC: 10.0.0.99 | STATUS: SUCCESS | PKT_SIZE: 250"),
    (3, "Server_Alpha", "SRC: 45.33.10.2 | STATUS: FAIL | PKT_SIZE: 9999"),
    (4, "Server_Gamma", "SRC: 192.168.1.50 | STATUS: FAIL | PKT_SIZE: 88"),
    (5, "Server_Alpha", "SRC: 10.0.0.99 | STATUS: SUCCESS | PKT_SIZE: 120"),
    (6, "Server_Beta", "SRC: 45.33.10.2 | STATUS: FAIL | PKT_SIZE: 8500")
]
log_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Threat Intelligence Feed
threat_schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("threat_level", StringType(), True),
    StructField("risk_score", IntegerType(), True)
])

threat_data = [
    ("45.33.10.2", "CRITICAL", 100),
    ("192.168.1.50", "ELEVATED", 25)
]
threat_df = spark.createDataFrame(data=threat_data, schema=threat_schema)
```

## Challenge 14 Task:

Write a single, chained PySpark pipeline to generate the final `threat_report_df`.

**Requirements:**

1. **UDF Extraction:** Write a standard Python UDF that parses the `raw_log_payload` string and returns *only* the IP address. Apply this UDF to create a new column called `source_ip`. 
2. **Data Enrichment:** Join the parsed log data with the `threat_df`. If an IP address does not exist in the threat feed, you must retain the log event and fill the missing `threat_level` with "SAFE" and `risk_score` with `0`.
3. **Complex Aggregation:** Group the data by `server_name`. Calculate the total sum of `risk_score` for each server (aliased as `total_server_risk`).
4. **Ranking (Window Function):** Add a column called `risk_rank` that ranks the servers based on their `total_server_risk` in descending order.
5. **Output Format:** Order the final DataFrame by `risk_rank` ascending.

**Expected Output (`threat_report_df.show()`):**

```text
+------------+-----------------+---------+
| server_name|total_server_risk|risk_rank|
+------------+-----------------+---------+
|Server_Alpha|              100|        1|
| Server_Beta|              100|        1|
|Server_Gamma|               25|        3|
+------------+-----------------+---------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, udf, split, explode, sum, desc, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Challenge_14").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Raw Network Logs
log_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("server_name", StringType(), True),
    StructField("raw_log_payload", StringType(), True)
])

log_data = [
    (1, "Server_Alpha", "SRC: 192.168.1.50 | STATUS: FAIL | PKT_SIZE: 1042"),
    (2, "Server_Beta", "SRC: 10.0.0.99 | STATUS: SUCCESS | PKT_SIZE: 250"),
    (3, "Server_Alpha", "SRC: 45.33.10.2 | STATUS: FAIL | PKT_SIZE: 9999"),
    (4, "Server_Gamma", "SRC: 192.168.1.50 | STATUS: FAIL | PKT_SIZE: 88"),
    (5, "Server_Alpha", "SRC: 10.0.0.99 | STATUS: SUCCESS | PKT_SIZE: 120"),
    (6, "Server_Beta", "SRC: 45.33.10.2 | STATUS: FAIL | PKT_SIZE: 8500")
]
log_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Threat Intelligence Feed
threat_schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("threat_level", StringType(), True),
    StructField("risk_score", IntegerType(), True)
])

threat_data = [
    ("45.33.10.2", "CRITICAL", 100),
    ("192.168.1.50", "ELEVATED", 25)
]
threat_df = spark.createDataFrame(data=threat_data, schema=threat_schema)

def get_ip(raw_log):

    if raw_log:
        return raw_log.split(" ")[1]
    return None

source_ip_udf = udf(get_ip, StringType())

risk_window_rank = Window.orderBy(col("total_server_risk").desc())

threat_report_df = (
    log_df
    .withColumn("source_ip", source_ip_udf(col("raw_log_payload")))
    .drop("raw_log_payload")
    .join(
        threat_df
        , col("source_ip") == col("ip_address")
        , how="left"
    )
    .na.fill(
        {
            "threat_level": "SAFE"
            , "risk_score": 0
        }
    )
    .groupBy("server_name")
    .agg(sum("risk_score").alias("total_server_risk"))
    .withColumn("risk_rank", rank().over(risk_window_rank))
    .orderBy(col("risk_rank").asc())
)
print("Threat Report:")
threat_report_df.show()
```

### My Output Verification:

```
+------------+-----------------+---------+
| server_name|total_server_risk|risk_rank|
+------------+-----------------+---------+
|Server_Alpha|              125|        1|
| Server_Beta|              100|        2|
|Server_Gamma|               25|        3|
+------------+-----------------+---------+
```
