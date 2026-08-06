# PySpark Challenge 15: The API Log ETL Pipeline (Boss Level)

**The Scenario:**

You are building an ETL pipeline to process raw API logs stored in S3 before loading them into a Redshift dimensional model. The upstream application dumps the event metadata as a stringified JSON payload in a single column. 

Your stakeholders need a consolidated **Security Audit Report** that identifies the most recent high-risk action performed by each user, joined with their official department profile.

**The Setup:**

Copy this code into your environment to generate the mock DataFrames. The schema for the JSON payload is provided for you so you can focus on the pipeline logic.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json, to_timestamp, rank, when, upper
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder.appName("Challenge_15").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Raw API Logs (JSON payloads stored as strings)
log_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("json_payload", StringType(), True)
])

log_data = [
    ("E01", "2026-05-01 08:15:00", '{"user": {"uid": "U101", "ip": "192.168.1.5"}, "action": "AssumeRole", "status": 200}'),
    ("E02", "2026-05-01 08:20:00", '{"user": {"uid": "U101", "ip": "192.168.1.5"}, "action": "TerminateInstances", "status": 403}'),
    ("E03", "2026-05-01 09:00:00", '{"user": {"uid": "U205", "ip": "10.0.0.12"}, "action": "ListBuckets", "status": 200}'),
    ("E04", "2026-05-01 10:30:00", '{"user": {"uid": "U101", "ip": "45.22.11.9"}, "action": "DeleteTable", "status": 403}'),
    ("E05", "2026-05-01 11:15:00", '{"user": {"uid": "U300", "ip": "172.16.0.1"}, "action": "DropDatabase", "status": 500}'),
    ("E06", "2026-05-01 11:20:00", '{"user": {"uid": "U300", "ip": "172.16.0.1"}, "action": "QueryData", "status": 200}')
]
logs_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Provided JSON Schema for Parsing
payload_schema = StructType([
    StructField("user", StructType([
        StructField("uid", StringType(), True),
        StructField("ip", StringType(), True)
    ]), True),
    StructField("action", StringType(), True),
    StructField("status", IntegerType(), True)
])

# 3. User Department Lookup Table
dept_schema = StructType([
    StructField("uid", StringType(), True),
    StructField("department", StringType(), True)
])

dept_data = [
    ("U101", "Data Engineering"),
    ("U205", "Marketing"),
    ("U300", "Data Science")
]
dept_df = spark.createDataFrame(data=dept_data, schema=dept_schema)
```

## Challenge 15 Task:

Write a single, chained PySpark pipeline to create `security_audit_df`.

**Requirements:**

1. Parse the stringified `json_payload` using the provided `payload_schema`.
2. Extract the nested `uid`, `ip`, `action`, and `status` fields into their own top-level columns.
3. Filter the dataset to **only** include "High Risk" events. An event is considered High Risk if the `status` is strictly greater than `200` OR the `action` is exactly `"AssumeRole"`.
4. Join the filtered data with `dept_df` to bring in the `department` name. 
5. Convert `event_timestamp` to a proper PySpark Timestamp type.
6. Using a Window function, isolate only the **most recent** High Risk event for each user (based on the timestamp).
7. Format the output: Drop the raw payload and intermediate ranking columns, standardize the `department` column to uppercase, and order the final DataFrame by the timestamp descending.

**Expected Output (`security_audit_df.show(truncate=False)`):**

```text
+--------+-------------------+----+----------+------------+------+----------------+
|event_id|event_timestamp    |uid |ip        |action      |status|department      |
+--------+-------------------+----+----------+------------+------+----------------+
|E05     |2026-05-01 11:15:00|U300|172.16.0.1|DropDatabase|500   |DATA SCIENCE    |
|E04     |2026-05-01 10:30:00|U101|45.22.11.9|DeleteTable |403   |DATA ENGINEERING|
+--------+-------------------+----+----------+------------+------+----------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json, to_timestamp, when, upper, row_number
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder.appName("Challenge_13").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Raw API Logs (JSON payloads stored as strings)
log_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("json_payload", StringType(), True)
])

log_data = [
    ("E01", "2026-05-01 08:15:00", '{"user": {"uid": "U101", "ip": "192.168.1.5"}, "action": "AssumeRole", "status": 200}'),
    ("E02", "2026-05-01 08:20:00", '{"user": {"uid": "U101", "ip": "192.168.1.5"}, "action": "TerminateInstances", "status": 403}'),
    ("E03", "2026-05-01 09:00:00", '{"user": {"uid": "U205", "ip": "10.0.0.12"}, "action": "ListBuckets", "status": 200}'),
    ("E04", "2026-05-01 10:30:00", '{"user": {"uid": "U101", "ip": "45.22.11.9"}, "action": "DeleteTable", "status": 403}'),
    ("E05", "2026-05-01 11:15:00", '{"user": {"uid": "U300", "ip": "172.16.0.1"}, "action": "DropDatabase", "status": 500}'),
    ("E06", "2026-05-01 11:20:00", '{"user": {"uid": "U300", "ip": "172.16.0.1"}, "action": "QueryData", "status": 200}')
]
logs_df = spark.createDataFrame(data=log_data, schema=log_schema)

# 2. Provided JSON Schema for Parsing
payload_schema = StructType([
    StructField("user", StructType([
        StructField("uid", StringType(), True),
        StructField("ip", StringType(), True)
    ]), True),
    StructField("action", StringType(), True),
    StructField("status", IntegerType(), True)
])

# 3. User Department Lookup Table
dept_schema = StructType([
    StructField("uid", StringType(), True),
    StructField("department", StringType(), True)
])

dept_data = [
    ("U101", "Data Engineering"),
    ("U205", "Marketing"),
    ("U300", "Data Science")
]
dept_df = spark.createDataFrame(data=dept_data, schema=dept_schema)

win_isolation = Window.partitionBy("uid").orderBy(col("event_timestamp").desc())

security_audit_df = (
    logs_df
    .withColumn("parsed_data", from_json(col("json_payload"), payload_schema))
    .select("event_id", "event_timestamp", "parsed_data.user.uid", "parsed_data.user.ip", "parsed_data.action", "parsed_data.status")
    .filter((col("status") > 200) | (col("action") == "AssumeRole"))
    .join(dept_df, on="uid", how="inner")
    .select("event_id", "event_timestamp", "uid", "ip", "action", "status", "department")
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    .withColumn("user_events_rank", row_number().over(win_isolation))
    .filter(col("user_events_rank") == 1)
    .withColumn("department", upper(col("department")))
    .select("event_id", "event_timestamp", "uid", "ip", "action", "status", "department")
    .orderBy(col("event_timestamp").desc())
)
security_audit_df.show(truncate=False)
```

### My Output Verification:

```
+--------+-------------------+----+----------+------------+------+----------------+
|event_id|event_timestamp    |uid |ip        |action      |status|department      |
+--------+-------------------+----+----------+------------+------+----------------+
|E05     |2026-05-01 11:15:00|U300|172.16.0.1|DropDatabase|500   |DATA SCIENCE    |
|E04     |2026-05-01 10:30:00|U101|45.22.11.9|DeleteTable |403   |DATA ENGINEERING|
+--------+-------------------+----+----------+------------+------+----------------+
```
