# PySpark Challenge 21: The Global Logistics Classifier (Boss Level)

**The Scenario:**

You are a Data Engineer for a global freight company. You receive daily shipping logs that contain a free-text `driver_notes` column. 

The operations team needs a monthly report on "High Risk" shipments. Because the driver notes are highly unstructured and require a complex regex/text parsing library logic that the built-in PySpark functions can't handle elegantly, you must write a **Pandas UDF** to classify the text. 

**The Setup:**

Copy this code to generate the mock DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum, month, rank, upper
from pyspark.sql.window import Window
from datetime import date
import pandas as pd
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.appName("Challenge_21").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Logistics Fact Table
logistics_schema = StructType([
    StructField("tracking_id", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("weight_kg", IntegerType(), True),
    StructField("dispatch_date", DateType(), True),
    StructField("driver_notes", StringType(), True)
])

logistics_data = [
    ("TRK01", "R1", 500, date(2026, 1, 15), "Handle with care, extremely FRAGILE!"),
    ("TRK02", "R2", 1200, date(2026, 1, 20), "Standard delivery, leave at dock."),
    ("TRK03", "R1", 300, date(2026, 1, 25), "Urgent: Fragile glass components inside."),
    ("TRK04", "R3", 800, date(2026, 2, 5), "Heavy load. Use forklift."),
    ("TRK05", "R2", 150, date(2026, 2, 10), "fragile items, do not drop."),
    ("TRK06", "R1", 600, date(2026, 2, 18), "Routine pickup.")
]
logistics_df = spark.createDataFrame(data=logistics_data, schema=logistics_schema)

# 2. Region Dimension Table
region_schema = StructType([
    StructField("region_id", StringType(), True),
    StructField("region_name", StringType(), True)
])

region_data = [
    ("R1", "North America"),
    ("R2", "Europe"),
    ("R3", "Asia")
]
region_df = spark.createDataFrame(data=region_data, schema=region_schema)
```

## Challenge 21 Task:

Write a PySpark pipeline to generate the `high_risk_logistics_report`.

**Business Goals & Expected Output:**

1. **Pandas UDF Creation:** Create a `@pandas_udf` returning a `StringType`. It must take a `pd.Series` of text. If the text contains the word "fragile" (case-insensitive), it should return `"HIGH_RISK"`. Otherwise, it should return `"STANDARD"`.
2. **Apply & Filter:** Apply your UDF to the `driver_notes` column to create a `risk_level` column. Filter the DataFrame to keep **only** `"HIGH_RISK"` shipments.
3. **Enrich:** Join the filtered data with `region_df` to bring in the `region_name`. Convert `region_name` to uppercase.
4. **Aggregate & Time Series:** Group the data by `region_name` and the `month` of the `dispatch_date`. Calculate the total `weight_kg` for that month (alias as `monthly_risk_weight`).
5. **Window Analytics:** Create a `cumulative_risk_weight` column that calculates the running total of the `monthly_risk_weight` for each region chronologically by month.
6. **Format:** Order the final DataFrame by `region_name` (ascending) and `month` (ascending).

**Expected Output (`high_risk_logistics_report.show()`):**

```text
+-------------+-----+-------------------+----------------------+
|  region_name|month|monthly_risk_weight|cumulative_risk_weight|
+-------------+-----+-------------------+----------------------+
|       EUROPE|    2|                150|                   150|
|NORTH AMERICA|    1|                800|                   800|
+-------------+-----+-------------------+----------------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum, month, rank, upper
from pyspark.sql.window import Window
from datetime import date
import pandas as pd
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.appName("Challenge_19").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Logistics Fact Table
logistics_schema = StructType([
    StructField("tracking_id", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("weight_kg", IntegerType(), True),
    StructField("dispatch_date", DateType(), True),
    StructField("driver_notes", StringType(), True)
])

logistics_data = [
    ("TRK01", "R1", 500, date(2026, 1, 15), "Handle with care, extremely FRAGILE!"),
    ("TRK02", "R2", 1200, date(2026, 1, 20), "Standard delivery, leave at dock."),
    ("TRK03", "R1", 300, date(2026, 1, 25), "Urgent: Fragile glass components inside."),
    ("TRK04", "R3", 800, date(2026, 2, 5), "Heavy load. Use forklift."),
    ("TRK05", "R2", 150, date(2026, 2, 10), "fragile items, do not drop."),
    ("TRK06", "R1", 600, date(2026, 2, 18), "Routine pickup.")
]
logistics_df = spark.createDataFrame(data=logistics_data, schema=logistics_schema)

# 2. Region Dimension Table
region_schema = StructType([
    StructField("region_id", StringType(), True),
    StructField("region_name", StringType(), True)
])

region_data = [
    ("R1", "North America"),
    ("R2", "Europe"),
    ("R3", "Asia")
]
region_df = spark.createDataFrame(data=region_data, schema=region_schema)

@pandas_udf(StringType())
def risk_status(d_notes: pd.Series) -> pd.Series:

    # # Approach 1 using apply.
    # def check_status(n):
    #     if pd.isna(n): return "Unknown"
    #     return "HIGH_RISK" if n.lower().find("fragile") != -1 else "STANDARD"

    # return d_notes.apply(check_status)
    # # End approach 1

    # Approach 2:
    result = pd.Series("STANDARD", index=d_notes.index)
    is_fragile = d_notes.str.lower().str.contains("fragile", na=False)
    result[is_fragile] = "HIGH_RISK"
    result[d_notes.isna()] = "Unknown"

    return result

running_total_win = (
    Window
    .partitionBy("region_name")
    .orderBy("month")
)

high_risk_logistics_report_df = (
    logistics_df
    .withColumn("risk_level", risk_status(col("driver_notes")))
    .filter(col("risk_level") == "HIGH_RISK")
    .join(
        region_df
        .withColumn("region_name", upper("region_name"))
        , on="region_id"
        , how="inner"
    )
    .groupBy("region_name", month(col("dispatch_date")).alias("month"))
    .agg(sum("weight_kg").alias("monthly_risk_weight"))
    .withColumn("cumulative_risk_weight", sum("monthly_risk_weight").over(running_total_win))
    .orderBy("region_name", "month")
)

high_risk_logistics_report_df.show()
```

### My Output Verification:

```
+-------------+-----+-------------------+----------------------+
|  region_name|month|monthly_risk_weight|cumulative_risk_weight|
+-------------+-----+-------------------+----------------------+
|       EUROPE|    2|                150|                   150|
|NORTH AMERICA|    1|                800|                   800|
+-------------+-----+-------------------+----------------------+
```
