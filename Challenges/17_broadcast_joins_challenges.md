# PySpark Challenge 19: The Black Friday Broadcast (Boss Level)

**The Scenario:**

You are a Data Engineer for a global retail chain. It is the day after Black Friday, and you need to process the raw, massive transaction logs.

The transactions contain an array of purchased categories. You need to join these transactions with a tiny store lookup table to figure out which categories generated the most revenue in each geographical region. Because the transaction table in production is hundreds of terabytes, your manager has explicitly mandated that you use a **Broadcast Join** to avoid a network shuffle, and you must rank the categories by revenue.

**The Setup:**

Copy this code to generate the mock DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql.functions import col, broadcast, explode, sum, rank, upper
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Challenge_19").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Massive Transaction Fact Table
txn_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("categories", ArrayType(StringType()), True)
])

txn_data = [
    ("T1", "S_01", 1200.50, ["Electronics", "Gaming"]),
    ("T2", "S_02", 45.00, ["Apparel"]),
    ("T3", "S_01", 300.00, ["Electronics", "Home"]),
    ("T4", "S_03", -50.00, ["Returns"]), # Invalid revenue, should be filtered
    ("T5", "S_03", 800.00, ["Gaming", "Apparel"]),
    ("T6", "S_02", 150.00, ["Home"])
]
txn_df = spark.createDataFrame(data=txn_data, schema=txn_schema)

# 2. Tiny Store Lookup Dimension Table
store_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("region", StringType(), True)
])

store_data = [
    ("S_01", "North America"),
    ("S_02", "Europe"),
    ("S_03", "Asia")
]
store_df = spark.createDataFrame(data=store_data, schema=store_schema)
```

## Challenge 19 Task:

Write a single, chained PySpark pipeline to create the `regional_category_ranks_df`.

**Business Goals & Requirements:**

1.  Filter out any transactions from `txn_df` where the `revenue` is less than or equal to `0`.
2.  Join the filtered transactions with the `store_df`. **You MUST use a Broadcast Join on the store table.**
3.  Standardize the `region` column to be completely uppercase.
4.  Flatten the `categories` array so that each category gets its own row (while retaining the transaction's revenue). *Note: For this analysis, we attribute the full transaction revenue to every category in the basket.*
5.  Calculate the total revenue generated per `region` and per `category` (alias this as `total_revenue`).
6.  Using a Window function, rank the categories within each region based on their `total_revenue` in descending order. Alias this as `category_rank`.
7.  Format the final output to only show `region`, `category`, `total_revenue`, and `category_rank`. Order the final DataFrame by `region` (ascending) and `category_rank` (ascending).

**Expected Output (`regional_category_ranks_df.show()`):**

```text
+-------------+-----------+-------------+-------------+
|       region|   category|total_revenue|category_rank|
+-------------+-----------+-------------+-------------+
|         ASIA|    Apparel|        800.0|            1|
|         ASIA|     Gaming|        800.0|            1|
|       EUROPE|       Home|        150.0|            1|
|       EUROPE|    Apparel|         45.0|            2|
|NORTH AMERICA|Electronics|       1500.5|            1|
|NORTH AMERICA|     Gaming|       1200.5|            2|
|NORTH AMERICA|       Home|        300.0|            3|
+-------------+-----------+-------------+-------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql.functions import col, broadcast, explode, sum, rank, upper
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Challenge_19").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Massive Transaction Fact Table
txn_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("categories", ArrayType(StringType()), True)
])

txn_data = [
    ("T1", "S_01", 1200.50, ["Electronics", "Gaming"]),
    ("T2", "S_02", 45.00, ["Apparel"]),
    ("T3", "S_01", 300.00, ["Electronics", "Home"]),
    ("T4", "S_03", -50.00, ["Returns"]), # Invalid revenue, should be filtered
    ("T5", "S_03", 800.00, ["Gaming", "Apparel"]),
    ("T6", "S_02", 150.00, ["Home"])
]
txn_df = spark.createDataFrame(data=txn_data, schema=txn_schema)

# 2. Tiny Store Lookup Dimension Table
store_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("region", StringType(), True)
])

store_data = [
    ("S_01", "North America"),
    ("S_02", "Europe"),
    ("S_03", "Asia")
]
store_df = spark.createDataFrame(data=store_data, schema=store_schema)

win_rank = Window.partitionBy("region").orderBy(col("total_revenue").desc())

regional_category_ranks_df = (
    txn_df
    .filter(col("revenue") > 0)
    .join(broadcast(store_df), on="store_id", how="inner")
    .withColumn("region", upper(col("region")))
    .withColumn("category", explode(col("categories")))
    .groupBy("region", "category").agg(sum(col("revenue")).alias("total_revenue"))
    # .withColumnRenamed("sum(revenue)", "total_revenue")
    .withColumn("category_rank", rank().over(win_rank))
    .orderBy(col("region"), col("category_rank"))
)
regional_category_ranks_df.show(truncate=False)
```

### My Output Verification:

```
+-------------+-----------+-------------+-------------+
|region       |category   |total_revenue|category_rank|
+-------------+-----------+-------------+-------------+
|ASIA         |Gaming     |800.0        |1            |
|ASIA         |Apparel    |800.0        |1            |
|EUROPE       |Home       |150.0        |1            |
|EUROPE       |Apparel    |45.0         |2            |
|NORTH AMERICA|Electronics|1500.5       |1            |
|NORTH AMERICA|Gaming     |1200.5       |2            |
|NORTH AMERICA|Home       |300.0        |3            |
+-------------+-----------+-------------+-------------+
```

