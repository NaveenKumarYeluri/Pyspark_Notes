# PySpark Capstone Project: The Medallion Architecture

You have completed the PySpark foundations! You now know how to handle Schemas, Missing Data, Joins, Aggregations, Conditionals, UDFs, Windows, and Streaming. 

It is time to put it all together into a single, comprehensive **Capstone Project**.

## The Scenario

You are the Lead Data Engineer for a massive global e-commerce brand. The company uses a **Medallion Architecture** (Bronze, Silver, Gold data layers). 

*   **Bronze:** Raw, messy data ingested from APIs and databases.
*   **Silver:** Cleaned, filtered, and joined data.
*   **Gold:** Highly aggregated, business-level reporting data.

The executive team needs a daily "Gold" report that identifies their top-tier customers, calculates total revenue, and ranks customers within their respective countries based on their spending.

## The Setup

Run the following code to generate your messy "Bronze" DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col, sum, when, upper, coalesce, rank, current_date, datediff
from pyspark.sql.window import Window
from datetime import date

spark = SparkSession.builder.appName("Capstone").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. BRONZE USERS (Notice missing countries and inconsistent casing)
users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True)
])
users_data = [
    ("U1", "Alice", "usa", date(2022, 1, 15)),
    ("U2", "Bob", None, date(2023, 5, 20)),
    ("U3", "Charlie", "UK", date(2020, 10, 10)),
    ("U4", "Diana", "USA", date(2024, 2, 28)),
    ("U5", "Eve", "uk", date(2021, 7, 14))
]
users_df = spark.createDataFrame(data=users_data, schema=users_schema)

# 2. BRONZE TRANSACTIONS (Notice missing amounts)
txns_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("amount", DoubleType(), True)
])
txns_data = [
    ("T1", "U1", "Electronics", 500.0),
    ("T2", "U1", "Apparel", 150.0),
    ("T3", "U2", "Electronics", None), # Missing amount
    ("T4", "U3", "Home", 800.0),
    ("T5", "U3", "Electronics", 1200.0),
    ("T6", "U4", "Apparel", 50.0),
    ("T7", "U5", "Home", 300.0),
    ("T8", "U5", "Apparel", 100.0)
]
txns_df = spark.createDataFrame(data=txns_data, schema=txns_schema)
```

## The Capstone Task

Write a single, heavily chained PySpark pipeline to generate the `gold_customer_report_df`.

**Business Requirements:**

1.  **Clean Transactions:** Fill any `null` values in the `amount` column of `txns_df` with `0.0`.
2.  **Aggregate Transactions:** Group the transactions by `user_id` and calculate the total sum of `amount` (alias as `total_spent`).
3.  **Enrich (Join):** Join the aggregated transactions with the `users_df` using an inner join.
4.  **Clean Users:** Fill any `null` values in the `country` column with `"UNKNOWN"`. Then, standardize the `country` column so it is entirely UPPERCASE.
5.  **Calculate Tenure:** Create a new column called `days_active` that calculates the number of days between the user's `signup_date` and the `current_date()`.
6.  **Conditional Loyalty Tier:** Create a new column called `loyalty_tier`:
    *   If `total_spent` > `1000` OR `days_active` > `1000`, set to `"Platinum"`.
    *   If `total_spent` > `500`, set to `"Gold"`.
    *   Otherwise, set to `"Silver"`.
7.  **Window Ranking:** Create a new column called `country_rank` that ranks customers *within each country* based on their `total_spent` (from highest to lowest).
8.  **Format:** Select only the following columns: `user_id`, `name`, `country`, `loyalty_tier`, `total_spent`, and `country_rank`.
9.  **Order:** Order the final DataFrame by `country` (ascending) and `country_rank` (ascending).

**Expected Output (`gold_customer_report_df.show()`):**
*(Note: Your output might look slightly different depending on exactly when you run the `current_date()` calculation for the Platinum tier, but the ranks and totals will be static).*

```text
+-------+-------+-------+------------+-----------+------------+
|user_id|   name|country|loyalty_tier|total_spent|country_rank|
+-------+-------+-------+------------+-----------+------------+
|     U3|Charlie|     UK|    Platinum|     2000.0|           1|
|     U5|    Eve|     UK|    Platinum|      400.0|           2|
|     U2|    Bob|UNKNOWN|      Silver|        0.0|           1|
|     U1|  Alice|    USA|    Platinum|      650.0|           1|
|     U4|  Diana|    USA|      Silver|       50.0|           2|
+-------+-------+-------+------------+-----------+------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col, sum, when, upper, coalesce, rank, current_date, datediff
from pyspark.sql.window import Window
from datetime import date

spark = SparkSession.builder.appName("Capstone").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. BRONZE USERS (Notice missing countries and inconsistent casing)
users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True)
])
users_data = [
    ("U1", "Alice", "usa", date(2022, 1, 15)),
    ("U2", "Bob", None, date(2023, 5, 20)),
    ("U3", "Charlie", "UK", date(2020, 10, 10)),
    ("U4", "Diana", "USA", date(2024, 2, 28)),
    ("U5", "Eve", "uk", date(2021, 7, 14))
]
users_df = spark.createDataFrame(data=users_data, schema=users_schema)

# 2. BRONZE TRANSACTIONS (Notice missing amounts)
txns_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("amount", DoubleType(), True)
])
txns_data = [
    ("T1", "U1", "Electronics", 500.0),
    ("T2", "U1", "Apparel", 150.0),
    ("T3", "U2", "Electronics", None), # Missing amount
    ("T4", "U3", "Home", 800.0),
    ("T5", "U3", "Electronics", 1200.0),
    ("T6", "U4", "Apparel", 50.0),
    ("T7", "U5", "Home", 300.0),
    ("T8", "U5", "Apparel", 100.0)
]
txns_df = spark.createDataFrame(data=txns_data, schema=txns_schema)

country_rank_win = Window.partitionBy("country").orderBy(col("total_spent").desc())

gold_customer_report_df = (
    txns_df
    .na.fill(
        {
            "amount": 0.0
        }
    )
    .groupBy(col("user_id"))
    .agg(sum("amount").alias("total_spent"))
    .join(users_df, on="user_id", how="inner")
    .na.fill(
        {"country":"UNKNOWN"}
    )
    .withColumn("country", upper(col("country")))
    .withColumn("days_active", datediff(current_date(), col("signup_date")))
    .withColumn("loyalty_tier", 
        when((col("total_spent") > 1000) | (col("days_active") > 1000), "Platinum")
        .when(col("total_spent") > 500, "Gold")
        .otherwise("Silver")
    )
    .withColumn("country_rank", rank().over(country_rank_win))
    .select("user_id", "name", "country", "loyalty_tier", "total_spent", "country_rank")
    .orderBy("country", "country_rank")
)

gold_customer_report_df.show()
```

### My Output Verification:

```
+-------+-------+-------+------------+-----------+------------+
|user_id|   name|country|loyalty_tier|total_spent|country_rank|
+-------+-------+-------+------------+-----------+------------+
|     U3|Charlie|     UK|    Platinum|     2000.0|           1|
|     U5|    Eve|     UK|    Platinum|      400.0|           2|
|     U2|    Bob|UNKNOWN|    Platinum|        0.0|           1|
|     U1|  Alice|    USA|    Platinum|      650.0|           1|
|     U4|  Diana|    USA|      Silver|       50.0|           2|
+-------+-------+-------+------------+-----------+------------+
```
