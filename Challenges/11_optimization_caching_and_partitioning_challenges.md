# PySpark Challenge 13: Optimization, Caching, and Partitioning

## PySpark Challenge 13a: The Master Pipelines

**The Task:**

We have a massive stream of telemetry data from our application. Your manager needs a **VIP Activity Report**. You must produce a summary that identifies the top 3 most "active" users per region based on their daily engagement, including their overall tenure status and total spend.

**The Setup:**

Copy this code to generate your mock HR and Transaction DataFrames.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, upper, when, year, sum, rank
from pyspark.sql.window import Window
from datetime import date

# 1. HR Data
hr_schema = StructType([
    StructField("user_id", LongType(), True),
    StructField("department", StringType(), True),
    StructField("hire_date", DateType(), True)
])
hr_data = [
    (1, "Engineering", date(2019, 3, 15)),
    (2, "Sales", date(2022, 7, 10)),
    (3, "Marketing", date(2020, 1, 20)),
    (4, "Engineering", date(2023, 11, 5)),
    (5, "Sales", date(2018, 9, 25)),
    (6, "Sales", date(2021, 5, 12))
]
hr_df = spark.createDataFrame(data=hr_data, schema=hr_schema)

# 2. Transaction Data
txn_schema = StructType([
    StructField("txn_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("region", StringType(), True),
    StructField("spend", DoubleType(), True)
])
txn_data = [
    (101, 1, "north america", 500.50),
    (102, 1, "north america", 200.00),
    (103, 2, "europe", 150.75),
    (104, 2, "europe", None),
    (105, 3, "asia", 1200.00),
    (106, 4, "north america", 350.25),
    (107, 5, "europe", 800.00),
    (108, 5, "europe", 400.00),
    (109, 6, "europe", 50.00)
]
txn_df = spark.createDataFrame(data=txn_data, schema=txn_schema)

print("HR Data:")
hr_df.show()
print("Transaction Data:")
txn_df.show()
```

**The Goal:**

Write a single, chained PySpark command to generate a consolidated VIP Activity Report by combining the transaction and HR datasets.

Your final DataFrame must contain the following columns:

* `user_id`

* `region` (Standardized to uppercase)

* `total_spend` (The total sum of spend per user and region. Any null spend values should be treated as `0.0` prior to calculation).

* `tenure_status` (`"Veteran"` if the employee was hired prior to the year 2022, otherwise `"Recent Hire"`).

* `regional_rank` (The user's rank based on their `total_spend` in descending order, partitioned within each `region`).

**Requirements:**

* Filter the final output to only include the top 2 spenders per region.

* Apply `.cache()` at the end of the chain to store the final report in memory.

* Order the final output by `region` (ascending) and `regional_rank` (ascending).

**Expected Output:**

```
+-------+-------------+-----------+-------------+-------------+
|user_id|       region|total_spend|tenure_status|regional_rank|
+-------+-------------+-----------+-------------+-------------+
|      3|         ASIA|     1200.0|      Veteran|            1|
|      5|       EUROPE|     1200.0|      Veteran|            1|
|      2|       EUROPE|     150.75|  Recent Hire|            2|
|      1|NORTH AMERICA|      700.5|      Veteran|            1|
|      4|NORTH AMERICA|     350.25|  Recent Hire|            2|
+-------+-------------+-----------+-------------+-------------+
```

### My Solution:

```python
## PySpark Challenge 13a: The Master Pipelines
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, upper, when, year, sum, rank, upper
from pyspark.sql.window import Window
from datetime import date

spark = SparkSession.builder.appName("challenge_11a").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. HR Data
hr_schema = StructType([
    StructField("user_id", LongType(), True),
    StructField("department", StringType(), True),
    StructField("hire_date", DateType(), True)
])
hr_data = [
    (1, "Engineering", date(2019, 3, 15)),
    (2, "Sales", date(2022, 7, 10)),
    (3, "Marketing", date(2020, 1, 20)),
    (4, "Engineering", date(2023, 11, 5)),
    (5, "Sales", date(2018, 9, 25)),
    (6, "Sales", date(2021, 5, 12))
]
hr_df = spark.createDataFrame(data=hr_data, schema=hr_schema)

# 2. Transaction Data
txn_schema = StructType([
    StructField("txn_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("region", StringType(), True),
    StructField("spend", DoubleType(), True)
])
txn_data = [
    (101, 1, "north america", 500.50),
    (102, 1, "north america", 200.00),
    (103, 2, "europe", 150.75),
    (104, 2, "europe", None),
    (105, 3, "asia", 1200.00),
    (106, 4, "north america", 350.25),
    (107, 5, "europe", 800.00),
    (108, 5, "europe", 400.00),
    (109, 6, "europe", 50.00)
]
txn_df = spark.createDataFrame(data=txn_data, schema=txn_schema)

# print("HR Data:")
# hr_df.show()
# print("Transaction Data:")
# txn_df.show()

window_spec = Window.partitionBy("region").orderBy(col("total_spend").desc())

activity_report_df = (
    txn_df
    .drop("txn_id")
    .withColumn("region", upper(col("region")))
    .na.fill(
        {
            "spend":0.0
        }
    )
    .join(
        hr_df
        .withColumn("tenure_status", 
            when(col("hire_date") >= '2022-01-01', "Recent Hire")
        .otherwise("Veteran")
        )
        .drop("hire_date", "department")
    , on="user_id"
    , how="inner"
    )
    .groupBy("user_id", "region", "tenure_status")
    .agg(sum("spend"))
    .withColumnRenamed("sum(spend)", "total_spend")
    .withColumn("regional_rank", rank().over(window_spec))
    .filter(col("regional_rank") <= 2)
    .orderBy(col("region").asc(), col("regional_rank").asc())
    .select("user_id", "region", "total_spend", "tenure_status", "regional_rank")
    .cache()
)
print("Final Report:")
activity_report_df.show()
```

### My Output Verification:

```
+-------+-------------+-----------+-------------+-------------+
|user_id|       region|total_spend|tenure_status|regional_rank|
+-------+-------------+-----------+-------------+-------------+
|      3|         ASIA|     1200.0|      Veteran|            1|
|      5|       EUROPE|     1200.0|      Veteran|            1|
|      2|       EUROPE|     150.75|  Recent Hire|            2|
|      1|NORTH AMERICA|      700.5|      Veteran|            1|
|      4|NORTH AMERICA|     350.25|  Recent Hire|            2|
+-------+-------------+-----------+-------------+-------------+
```

---

## PySpark Challenge 13b: The Global E-Commerce Audit (Boss Level)

**The Scenario:**

You are auditing sales performance for an international e-commerce platform. You have two datasets:

1. **Sales Data:** Contains transaction details, but it has some missing values due to system errors.

2. **Product Catalog:** Contains product categories and base prices.

You need to create a `Sales_Audit_Report` that identifies high-value transactions, calculates effective discounts, and summarizes performance by category and region.

**The Setup:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType
from datetime import date

spark = SparkSession.builder.appName("Challenge_14").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Sales Data
sales_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("product_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("units_sold", LongType(), True),
    StructField("total_revenue", DoubleType(), True),
    StructField("order_date", DateType(), True)
])

sales_data = [
    (1001, "P1", "North America", 5, 250.00, date(2023, 10, 15)),
    (1002, "P2", "Europe", None, 120.00, date(2023, 10, 16)), # Missing units
    (1003, "P1", "Asia", 10, 450.00, date(2023, 11, 01)),
    (1004, "P3", "North America", 2, None, date(2023, 11, 05)), # Missing revenue
    (1005, "P2", "Europe", 8, 400.00, date(2023, 12, 10)),
    (1006, "P4", "Asia", 1, 1500.00, date(2023, 12, 12)),
    (1007, "P1", None, 3, 150.00, date(2023, 12, 15)) # Missing region
]
sales_df = spark.createDataFrame(data=sales_data, schema=sales_schema)

# 2. Product Catalog
catalog_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("base_price", DoubleType(), True)
])

catalog_data = [
    ("P1", "Electronics", 50.00),
    ("P2", "Accessories", 50.00),
    ("P3", "Apparel", 40.00),
    ("P4", "Electronics", 1500.00)
]
catalog_df = spark.createDataFrame(data=catalog_data, schema=catalog_schema)
```

**The Task:**

Write a single, chained PySpark command to create the `Sales_Audit_Report` DataFrame that achieves the following:

1. **Clean the Sales Data:**

* Fill missing `region` values with "Unknown".

* Fill missing `units_sold` with 1.

* Fill missing `total_revenue` with 0.0.

2. **Join:** Combine the cleaned sales data with the product catalog.

3. Calculate Expected Revenue: Add a new column `expected_revenue` which is `units_sold * base_price`.

4. Calculate Discount Status: Add a new column discount_status:

    * If `total_revenue` is less than `expected_revenue`, set it to "Discounted".

    * If `total_revenue` is equal to `expected_revenue`, set it to "Full Price".

    * If `total_revenue` is greater than `expected_revenue`, set it to "Premium/Surcharge".

5. **Filter:** Only keep orders from the year 2023.

6. **Aggregate:** Group by `category` and `region`. Calculate:

    * The sum of `total_revenue` (aliased as `actual_revenue`).

    * The total number of units sold (aliased as `total_units`).

7. **Final Polish:** Order the final report by `category` (ascending) and then by `actual_revenue` (descending).


**Expected Output:**

Your final DataFrame (`Sales_Audit_Report.show()`) should look exactly like this:

```
+-----------+-------------+--------------+-----------+
|   category|       region|actual_revenue|total_units|
+-----------+-------------+--------------+-----------+
|Accessories|       Europe|         520.0|          9|
|    Apparel|North America|           0.0|          2|
|Electronics|         Asia|        1950.0|         11|
|Electronics|North America|         250.0|          5|
|Electronics|      Unknown|         150.0|          3|
+-----------+-------------+--------------+-----------+
```

### My Solution:

```python
## PySpark Challenge 13b: The Global E-Commerce Audit (Boss Level)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType
from pyspark.sql.functions import sum, count
from datetime import date

spark = SparkSession.builder.appName("Challenge_13b").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Sales Data
sales_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("product_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("units_sold", LongType(), True),
    StructField("total_revenue", DoubleType(), True),
    StructField("order_date", DateType(), True)
])

sales_data = [
    (1001, "P1", "North America", 5, 250.00, date(2023, 10, 15)),
    (1002, "P2", "Europe", None, 120.00, date(2023, 10, 16)), # Missing units
    (1003, "P1", "Asia", 10, 450.00, date(2023, 11, int("01"))),
    (1004, "P3", "North America", 2, None, date(2023, 11, int("05"))), # Missing revenue
    (1005, "P2", "Europe", 8, 400.00, date(2023, 12, 10)),
    (1006, "P4", "Asia", 1, 1500.00, date(2023, 12, 12)),
    (1007, "P1", None, 3, 150.00, date(2023, 12, 15)) # Missing region
]
sales_df = spark.createDataFrame(data=sales_data, schema=sales_schema)

# 2. Product Catalog
catalog_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("base_price", DoubleType(), True)
])

catalog_data = [
    ("P1", "Electronics", 50.00),
    ("P2", "Accessories", 50.00),
    ("P3", "Apparel", 40.00),
    ("P4", "Electronics", 1500.00)
]
catalog_df = spark.createDataFrame(data=catalog_data, schema=catalog_schema)

sales_audit_report_df = (
    sales_df
    .filter(col("order_date") >= '2023-01-01')
    .na.fill(
        {
            "region": "Unknown"
            , "units_sold": 1
            , "total_revenue": 0.0
        }
    )
    .join(
        catalog_df
        , on="product_id"
        , how="inner"
    )
    .withColumn("expected_revenue", col("units_sold") * col("base_price"))
    .withColumn("discount_status",
        when(col("total_revenue") < col("expected_revenue"), "Discounted")
        .when(col("total_revenue") == col("expected_revenue"), "Full Price")
        .when(col("total_revenue") > col("expected_revenue"), "Premium/Surcharge")
    )
    .groupBy("category", "region")
    .agg(
        sum("total_revenue").alias("actual_revenue")
        , sum("units_sold").alias("total_units")        
    )
    .orderBy(col("category").asc(), col("actual_revenue").desc())
)
print("Sales Audit Report:")
sales_audit_report_df.show()
```

### My Output Verification:

```
Sales Audit Report:
+-----------+-------------+--------------+-----------+
|   category|       region|actual_revenue|total_units|
+-----------+-------------+--------------+-----------+
|Accessories|       Europe|         520.0|          9|
|    Apparel|North America|           0.0|          2|
|Electronics|         Asia|        1950.0|         11|
|Electronics|North America|         250.0|          5|
|Electronics|      Unknown|         150.0|          3|
+-----------+-------------+--------------+-----------+
```
