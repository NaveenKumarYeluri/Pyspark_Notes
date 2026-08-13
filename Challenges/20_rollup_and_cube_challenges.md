# PySpark Challenge 22: The Executive BI Cube (Boss Level)

**The Scenario:**

You are building the backend data layer for a C-suite Executive Dashboard. The CEO wants to see quarterly revenue broken down by `Region`, `Category`, and `Platform` (Web vs. Mobile).

However, they also want to be able to instantly see the subtotals for *any combination* of those three dimensions (e.g., just Mobile sales across all regions, or just Electronics sales in North America).

**The Setup:**

Copy this code to generate the mock DataFrame.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, sum, when

spark = SparkSession.builder.appName("Challenge_22").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Sales Fact Table
sales_schema = StructType([
    StructField("region", StringType(), True),
    StructField("category", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("revenue", DoubleType(), True)
])

sales_data = [
    ("North America", "Electronics", "Web", 5000.0),
    ("North America", "Electronics", "Mobile", 3000.0),
    ("North America", "Apparel", "Web", 2000.0),
    ("Europe", "Electronics", "Web", 4000.0),
    ("Europe", "Electronics", "Mobile", 1000.0),
    ("Europe", "Apparel", "Mobile", 1500.0)
]
sales_df = spark.createDataFrame(data=sales_data, schema=sales_schema)
```

## Challenge 22 Task:

Write a single, chained PySpark pipeline to generate the `executive_cube_df`.

**Business Goals & Expected Output:**

1. Use the appropriate advanced aggregation function to calculate the total `revenue` for **every possible combination** of `region`, `category`, and `platform`. (Alias the aggregated sum as `total_revenue`).
2. Clean up the output: Any `null` values generated in the `region`, `category`, or `platform` columns by the aggregation must be replaced with the string `"ALL"`.
3. Add a conditional `performance_flag` column:
   * If the `total_revenue` is greater than or equal to `5000.0`, label it `"High"`.
   * Otherwise, label it `"Standard"`.
4. Order the final report by `region` (descending), `category` (descending), and `platform` (descending).

**Expected Output (`executive_cube_df.show(15)`):**
*(Note: Showing first 15 rows of the resulting 27-row dataframe)*

```text
+-------------+-----------+--------+-------------+----------------+
|       region|   category|platform|total_revenue|performance_flag|
+-------------+-----------+--------+-------------+----------------+
|North America|Electronics|     Web|       5000.0|            High|
|North America|Electronics|  Mobile|       3000.0|        Standard|
|North America|Electronics|     ALL|       8000.0|            High|
|North America|    Apparel|     Web|       2000.0|        Standard|
|North America|    Apparel|     ALL|       2000.0|        Standard|
|North America|        ALL|     Web|       7000.0|            High|
|North America|        ALL|  Mobile|       3000.0|        Standard|
|North America|        ALL|     ALL|      10000.0|            High|
|       Europe|Electronics|     Web|       4000.0|        Standard|
|       Europe|Electronics|  Mobile|       1000.0|        Standard|
|       Europe|Electronics|     ALL|       5000.0|            High|
|       Europe|    Apparel|  Mobile|       1500.0|        Standard|
|       Europe|    Apparel|     ALL|       1500.0|        Standard|
|       Europe|        ALL|     Web|       4000.0|        Standard|
|       Europe|        ALL|  Mobile|       2500.0|        Standard|
+-------------+-----------+--------+-------------+----------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, sum, when

spark = SparkSession.builder.appName("Challenge_22").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Sales Fact Table
sales_schema = StructType([
    StructField("region", StringType(), True),
    StructField("category", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("revenue", DoubleType(), True)
])

sales_data = [
    ("North America", "Electronics", "Web", 5000.0),
    ("North America", "Electronics", "Mobile", 3000.0),
    ("North America", "Apparel", "Web", 2000.0),
    ("Europe", "Electronics", "Web", 4000.0),
    ("Europe", "Electronics", "Mobile", 1000.0),
    ("Europe", "Apparel", "Mobile", 1500.0)
]
sales_df = spark.createDataFrame(data=sales_data, schema=sales_schema)

executive_cube_df = (
    sales_df
    .cube("region", "category", "platform").agg(sum("revenue").alias("total_revenue"))
    .na.fill("ALL")
    .withColumn("performance_flag", when(col("total_revenue") >= 5000.0, "High").otherwise("Standard"))
    .orderBy(col("region").desc(), col("category").desc(), col("platform").desc())
)

executive_cube_df.show()
```

### My Output Verification:

```
+-------------+-----------+--------+-------------+----------------+
|       region|   category|platform|total_revenue|performance_flag|
+-------------+-----------+--------+-------------+----------------+
|North America|Electronics|     Web|       5000.0|            High|
|North America|Electronics|  Mobile|       3000.0|        Standard|
|North America|Electronics|     ALL|       8000.0|            High|
|North America|    Apparel|     Web|       2000.0|        Standard|
|North America|    Apparel|     ALL|       2000.0|        Standard|
|North America|        ALL|     Web|       7000.0|            High|
|North America|        ALL|  Mobile|       3000.0|        Standard|
|North America|        ALL|     ALL|      10000.0|            High|
|       Europe|Electronics|     Web|       4000.0|        Standard|
|       Europe|Electronics|  Mobile|       1000.0|        Standard|
|       Europe|Electronics|     ALL|       5000.0|            High|
|       Europe|    Apparel|  Mobile|       1500.0|        Standard|
|       Europe|    Apparel|     ALL|       1500.0|        Standard|
|       Europe|        ALL|     Web|       4000.0|        Standard|
|       Europe|        ALL|  Mobile|       2500.0|        Standard|
|       Europe|        ALL|     ALL|       6500.0|            High|
|          ALL|Electronics|     Web|       9000.0|            High|
|          ALL|Electronics|  Mobile|       4000.0|        Standard|
|          ALL|Electronics|     ALL|      13000.0|            High|
|          ALL|    Apparel|     Web|       2000.0|        Standard|
+-------------+-----------+--------+-------------+----------------+
only showing top 20 rows
```
