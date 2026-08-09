# PySpark Challenge 18: The Financial Reshape (Boss Level)

**The Scenario:**

You are a Data Engineer working for a logistics company. The regional managers manually input their quarterly logistics costs into a wide, messy Excel-like format. 

The finance team needs this data combined with the regional director directory, cleaned, and then completely reshaped into a "Manager Scorecard" where they can see the total costs per quarter for each Director side-by-side.

**The Setup:**

Copy this code to generate the mock DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, expr, sum, round

spark = SparkSession.builder.appName("Challenge_18").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Wide Quarterly Costs Data
costs_schema = StructType([
    StructField("region_code", StringType(), True),
    StructField("Q1_Cost", DoubleType(), True),
    StructField("Q2_Cost", DoubleType(), True),
    StructField("Q3_Cost", DoubleType(), True),
    StructField("Q4_Cost", DoubleType(), True)
])

costs_data = [
    ("NE-1", 1500.50, 2000.00, None, 1800.75),
    ("NW-2", 3000.00, 3100.50, 2900.00, 3500.00),
    ("SE-3", None, 500.00, 450.25, 0.0),
    ("SW-4", 1200.00, 1100.00, 1050.00, 1300.00)
]
costs_df = spark.createDataFrame(data=costs_data, schema=costs_schema)

# 2. Regional Director Lookup
dir_schema = StructType([
    StructField("region_code", StringType(), True),
    StructField("director_name", StringType(), True)
])

dir_data = [
    ("NE-1", "Alice Smith"),
    ("NW-2", "Bob Jones"),
    ("SE-3", "Alice Smith"), # Alice manages two regions
    ("SW-4", "Charlie Brown")
]
dir_df = spark.createDataFrame(data=dir_data, schema=dir_schema)
```

## Challenge 18 Task:

Write a single, chained PySpark pipeline to generate the `director_scorecard_df`.

**Business Goals & Logic:**

1.  **Unpivot:** Transform the wide `costs_df` so that the four quarterly cost columns (`Q1_Cost`, `Q2_Cost`, etc.) are melted down into two columns: `quarter` (containing strings like "Q1", "Q2") and `cost` (containing the numeric values).
2.  **Clean:** Remove any resulting rows where the `cost` is `NULL` or exactly `0.0` (as Finance does not want to see inactive quarters).
3.  **Enrich:** Join the unpivoted data with `dir_df` to attach the `director_name` to every record.
4.  **Pivot:** Reshape the data *back* into a wide format. Group the data by `director_name` and pivot on the `quarter` column to calculate the `sum` of the `cost`.
5.  **Format:** Fill any resulting `NULL` values in the final pivoted columns with `0.0`. Order the final DataFrame alphabetically by `director_name`.

**Expected Output (`director_scorecard_df.show()`):**

```text
+-------------+-------+-------+------+-------+
|director_name|     Q1|     Q2|    Q3|     Q4|
+-------------+-------+-------+------+-------+
|  Alice Smith| 1500.5| 2500.0|450.25|1800.75|
|    Bob Jones| 3000.0| 3100.5|2900.0| 3500.0|
|Charlie Brown| 1200.0| 1100.0|1050.0| 1300.0|
+-------------+-------+-------+------+-------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, expr, sum, round

spark = SparkSession.builder.appName("Challenge_16").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Wide Quarterly Costs Data
costs_schema = StructType([
    StructField("region_code", StringType(), True),
    StructField("Q1_Cost", DoubleType(), True),
    StructField("Q2_Cost", DoubleType(), True),
    StructField("Q3_Cost", DoubleType(), True),
    StructField("Q4_Cost", DoubleType(), True)
])

costs_data = [
    ("NE-1", 1500.50, 2000.00, None, 1800.75),
    ("NW-2", 3000.00, 3100.50, 2900.00, 3500.00),
    ("SE-3", None, 500.00, 450.25, 0.0),
    ("SW-4", 1200.00, 1100.00, 1050.00, 1300.00)
]
costs_df = spark.createDataFrame(data=costs_data, schema=costs_schema)

# 2. Regional Director Lookup
dir_schema = StructType([
    StructField("region_code", StringType(), True),
    StructField("director_name", StringType(), True)
])

dir_data = [
    ("NE-1", "Alice Smith"),
    ("NW-2", "Bob Jones"),
    ("SE-3", "Alice Smith"), # Alice manages two regions
    ("SW-4", "Charlie Brown")
]
dir_df = spark.createDataFrame(data=dir_data, schema=dir_schema)

unpivot_expr = """
    stack(
        4,
        "Q1", Q1_Cost,
        "Q2", Q2_Cost,
        "Q3", Q3_Cost,
        "Q4", Q4_Cost
    ) AS (quarter, cost)
"""

director_scorecard_df = (
    costs_df
    .select("region_code", expr(unpivot_expr))
    .filter(col("cost") > 0)
    .join(dir_df, on="region_code", how="inner")
    .groupBy("director_name").pivot("quarter").agg(sum("cost"))
    .na.fill(0)
    .orderBy("director_name")
)
director_scorecard_df.show()
```

### My Output Verification:

```
+-------------+------+------+------+-------+
|director_name|    Q1|    Q2|    Q3|     Q4|
+-------------+------+------+------+-------+
|  Alice Smith|1500.5|2500.0|450.25|1800.75|
|    Bob Jones|3000.0|3100.5|2900.0| 3500.0|
|Charlie Brown|1200.0|1100.0|1050.0| 1300.0|
+-------------+------+------+------+-------+
```
