# PySpark Challenge 32: The Data Scientist's Migration

**The Scenario:**

You are helping a Data Scientist migrate their local `pandas` script to run on your new PySpark cluster. They wrote a script to analyze real estate prices, but it crashes on their laptop when they run it on the massive production dataset.

You need to convert the data to the Pandas API on Spark, let them use their standard Pandas syntax to find the average price per city, and then convert it back to a standard PySpark DataFrame so you can write it to the Data Lake.

**The Setup:**

Run this code to generate the starting PySpark DataFrame.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import warnings

# Suppress pandas API warnings for clean output
warnings.filterwarnings("ignore")

spark = SparkSession.builder.appName("Challenge_32").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("property_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("sq_ft", DoubleType(), True),
    StructField("price", DoubleType(), True)
])

data = [
    ("P1", "Seattle", 1200.0, 600000.0),
    ("P2", "Seattle", 1500.0, 800000.0),
    ("P3", "Austin", 2000.0, 500000.0),
    ("P4", "Austin", 2500.0, 650000.0),
    ("P5", "Denver", 1800.0, 450000.0)
]
pyspark_df = spark.createDataFrame(data, schema)
```

## Challenge 32 Task:

1. Import `pyspark.pandas as ps`.
2. Convert the `pyspark_df` into a Spark Pandas DataFrame named `ps_df` using `.pandas_api()`.
3. **Use Pandas Syntax:** Create a new column in `ps_df` called `price_per_sqft` by dividing the `price` column by the `sq_ft` column. *(Do not use PySpark's `.withColumn()`, use Pandas bracket syntax: `ps_df['new_col'] = ...`)*.
4. **Use Pandas Aggregation:** Group `ps_df` by the `city` column and calculate the `.mean()`. Save this as `ps_summary_df`.
5. Select only the `price_per_sqft` column from the summary dataframe. (In pandas, grouping automatically sets the grouped column as the index, so `city` is already safe).
6. Convert `ps_summary_df` back into a standard PySpark DataFrame named `final_pyspark_df` using `.to_spark()`.
7. Show the final PySpark DataFrame!

**Expected Output (`final_pyspark_df.show()`):**

```text
+-------+------------------+
|   city|    price_per_sqft|
+-------+------------------+
| Denver|             250.0|
| Austin|             255.0|
|Seattle| 516.6666666666666|
+-------+------------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import warnings
import pyspark.pandas as ps

# Suppress pandas API warnings for clean output
warnings.filterwarnings("ignore")

spark = SparkSession.builder.appName("Challenge_31").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("property_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("sq_ft", DoubleType(), True),
    StructField("price", DoubleType(), True)
])

data = [
    ("P1", "Seattle", 1200.0, 600000.0),
    ("P2", "Seattle", 1500.0, 800000.0),
    ("P3", "Austin", 2000.0, 500000.0),
    ("P4", "Austin", 2500.0, 650000.0),
    ("P5", "Denver", 1800.0, 450000.0)
]
pyspark_df = spark.createDataFrame(data, schema)

# Pyspark to Pandas
ps_df = pyspark_df.pandas_api()

# Aggregation
ps_df["price_per_sqft"] = ps_df["price"] / ps_df["sq_ft"]

# Grouping data to get mean
ps_summary_df = ps_df.groupby("city").mean().reset_index()
print(ps_summary_df.head(), "\n")

# Selecting required cols
ps_summary_df = ps_summary_df[["city", "price_per_sqft"]]

# Revert to Pyspark.
final_pyspark_df = ps_summary_df.to_spark()
final_pyspark_df.show()
```

### My Output Verification:

```
city   sq_ft     price  price_per_sqft
0  Seattle  1350.0  700000.0      516.666667
1   Austin  2250.0  575000.0      255.000000
2   Denver  1800.0  450000.0      250.000000 

+-------+-----------------+
|   city|   price_per_sqft|
+-------+-----------------+
|Seattle|516.6666666666667|
| Austin|            255.0|
| Denver|            250.0|
+-------+-----------------+
```
