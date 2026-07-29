# PySpark Challenge 7: Writing and Saving Data

**The Setup:**
Copy the following code into your notebook to generate our final mock DataFrame.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType

final_schema = StructType([
    StructField("flight_id", LongType(), True),
    StructField("airline", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("is_delayed", BooleanType(), True)
])

final_data = [
    (101, "Delta", "LAX", False),
    (102, "United", "JFK", True),
    (103, "Delta", "ORD", True),
    (104, "Southwest", "MIA", False),
    (105, "United", "SFO", False)
]

final_df = spark.createDataFrame(data=final_data, schema=final_schema)
final_df.show()
```

---

## Challenge 7a: The CSV Export

**Task:**

Write a command to save `final_df` as a CSV into a folder named `"challenge_7_csv"`. Make sure to include the column headers! 
*(If you are in Colab, you will see this folder appear in the file browser on the left side of your screen).*

## Challenge 7b: The Production Parquet Export

**Task:** 

Write a single, chained PySpark command that saves `final_df` to a folder named `"challenge_7_parquet"` with the following production-grade requirements:

1. The format must be **Parquet**.
2. It must safely **overwrite** any existing files in that folder.
3. The data must be physically **partitioned** on the disk by the `airline` column.


### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType

spark = SparkSession.builder \
    .appName("challenge_4") \
    .master("local[*]") \
    .getOrCreate()

final_schema = StructType([
    StructField("flight_id", LongType(), True),
    StructField("airline", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("is_delayed", BooleanType(), True)
])

final_data = [
    (101, "Delta", "LAX", False),
    (102, "United", "JFK", True),
    (103, "Delta", "ORD", True),
    (104, "Southwest", "MIA", False),
    (105, "United", "SFO", False)
]

final_df = spark.createDataFrame(data=final_data, schema=final_schema)
final_df.show()

## Challenge 7a: The CSV Export
final_df.write.csv("/content/Output_Files/challenge_7_csv/", header=True)

## Challenge 7b: The Production Parquet Export
(final_df
    .write.mode("overwrite")
    .partitionBy("airline")
    .parquet("/content/Output_Files/challenge_7_parquet/")
)
```

### My Output Verification:

```
+---------+---------+-----------+----------+
|flight_id|  airline|destination|is_delayed|
+---------+---------+-----------+----------+
|      101|    Delta|        LAX|     false|
|      102|   United|        JFK|      true|
|      103|    Delta|        ORD|      true|
|      104|Southwest|        MIA|     false|
|      105|   United|        SFO|     false|
+---------+---------+-----------+----------+
```
