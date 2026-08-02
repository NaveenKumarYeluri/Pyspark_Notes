# PySpark Challenge 11: Strings and Dates (Easy to Complex)

**The Setup:**

Copy the following code into your notebook to generate our mock DataFrame of employee reviews and hire dates.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType
from datetime import date

# Schema
hr_schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("raw_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("review_score", LongType(), True)
])

# Mock Data (Notice the messy names and varying dates)
hr_data = [
    (1, "   John DOE ", "engineering", date(2018, 5, 15), 4),
    (2, "jane smith", "SALES", date(2021, 11, 1), 5),
    (3, "  BOB roberts", "marketing", date(2022, 1, 20), 2),
    (4, "alice johnson", "ENGINEERING", date(2019, 8, 10), 3),
    (5, "Charlie Brown   ", "Sales", date(2023, 3, 5), 4)
]

hr_df = spark.createDataFrame(data=hr_data, schema=hr_schema)
hr_df.show()
```

---

## Challenge 11a: Basic Text Cleaning (Easy)

**Task:**

Create a new DataFrame based on `hr_df`. Add a column called `clean_name` that removes any leading/trailing spaces from `raw_name` and converts the entire name to uppercase. Select only the `emp_id` and your new `clean_name` column.

## Challenge 11b: Date Extraction and Filtering (Medium)

**Task:**

Create a new DataFrame. We want to find employees hired in the first half of the year.

1. Add a column called `hire_month` that extracts the integer month from the `hire_date`.
2. Filter the DataFrame to keep ONLY employees where the `hire_month` is less than or equal to `6` (June).
3. Select `emp_id`, `hire_date`, and `hire_month`.

## Challenge 11c: The Year-End Department Report (Boss Level)

**Task:**

HR needs a standardized report combining string manipulation, date extraction, conditional logic, and aggregation. Write a single chained PySpark command that does the following:

1. **Standardize Department Names (Strings):** Use `upper()` to overwrite the existing `department` column so they are all consistent (e.g., "engineering" and "ENGINEERING" both become "ENGINEERING").
2. **Determine Tenure Status (Dates & Logic):** 
   * Extract the `year()` from the `hire_date`. 
   * Add a column called `status`. If the hire year is strictly less than `2020`, their status is `"Veteran"`. Otherwise, it is `"Recent Hires"`.
3. **Aggregate (Grouping):** Group the data by `department` and `status`.
4. **Calculate (Math):** Calculate the average `review_score` for each group and alias it as `avg_score`.
5. **Format:** Order the final report alphabetically by `department`, and then by `avg_score` descending.

*(Hint: Don't forget to import `upper`, `year`, `when`, `avg`, and `col`!)*

### My Solution:

```python
# Challenge 9: Strings and Dates (Easy to Complex)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, month, when, year, desc, asc
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType
from datetime import date

spark = SparkSession.builder.appName("challenge_9").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema
hr_schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("raw_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("review_score", LongType(), True)
])

# Mock Data (Notice the messy names and varying dates)
hr_data = [
    (1, "   John DOE ", "engineering", date(2018, 5, 15), 4),
    (2, "jane smith", "SALES", date(2021, 11, 1), 5),
    (3, "  BOB roberts", "marketing", date(2022, 1, 20), 2),
    (4, "alice johnson", "ENGINEERING", date(2019, 8, 10), 3),
    (5, "Charlie Brown   ", "Sales", date(2023, 3, 5), 4)
]

hr_df = spark.createDataFrame(data=hr_data, schema=hr_schema)


## Challenge 11a: Basic Text Cleaning (Easy)
new_hr_df = (
    hr_df
    .withColumn("clean_name", upper(trim(col("raw_name"))))
    .select(col("emp_id"), col("clean_name"))
)
new_hr_df.show()

## Challenge 11b: Date Extraction and Filtering (Medium)
new_emp_df = (
    hr_df
    .withColumn("hire_month", month(col("hire_date")))
    .filter(col("hire_month") <= 6)
    .select(col("emp_id"), col("hire_date"), col("hire_month"))
)
new_emp_df.show()

## Challenge 11c: The Year-End Department Report (Boss Level)
report_df = (
    hr_df
    .withColumn("department", upper(col("department")))
    .withColumn("status", 
        when(year(col("hire_date")) < 2020, "Veteran")
        .otherwise("Recent Hires")
    )
    .groupBy("department", "status")
        .avg("review_score")
        .withColumnRenamed("avg(review_score)", "avg_score")
        .orderBy("department", desc("avg_score"))
)
report_df.show()
```

### My Output Verification:

```
+------+-------------+
|emp_id|   clean_name|
+------+-------------+
|     1|     JOHN DOE|
|     2|   JANE SMITH|
|     3|  BOB ROBERTS|
|     4|ALICE JOHNSON|
|     5|CHARLIE BROWN|
+------+-------------+

+------+----------+----------+
|emp_id| hire_date|hire_month|
+------+----------+----------+
|     1|2018-05-15|         5|
|     3|2022-01-20|         1|
|     5|2023-03-05|         3|
+------+----------+----------+

+-----------+------------+---------+
| department|      status|avg_score|
+-----------+------------+---------+
|ENGINEERING|      Vetern|      3.5|
|  MARKETING|Recent Hires|      2.0|
|      SALES|Recent Hires|      4.5|
+-----------+------------+---------+
```
