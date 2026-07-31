# PySpark Challenge 9: Window Functions

**The Setup:**

Copy the following code into your notebook to generate our mock DataFrame of company employees.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, avg

# Schema
emp_schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# Mock Data
emp_data = [
    (1, "Engineering", 120000.0),
    (2, "Engineering", 95000.0),
    (3, "Engineering", 130000.0),
    (4, "Sales", 85000.0),
    (5, "Sales", 90000.0),
    (6, "Sales", 110000.0)
]

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp_df.show()
```

---

## Challenge 9a: Create the Window Specification

**Task:**

Create a Window specification stored in a variable named `dept_window`. It must:

1. Partition the data by `department`.
2. Order the data by `salary` in **descending** order.

## Challenge 9b: Apply the Window Function

**Task:**

Write a single, chained PySpark command that does the following:

1. Uses `.withColumn()` to create a new column named `salary_rank`.
2. Applies the `rank()` function over your `dept_window` to populate this new column.
3. Shows the final DataFrame.

*(If you did this correctly, you should see the employees ranked 1, 2, 3 within the Engineering department, and the rank resetting to 1, 2, 3 for the Sales department!)*

### My Solution:

```python
# Challenge 9a and 9b

from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("challenge_7").master("local[*]").getOrCreate()

# Add this line to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# Schema
emp_schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# Mock Data
emp_data = [
    (1, "Engineering", 120000.0),
    (2, "Engineering", 95000.0),
    (3, "Engineering", 130000.0),
    (4, "Sales", 85000.0),
    (5, "Sales", 90000.0),
    (6, "Sales", 110000.0)
]

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp_df.show()

## Challenge 9a: Create the Window Specification
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())

## Challenge 9b: Apply the Window Function
ranked_df = emp_df.withColumn(
    "salary_rank",
    rank().over(dept_window)
)
ranked_df.show()
```

### My Output Verification:

```
+------+-----------+--------+
|emp_id| department|  salary|
+------+-----------+--------+
|     1|Engineering|120000.0|
|     2|Engineering| 95000.0|
|     3|Engineering|130000.0|
|     4|      Sales| 85000.0|
|     5|      Sales| 90000.0|
|     6|      Sales|110000.0|
+------+-----------+--------+

+------+-----------+--------+-----------+
|emp_id| department|  salary|salary_rank|
+------+-----------+--------+-----------+
|     3|Engineering|130000.0|          1|
|     1|Engineering|120000.0|          2|
|     2|Engineering| 95000.0|          3|
|     6|      Sales|110000.0|          1|
|     5|      Sales| 90000.0|          2|
|     4|      Sales| 85000.0|          3|
+------+-----------+--------+-----------+
```

---

# PySpark Challenge 9c: The Real-World Pipeline (Boss Level)

**The Scenario:**

You are a Data Engineer for a large company. HR has provided you with two messy datasets: an employee table (which is missing some salary data) and a department reference table. 

They want a report of the top 2 highest-paid employees in each department, and they want to know exactly how much those top performers make compared to their department's average salary.

**The Setup:**

Copy this code to generate your messy starting DataFrames.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, avg, round

# 1. Employee Data (Notice the null salaries and the unmatched D3)
emp_schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("dept_id", StringType(), True),
    StructField("salary", DoubleType(), True)
])
emp_data = [
    (1, "Alice", "D1", 120000.0),
    (2, "Bob", "D1", 95000.0),
    (3, "Charlie", "D1", None), 
    (4, "David", "D2", 85000.0),
    (5, "Eve", "D2", 90000.0),
    (6, "Frank", "D2", 110000.0),
    (7, "Grace", "D3", 75000.0), 
]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

# 2. Department Data
dept_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])
dept_data = [
    ("D1", "Engineering"),
    ("D2", "Sales"),
    ("D4", "Marketing")
]
dept_df = spark.createDataFrame(data=dept_data, schema=dept_schema)

print("Raw Employees:")
emp_df.show()
print("Raw Departments:")
dept_df.show()
```

---

## Challenge 9c: The Master Pipeline

**Task:**

Write a script that processes this data. You will need to define your Window specifications first, and then write a single, heavily chained PySpark command to transform the data.

Your pipeline must accomplish the following steps in this exact logical order:

1. **Cleanse (Part 5):** Fill any `null` values in the `salary` column of `emp_df` with a default value of `80000.0`.
2. **Join (Part 6):** Perform an `inner` join between your cleaned employee data and `dept_df` on the `dept_id` column. Chain a `.drop()` command to remove the redundant `dept_id` column.
3. **Window 1 - Aggregation (Part 9 & 4):** Add a new column called `dept_avg` that calculates the average salary for each `dept_name` across the whole department.
4. **Window 2 - Ranking (Part 9):** Add a new column called `salary_rank` that ranks employees within their `dept_name` based on their `salary` (highest to lowest).
5. **Column Math (Part 4):** Add a new column called `diff_from_avg` that subtracts the `dept_avg` from the employee's `salary`. 
6. **Filter (Part 3):** Keep ONLY the employees who have a `salary_rank` of 1 or 2.
7. **Format (Part 3 & 4):** Select only the following columns for the final report: `name`, `dept_name`, `salary`, `salary_rank`, and `diff_from_avg`. Finally, order the whole DataFrame by `dept_name` (ascending) and `salary_rank` (ascending).

*(Pro-tip: Use the parentheses trick you learned in Part 6 to format this massive chain cleanly!)*


### My Solution:

```python
# Challenge 9c

from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.sql.functions import col, rank, avg, when
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("challenge_7c").master("local[*]").getOrCreate()

# Add this line to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# 1. Employee Data (Notice the null salaries and the unmatched D3)
emp_schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("dept_id", StringType(), True),
    StructField("salary", DoubleType(), True)
])
emp_data = [
    (1, "Alice", "D1", 120000.0),
    (2, "Bob", "D1", 95000.0),
    (3, "Charlie", "D1", None), 
    (4, "David", "D2", 85000.0),
    (5, "Eve", "D2", 90000.0),
    (6, "Frank", "D2", 110000.0),
    (7, "Grace", "D3", 75000.0), 
]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

# 2. Department Data
dept_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])
dept_data = [
    ("D1", "Engineering"),
    ("D2", "Sales"),
    ("D4", "Marketing")
]
dept_df = spark.createDataFrame(data=dept_data, schema=dept_schema)

# print("Raw Employees:")
# emp_df.show()
# print("Raw Departments:")
# dept_df.show()

dept_avg_sal_window = Window.partitionBy("dept_name")
dept_rank_sal_window = Window.partitionBy("dept_name").orderBy(col("salary").desc())

modified_df = (emp_df
    .na.fill({"salary": 80000.0})
    .join(dept_df, on="dept_id", how="inner")
    .drop(col("dept_id"))
    .withColumn("dept_avg", avg(col("salary")).over(dept_avg_sal_window))
    .withColumn("salary_rank", rank().over(dept_rank_sal_window))
    .withColumn("diff_from_avg", col("salary") - col("dept_avg"))
    .filter((col("salary_rank") == 1) | (col("salary_rank") == 2))
    .select(col("name"), col("dept_name"), col("salary"), col("salary_rank"), col("diff_from_avg"))
    .orderBy(col("dept_name").asc(), col("salary_rank").asc())
)

modified_df.show()
```

### My Output Verification:

```
+-----+-----------+--------+-----------+-------------------+
| name|  dept_name|  salary|salary_rank|      diff_from_avg|
+-----+-----------+--------+-----------+-------------------+
|Alice|Engineering|120000.0|          1|  21666.66666666667|
|  Bob|Engineering| 95000.0|          2|-3333.3333333333285|
|Frank|      Sales|110000.0|          1|            15000.0|
|  Eve|      Sales| 90000.0|          2|            -5000.0|
+-----+-----------+--------+-----------+-------------------+
```
