# PySpark Challenge 29: The Production Architect (The Final Challenge)

**The Scenario:**

You are migrating a Junior Analyst's notebook code into a production GitHub repository.

The analyst wrote a simple script that reads some mock data, filters it, and prints the result. However, the script is completely un-packaged, relies on global variables floating in the file, and hardcodes the local master URL.

**The Setup (The Analyst's Messy Code):**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# BAD: Hardcoded local master inside the code
spark = SparkSession.builder.appName("Analyst_Job").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# BAD: Global variables just floating in the file
mock_data = [("A", 10), ("B", 20), ("C", 5)]
df = spark.createDataFrame(mock_data, ["id", "val"])

# BAD: Logic executed sequentially at the root level
filtered_df = df.filter(col("val") > 10)
filtered_df.show()
```

## Challenge 29 Task:

Rewrite the analyst's code into a clean, production-ready Python script template.

**Requirements:**

1. Create a function called `process_data(spark)`. Move the data creation, filtering, and `.show()` logic *inside* this function.
2. Create a `main()` function. Inside `main`:
   * Initialize the `SparkSession`.
   * **Crucial:** Remove the `.master("local[*]")` configuration. (It will run locally anyway by default if you just run the python script, but removing it allows `spark-submit` to override it later).
   * Call your `process_data(spark)` function.
3. Add the standard Python `if __name__ == "__main__":` block at the bottom of the script to trigger `main()`.
4. Run it locally one last time to ensure it outputs the single row `("B", 20)`.

**Expected Output:**

The exact same visual output on your screen, but wrapped in an architecture that is ready to be deployed to a massive production cluster!

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("Analyst_Job").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    process_data(spark)

def process_data(spark):
    mock_data = [("A", 10), ("B", 20), ("C", 5)]
    df = spark.createDataFrame(mock_data, ["id", "val"])

    filtered_df = df.filter(col("val") > 10)
    filtered_df.show()

if __name__ == "__main__":
    main()
```

### My Output Verification:

```
+---+---+
| id|val|
+---+---+
|  B| 20|
+---+---+
```
