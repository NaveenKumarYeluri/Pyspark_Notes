# PySpark Challenge 27: The QA Engineer (Testing Basics)

**The Scenario:**

You have written a custom transformation function for the finance team that calculates a 10% tax on orders and rounds the result to 2 decimal places. 

Before this code is merged into the main production branch, the QA (Quality Assurance) pipeline requires a unit test to prove it calculates the math correctly and handles standard inputs.

**The Setup:**

Run this code in your environment. It defines the business function `apply_tax` that you need to test.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, round

# The business function to be tested
def apply_tax(df):
    """
    Takes a DataFrame with an 'order_total' column.
    Adds a 'tax_amount' column calculated as 10% of the total, rounded to 2 decimals.
    """
    return df.withColumn("tax_amount", round(col("order_total") * 0.10, 2))

# Initialize Spark for testing
spark = SparkSession.builder.appName("Challenge_27").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Shared Schema for your test data
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_total", DoubleType(), True)
])
```

## Challenge 27 Task:

Write a test function named `test_apply_tax()` to validate the business logic.

**Requirements:**

1.  **Create `input_df`**: Use the `order_schema` to create a mock input DataFrame containing two rows:
    *   `"O_1"`, `100.0`
    *   `"O_2"`, `55.55`
2.  **Create `expected_df`**: Create the *expected* output DataFrame. It needs the original columns plus the new `tax_amount` column. You must do the math manually to provide the expected answers (10% of 100.0, and 10% of 55.55 rounded to 2 decimals).
3.  **Execute**: Pass your `input_df` into the `apply_tax()` function and save the result as `actual_df`.
4.  **Assert**: Write an `assert` statement comparing `actual_df.collect()` to `expected_df.collect()`.
5.  Call your test function `test_apply_tax()`. If nothing crashes and it prints "Test Passed!", you have successfully validated your code!

**Expected Output:**

If your expected data matches the actual data, the script should silently pass the assertion and complete successfully.


### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, round

# The business function to be tested
def apply_tax(df):
    """
    Takes a DataFrame with an 'order_total' column.
    Adds a 'tax_amount' column calculated as 10% of the total, rounded to 2 decimals.
    """
    return df.withColumn("tax_amount", round(col("order_total") * 0.10, 2))

# Initialize Spark for testing
spark = SparkSession.builder.appName("Challenge_26").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Shared Schema for your test data
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_total", DoubleType(), True)
])

def test_apply_tax():
    input_data = [("O_1", 100.0), ("O_2", 55.55)]
    input_df = spark.createDataFrame(data = input_data, schema = order_schema)

    input_df.show()

    order_output_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_total", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True)
    ])

    actual_df = apply_tax(input_df)

    expected_data = [("O_1", 100.0, 10.00), ("O_2", 55.55, 5.56)]
    expected_df = spark.createDataFrame(data = expected_data, schema = order_output_schema)

    expected_df.show()
    
    assert actual_df.schema == expected_df.schema, "Schemas do not match!"
    assert actual_df.collect() == expected_df.collect(), "Data does not match!"
    print("Test Passed!")

if __name__ == "__main__":
    test_apply_tax()
```

### My Output Verification:

```
+--------+-----------+
|order_id|order_total|
+--------+-----------+
|     O_1|      100.0|
|     O_2|      55.55|
+--------+-----------+

+--------+-----------+----------+
|order_id|order_total|tax_amount|
+--------+-----------+----------+
|     O_1|      100.0|      10.0|
|     O_2|      55.55|      5.56|
+--------+-----------+----------+

Test Passed!
```
