# PySpark Learning Log: Part 27 - Unit Testing (with pytest)

In a production environment, you never run a PySpark pipeline on a live cluster without testing it first. Data Engineers use automated testing frameworks (like `pytest`) to verify that their transformations do exactly what they are supposed to do.

The concept is simple:

1. Create a tiny "Mock" input DataFrame.
2. Create a tiny "Expected" output DataFrame (what the data *should* look like).
3. Run your transformation function on the input.
4. `Assert` that the actual output matches the expected output.

## 1. Modularizing Your Code

To test PySpark code, you can no longer write long, continuous scripts. You must wrap your transformations inside Python functions so they can be imported and tested.

```python
from pyspark.sql.functions import col

# This is the function we want to test
def filter_adults(df):
    return df.filter(col("age") >= 18)
```

## 2. Writing a PySpark Test

In a test file, we initialize a local `SparkSession`, generate mock data, and use an assertion to verify the logic.

```python
# Typically saved in a file named test_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_filter_adults():
    # 1. Setup Spark (usually done via pytest fixtures in real life)
    spark = SparkSession.builder.appName("Testing").master("local[1]").getOrCreate()
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    # 2. Create Mock Input
    input_data = [("Alice", 25), ("Bob", 15), ("Charlie", 18)]
    input_df = spark.createDataFrame(input_data, schema)
    
    # 3. Create Expected Output
    expected_data = [("Alice", 25), ("Charlie", 18)]
    expected_df = spark.createDataFrame(expected_data, schema)
    
    # 4. Run the function
    actual_df = filter_adults(input_df)
    
    # 5. Assert Equality (Check if the data matches)
    # We collect() the DataFrames into Python lists to compare them easily
    assert actual_df.collect() == expected_df.collect()
    
    print("Test Passed!")
```
