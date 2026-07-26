# PySpark Challenge 4: Manipulations and Aggregations

**The Scenario:**

You have the following DataFrame `df` built from the mock data in Part 3.

```python
# Assuming this DataFrame is already created:
# df = spark.createDataFrame(data=mock_data, schema=flight_schema)
```

Your data engineering manager has asked you to create a summary report of international vs. domestic flights.

**Your Task:**

Write a single chained PySpark command that does the following in order:

1. Modifies the DataFrame by adding a $25 processing fee to the existing `ticket_price` column using `withColumn()`.
2. Renames the `is_international` column to `flight_type`.
3. Groups the data by `flight_type`.
4. Calculates the **maximum** (`max`) `ticket_price` for both flight types.
5. Aliases the new aggregated column as `max_price`.
6. Executes the transformation to display the results on the screen.

*(Hint: Don't forget to import any necessary aggregate functions from `pyspark.sql.functions`!)*

### My Solution:

```python
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, max
    from pyspark.sql.types import (
        LongType,
        StringType,
        BooleanType,
        DoubleType,
        StructType,
        StructField,
    )

    # Sparksession
    spark = SparkSession.builder.appName("Challenge_2").master("local[*]").getOrCreate()

    # Define the schema explicitly
    flight_schema = StructType(
        [
            StructField("record_id", LongType(), True),
            StructField("departure_code", StringType(), True),
            StructField("ticket_price", DoubleType(), True),
            StructField("is_international", BooleanType(), True),
        ]
    )

    # 1. Create the raw data using standard Python lists and tuples
    mock_data = [
        (84729481, "LAX", 450.75, False),
        (84729482, "JFK", 1250.00, True),
        (84729483, "ORD", 299.99, False),
        (84729484, "LHR", 890.50, True),
    ]

    df = spark.createDataFrame(data=mock_data, schema=flight_schema)

    mod_df = (
        df.withColumn("ticket_price", col("ticket_price") + 25)
        .withColumnRenamed("is_international", "flight_type")
        .groupBy("flight_type")
        .agg(max("ticket_price").alias("max_price"))
    )
    mod_df.show()
```

### My Output Verification:

```
    +-----------+---------+
    |flight_type|max_price|
    +-----------+---------+
    |       true|   1275.0|
    |      false|   475.75|
    +-----------+---------+
```
