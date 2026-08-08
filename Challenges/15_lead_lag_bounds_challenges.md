# PySpark Challenge 17: The Stock Market Ticker (Advanced Windows)

**The Scenario:**

You are processing financial market data for a trading algorithm. The system receives daily closing prices for various tech stocks. 

The quantitative analysts (Quants) need a flattened dataset that shows the absolute daily price change for each stock, alongside a smoothed 3-day moving average to help identify short-term trends.

**The Setup:**

Copy this code into your environment to generate the mock DataFrame.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import date

spark = SparkSession.builder.appName("Challenge_17").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Stock Market Data
stock_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("trade_date", DateType(), True),
    StructField("close_price", DoubleType(), True)
])

stock_data = [
    ("AAPL", date(2026, 8, 1), 150.00),
    ("AAPL", date(2026, 8, 2), 155.00),
    ("AAPL", date(2026, 8, 3), 152.00),
    ("AAPL", date(2026, 8, 4), 160.00),
    ("GOOG", date(2026, 8, 1), 2800.00),
    ("GOOG", date(2026, 8, 2), 2750.00),
    ("GOOG", date(2026, 8, 3), 2780.00)
]
stock_df = spark.createDataFrame(data=stock_data, schema=stock_schema)
```

## Challenge 17 Task:

Write a single, chained PySpark pipeline to generate the `stock_analysis_df`. 

**Requirements:**

1.  **Lag Offset:** Create a new column called `prev_close`. Use a Window function to fetch the `close_price` from the *previous* chronological day for that specific `ticker`.
2.  **Column Math:** Create a new column called `daily_change` that calculates the mathematical difference between the current `close_price` and the `prev_close`. Wrap it in `round(..., 2)` to ensure clean decimals.
3.  **Moving Average:** Create a new column called `3_day_moving_avg`. Use a bounded Window function to calculate the average `close_price` over a 3-day window (the current day + the 2 previous days). Round this to 2 decimal places.
4.  **Format:** Select the `ticker`, `trade_date`, `close_price`, `daily_change`, and `3_day_moving_avg` columns. Order the final output by `ticker` (ascending) and `trade_date` (ascending).

**Expected Output (`stock_analysis_df.show()`):**

```text
+------+----------+-----------+------------+----------------+
|ticker|trade_date|close_price|daily_change|3_day_moving_avg|
+------+----------+-----------+------------+----------------+
|  AAPL|2026-08-01|      150.0|        NULL|           150.0|
|  AAPL|2026-08-02|      155.0|         5.0|           152.5|
|  AAPL|2026-08-03|      152.0|        -3.0|          152.33|
|  AAPL|2026-08-04|      160.0|         8.0|          155.67|
|  GOOG|2026-08-01|     2800.0|        NULL|          2800.0|
|  GOOG|2026-08-02|     2750.0|       -50.0|          2775.0|
|  GOOG|2026-08-03|     2780.0|        30.0|         2776.67|
+------+----------+-----------+------------+----------------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import lag, col, round, avg
from pyspark.sql.window import Window
from datetime import date

spark = SparkSession.builder.appName("Challenge_15").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Stock Market Data
stock_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("trade_date", DateType(), True),
    StructField("close_price", DoubleType(), True)
])

stock_data = [
    ("AAPL", date(2026, 8, 1), 150.00),
    ("AAPL", date(2026, 8, 2), 155.00),
    ("AAPL", date(2026, 8, 3), 152.00),
    ("AAPL", date(2026, 8, 4), 160.00),
    ("GOOG", date(2026, 8, 1), 2800.00),
    ("GOOG", date(2026, 8, 2), 2750.00),
    ("GOOG", date(2026, 8, 3), 2780.00)
]
stock_df = spark.createDataFrame(data=stock_data, schema=stock_schema)

window_lag = Window.partitionBy("ticker").orderBy("trade_date")
moving_avg_window = (
    Window
    .partitionBy("ticker")
    .orderBy("trade_date")
    .rowsBetween(-2, Window.currentRow)
)

stock_analysis_df = (
    stock_df
    .withColumn("prev_close", lag("close_price", 1).over(window_lag))
    .withColumn("daily_change", round((col("close_price") - col("prev_close")), 2))
    .withColumn("3_day_moving_avg", round(avg("close_price").over(moving_avg_window), 2))
    .select("ticker", "trade_date", "close_price", "daily_change", "3_day_moving_avg")
    .orderBy(col("ticker").asc(), col("trade_date").asc())
)
stock_analysis_df.show()
```

### My Output Verification:

```
+------+----------+-----------+------------+----------------+
|ticker|trade_date|close_price|daily_change|3_day_moving_avg|
+------+----------+-----------+------------+----------------+
|  AAPL|2026-08-01|      150.0|        NULL|           150.0|
|  AAPL|2026-08-02|      155.0|         5.0|           152.5|
|  AAPL|2026-08-03|      152.0|        -3.0|          152.33|
|  AAPL|2026-08-04|      160.0|         8.0|          155.67|
|  GOOG|2026-08-01|     2800.0|        NULL|          2800.0|
|  GOOG|2026-08-02|     2750.0|       -50.0|          2775.0|
|  GOOG|2026-08-03|     2780.0|        30.0|         2776.67|
+------+----------+-----------+------------+----------------+
```
