# PySpark Challenge 10a: Conditional Operations

**The Setup:**

Copy the following code into your notebook to generate our mock DataFrame of customer orders.

```python
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark.sql.functions import col, when

# Schema
order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("items_purchased", LongType(), True),
    StructField("order_total", DoubleType(), True)
])

# Mock Data
order_data = [
    (1, 2, 45.00),
    (2, 5, 120.00),
    (3, 1, 15.00),
    (4, 12, 350.50),
    (5, 8, 210.25)
]

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)
orders_df.show()
```

---

## Challenge 10aa: The Shipping Calculator

**Task:** 

The logistics team needs you to assign a "shipping_priority" and calculate a "final_total" for each order based on specific business rules.

Write a single chained PySpark command that does the following:

1. **Create a `shipping_priority` column:**

   * If `order_total` is strictly greater than `200`, it should be `"Express"`.
   * If `order_total` is between `50` and `200` (inclusive), it should be `"Standard"`.
   * Otherwise, it should be `"Economy"`.
   
2. **Create a `discounted_total` column:**

   * If they bought `10` or more items (`items_purchased`), give them a flat `$50` discount off their `order_total`.
   * Otherwise, their total remains exactly the same as the original `order_total`.

3. **Format:**

Show the final DataFrame to verify your logic worked.

### My Solution:

```python
# PySpark Challenge 10a: Conditional Operations
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("challenge_8a").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema
order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("items_purchased", LongType(), True),
    StructField("order_total", DoubleType(), True)
])

# Mock Data
order_data = [
    (1, 2, 45.00),
    (2, 5, 120.00),
    (3, 1, 15.00),
    (4, 12, 350.50),
    (5, 8, 210.25)
]

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)

orders_df = (
    orders_df
    .withColumn("shipping_priority", (
        when(col("order_total") > 200, "Express")
        .when((col("order_total") >= 50) & (col("order_total") <= 200), "Standard")
        ).otherwise("Economy")
    )
    .withColumn("discounted_total", (
        when(col("items_purchased") > 10, col("order_total")-50)
        .otherwise(col("order_total"))
        )
    )
)
orders_df.show()
```

### My Output Verification:

```
+--------+---------------+-----------+-----------------+----------------+
|order_id|items_purchased|order_total|shipping_priority|discounted_total|
+--------+---------------+-----------+-----------------+----------------+
|       1|              2|       45.0|          Economy|            45.0|
|       2|              5|      120.0|         Standard|           120.0|
|       3|              1|       15.0|          Economy|            15.0|
|       4|             12|      350.5|          Express|           300.5|
|       5|              8|     210.25|          Express|          210.25|
+--------+---------------+-----------+-----------------+----------------+
```

---

# PySpark Challenge 10b: Conditional Logic (Easy to Complex)

**The Setup:**
Copy the following code into your notebook to generate our mock DataFrame of customer orders.

```python
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql.functions import col, when

# Schema
order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_type", StringType(), True),
    StructField("items_purchased", LongType(), True),
    StructField("order_total", DoubleType(), True)
])

# Mock Data
order_data = [
    (1, "Retail", 2, 45.00),
    (2, "Wholesale", 50, 1200.00),
    (3, "Retail", 1, 15.00),
    (4, "Retail", 12, 350.50),
    (5, "Wholesale", 8, 210.25),
    (6, "Retail", 5, 200.00)
]

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)
orders_df.show()
```

---

## Challenge 10ba: The Basic Tier (Easy)

**Task:**

Create a new DataFrame based on `orders_df`. Add a column called `order_size`.

* If `items_purchased` is less than `5`, the value should be `"Small"`.
* Otherwise, the value should be `"Large"`.

## Challenge 10bb: The Tiered Shipping Calculator (Medium)

**Task:** 

Create a new DataFrame. Add a column called `shipping_priority`. You must chain multiple `.when()` statements.

* If `order_total` is strictly greater than `500`, it should be `"Express"`.
* If `order_total` is between `100` and `500` (inclusive), it should be `"Standard"`.
* Otherwise, it should be `"Economy"`.

## Challenge 10bc: The Complex Discount Engine (Boss Level)

**Task:**

The sales team wants to apply targeted discounts. Write a single chained PySpark command that does the following:

1.  **Add a `discount_applied` flag (Boolean):**
    *   Set this to `True` **IF** the `customer_type` is `"Wholesale"` **AND** the `order_total` is greater than or equal to `500`.
    *   **OR** set this to `True` **IF** the `items_purchased` is greater than `10`.
    *   Otherwise, set it to `False`.
2.  **Add a `final_total` column (Math):**
    *   If `discount_applied` is `True`, multiply the `order_total` by `0.80` (a 20% discount).
    *   Otherwise, return the original `order_total`.
3.  **Format:** Filter the DataFrame to only show orders where `discount_applied` is `True`, and select only the `order_id`, `customer_type`, `order_total`, and `final_total` columns.

### My Solution:

```python
# PySpark Challenge 8b: Conditional Logic (Easy to Complex)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("challenge_8b").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema
order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_type", StringType(), True),
    StructField("items_purchased", LongType(), True),
    StructField("order_total", DoubleType(), True)
])

# Mock Data
order_data = [
    (1, "Retail", 2, 45.00),
    (2, "Wholesale", 50, 1200.00),
    (3, "Retail", 1, 15.00),
    (4, "Retail", 12, 350.50),
    (5, "Wholesale", 8, 210.25),
    (6, "Retail", 5, 200.00)
]

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)
print("Original Orders DF:")
orders_df.show()

## Challenge 10ba: The Basic Tier (Easy)
order_size_df = (
    orders_df
    .withColumn("order_size", when(col("items_purchased") < 5, "Small").otherwise("Large"))
)
print("Order Size:")
order_size_df.show()


## Challenge 10bb: The Tiered Shipping Calculator (Medium)
shipping_priority_df = (
    orders_df
    .withColumn("shipping_priority", (
            when(col("order_total") > 500, "Express")
            .when((col("order_total") >= 100) & (col("order_total") <= 500), "Standard")
            .otherwise("Economy")
        )
    )
)
print("Shipping Priority:")
shipping_priority_df.show()

## Challenge 10bc: The Complex Discount Engine (Boss Level)
orders_df = (
    orders_df
    .withColumn(
        "discount_applied", when
            (
                (col("customer_type") == "Wholesale") & (col("order_total") > 500), True
            )
            .when(
                col("items_purchased") > 10, True
            )
            .otherwise(False)
    )
    .withColumn(
        "final_total", when
            (
                col("discount_applied") == True, col("order_total") * 0.8
            )
            .otherwise(col("order_total"))
    )
)
final_orders_df = (
    orders_df
    .filter(col("discount_applied") == True)
    .select(col("order_id"), col("customer_type"), col("order_total"), col("final_total"))
)
print("Discount Engine:")
final_orders_df.show()
```

### My Output Verification:

```
Original Orders DF:
+--------+-------------+---------------+-----------+
|order_id|customer_type|items_purchased|order_total|
+--------+-------------+---------------+-----------+
|       1|       Retail|              2|       45.0|
|       2|    Wholesale|             50|     1200.0|
|       3|       Retail|              1|       15.0|
|       4|       Retail|             12|      350.5|
|       5|    Wholesale|              8|     210.25|
|       6|       Retail|              5|      200.0|
+--------+-------------+---------------+-----------+

Order Size:
+--------+-------------+---------------+-----------+----------+
|order_id|customer_type|items_purchased|order_total|order_size|
+--------+-------------+---------------+-----------+----------+
|       1|       Retail|              2|       45.0|     Small|
|       2|    Wholesale|             50|     1200.0|     Large|
|       3|       Retail|              1|       15.0|     Small|
|       4|       Retail|             12|      350.5|     Large|
|       5|    Wholesale|              8|     210.25|     Large|
|       6|       Retail|              5|      200.0|     Large|
+--------+-------------+---------------+-----------+----------+

Shipping Priority:
+--------+-------------+---------------+-----------+-----------------+
|order_id|customer_type|items_purchased|order_total|shipping_priority|
+--------+-------------+---------------+-----------+-----------------+
|       1|       Retail|              2|       45.0|          Economy|
|       2|    Wholesale|             50|     1200.0|          Express|
|       3|       Retail|              1|       15.0|          Economy|
|       4|       Retail|             12|      350.5|         Standard|
|       5|    Wholesale|              8|     210.25|         Standard|
|       6|       Retail|              5|      200.0|         Standard|
+--------+-------------+---------------+-----------+-----------------+

Discount Engine:
+--------+-------------+-----------+------------------+
|order_id|customer_type|order_total|       final_total|
+--------+-------------+-----------+------------------+
|       2|    Wholesale|     1200.0|             960.0|
|       4|       Retail|      350.5|280.40000000000003|
+--------+-------------+-----------+------------------+
```
