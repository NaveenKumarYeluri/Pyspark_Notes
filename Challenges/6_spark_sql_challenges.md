# PySpark Challenge 8: Spark SQL

**The Setup:**

Copy the following code into your notebook to generate our mock DataFrame.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# Schema
order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_type", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("total_spent", DoubleType(), True)
])

# Mock Data
order_data = [
    (1, "Premium", "Electronics", 1200.50),
    (2, "Standard", "Clothing", 45.99),
    (3, "Premium", "Clothing", 75.00),
    (4, "Standard", "Electronics", 800.00),
    (5, "Premium", "Electronics", 2500.00)
]

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)
orders_df.show()
```

---

## Challenge 8a: Register the View

**Task:** 

Write the PySpark command to register `orders_df` as a temporary SQL view named `"ecommerce_orders"`.

## Challenge 8b: The SQL Query

**Task:** 

Write a Python variable containing a multiline SQL query string, pass it into `spark.sql()`, and show the results.

Your SQL query must do the following:

1. Select the `customer_type`.
2. Calculate the total sum of `total_spent` for each customer type (alias this as `total_revenue`).
3. Group the results by `customer_type`.
4. Order the final results by `total_revenue` in descending order.

*(Hint: Treat "ecommerce_orders" exactly like a real SQL table in your FROM clause).*

### My Solution:

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("challenge_6").master("local[*]").getOrCreate()

# Schema
order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_type", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("total_spent", DoubleType(), True)
])

# Mock Data
order_data = [
    (1, "Premium", "Electronics", 1200.50),
    (2, "Standard", "Clothing", 45.99),
    (3, "Premium", "Clothing", 75.00),
    (4, "Standard", "Electronics", 800.00),
    (5, "Premium", "Electronics", 2500.00)
]

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)
orders_df.show()


## Challenge 8a: Register the View
orders_df.createOrReplaceTempView("ecommerce_orders")

## Challenge 8b: The SQL Query
my_sql_query = """
    SELECT customer_type, SUM(total_spent) AS total_revenue
    FROM ecommerce_orders
    GROUP BY customer_type
    ORDER BY total_revenue DESC
"""

sql_query_df = spark.sql(my_sql_query)
sql_query_df.show()
```

### My Output Verification:

```
+--------+-------------+----------------+-----------+
|order_id|customer_type|product_category|total_spent|
+--------+-------------+----------------+-----------+
|       1|      Premium|     Electronics|     1200.5|
|       2|     Standard|        Clothing|      45.99|
|       3|      Premium|        Clothing|       75.0|
|       4|     Standard|     Electronics|      800.0|
|       5|      Premium|     Electronics|     2500.0|
+--------+-------------+----------------+-----------+

+-------------+-------------+
|customer_type|total_revenue|
+-------------+-------------+
|      Premium|       3775.5|
|     Standard|       845.99|
+-------------+-------------+
```
