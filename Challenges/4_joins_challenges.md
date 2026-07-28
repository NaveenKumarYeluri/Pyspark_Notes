# PySpark Challenge 6: Joins

**The Setup:**

Copy the following code into your notebook. This creates two DataFrames: one containing flight records and another containing airport reference data.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType

# 1. Flights Data
flight_schema = StructType([
    StructField("flight_id", LongType(), True),
    StructField("destination", StringType(), True)
])
flight_data = [
    (101, "LAX"),
    (102, "JFK"),
    (103, "XYZ"), # Notice XYZ does not exist in the airports data!
    (104, "ORD")
]
flights_df = spark.createDataFrame(data=flight_data, schema=flight_schema)

# 2. Airports Data
airport_schema = StructType([
    StructField("airport_code", StringType(), True),
    StructField("city_name", StringType(), True)
])
airport_data = [
    ("LAX", "Los Angeles"),
    ("JFK", "New York"),
    ("ORD", "Chicago"),
    ("MIA", "Miami") # Notice MIA does not have any flights going to it!
]
airports_df = spark.createDataFrame(data=airport_data, schema=airport_schema)

print("Flights:")
flights_df.show()
print("Airports:")
airports_df.show()
```

---

## Challenge 6a: The Inner Join

**Task:** 

1. Perform an `inner` join between `flights_df` and `airports_df`. 
2. You will need to use the explicit condition syntax because the column names are different (`destination` vs `airport_code`).
3. Show the resulting DataFrame. Notice which records are dropped!

## Challenge 6b: The Left Join & Clean (Complex)

**Task:** 

Write a chained PySpark command that does the following in order:

1. Performs a `left` join, keeping ALL records from `flights_df` and bringing in data from `airports_df` based on the destination code matching the airport code.
2. Drops the redundant `airport_code` column that comes over from the right table.
3. Uses `na.fill()` to replace any `null` values in the new `city_name` column with the string `"Unknown City"`.
4. Shows the resulting DataFrame.


### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, StringType

spark = SparkSession.builder \
    .appName("challenge_4") \
    .master("local[*]") \
    .getOrCreate()

# 1. Flights Data
flight_schema = StructType([
    StructField("flight_id", LongType(), True),
    StructField("destination", StringType(), True)
])
flight_data = [
    (101, "LAX"),
    (102, "JFK"),
    (103, "XYZ"), # Notice XYZ does not exist in the airports data!
    (104, "ORD")
]
flights_df = spark.createDataFrame(data=flight_data, schema=flight_schema)

# 2. Airports Data
airport_schema = StructType([
    StructField("airport_code", StringType(), True),
    StructField("city_name", StringType(), True)
])
airport_data = [
    ("LAX", "Los Angeles"),
    ("JFK", "New York"),
    ("ORD", "Chicago"),
    ("MIA", "Miami") # Notice MIA does not have any flights going to it!
]
airports_df = spark.createDataFrame(data=airport_data, schema=airport_schema)

print("Flights:")
flights_df.show()
print("Airports:")
airports_df.show()

challenge_6a = (flights_df.join(
    airports_df,
    flights_df["destination"] == airports_df["airport_code"],
    how="inner")
)
print("Challenge 6a:")
challenge_6a.show()

challenge_6b = (flights_df.join(
    airports_df,
    flights_df["destination"] == airports_df["airport_code"],
    how="left")
    .drop(col("airport_code"))
    .na.fill({
        "city_name": "Unknown City"
    })
)
print("Challenge 6b:")
challenge_6b.show()
```

### My Output Verification:

```
Flights:
+---------+-----------+
|flight_id|destination|
+---------+-----------+
|      101|        LAX|
|      102|        JFK|
|      103|        XYZ|
|      104|        ORD|
+---------+-----------+

Airports:
+------------+-----------+
|airport_code|  city_name|
+------------+-----------+
|         LAX|Los Angeles|
|         JFK|   New York|
|         ORD|    Chicago|
|         MIA|      Miami|
+------------+-----------+

Challenge 6a:
+---------+-----------+------------+-----------+
|flight_id|destination|airport_code|  city_name|
+---------+-----------+------------+-----------+
|      102|        JFK|         JFK|   New York|
|      101|        LAX|         LAX|Los Angeles|
|      104|        ORD|         ORD|    Chicago|
+---------+-----------+------------+-----------+

Challenge 6b:
+---------+-----------+------------+
|flight_id|destination|   city_name|
+---------+-----------+------------+
|      101|        LAX| Los Angeles|
|      102|        JFK|    New York|
|      103|        XYZ|Unknown City|
|      104|        ORD|     Chicago|
+---------+-----------+------------+
```
