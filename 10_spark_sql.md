# PySpark Learning Log: Part 8 - Spark SQL

Up to this point, we have been using the **DataFrame API** (e.g., `df.select()`, `df.filter()`, `df.groupBy()`). However, PySpark has a secret weapon: it allows you to query your DataFrames using standard SQL syntax. 

This is incredibly powerful if you or your teammates already know SQL, as you can seamlessly switch between Python methods and SQL queries in the same script.

## 1. Creating a Temporary View

To run SQL queries against a PySpark DataFrame, you first have to register it as a **Temporary View**. This acts like a virtual table in a database that exists only for the duration of your Spark session.

```python
# Assuming you have a DataFrame named 'flights_df'
# Register the DataFrame as a SQL table named "flights_table"
flights_df.createOrReplaceTempView("flights_table")
```

## 2. Executing SQL Queries (`spark.sql`)

Once the view is registered, you use the `spark.sql()` method to write your SQL query as a standard string. 

The beauty of `spark.sql()` is that **it returns a new PySpark DataFrame**. This means you can run a SQL query, get the results, and then immediately go back to using Python DataFrame methods on those results!

```python
# Write a standard SQL query
sql_query = """
    SELECT departure_code, AVG(ticket_price) as avg_price
    FROM flights_table
    WHERE is_international = true
    GROUP BY departure_code
    ORDER BY avg_price DESC
"""

# Execute the query (this returns a DataFrame!)
sql_results_df = spark.sql(sql_query)

# Use standard PySpark actions on the result
sql_results_df.show()
```

## 3. Why Use Spark SQL?

You might wonder why you would use SQL when the DataFrame API can do the same things.

1. **Migration:** It makes migrating legacy SQL code into PySpark incredibly easy.
2. **Readability:** Complex aggregations and window functions are sometimes much easier to read in SQL than in deeply chained Python methods.
3. **Collaboration:** Data Analysts who don't know Python can write SQL strings, and Data Engineers can wrap those strings in PySpark pipelines.

*Note: Behind the scenes, Spark's Catalyst Optimizer treats your DataFrame API code and your Spark SQL strings exactly the same. There is no performance penalty for using SQL over Python!*
