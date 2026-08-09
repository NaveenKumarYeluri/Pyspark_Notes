# PySpark Learning Log: Part 18 - Data Reshaping (Pivot and Unpivot)

In Data Engineering, you will constantly receive data in the "wrong" shape. 
Sometimes data is too wide (many columns, e.g., a column for every month of the year), and you need to make it "long" to filter or aggregate it properly. Other times, the data is long, and business stakeholders want it "wide" so they can read it like a cross-tab report.

PySpark handles this using Pivot and Unpivot operations.

## 1. Pivoting (Long to Wide)

Pivoting takes unique values from a single column and turns them into *multiple new columns*. It is always used in conjunction with a `.groupBy()` and an aggregation.

```python
from pyspark.sql.functions import sum

# Scenario: We have a long table of sales by store and month.
# We want ONE row per store, with a column for each month's total sales.

# 1. Group by the column you want to keep as rows (store)
# 2. Pivot on the column whose values will become the new columns (month)
# 3. Aggregate the data that goes inside the new columns (sales)
pivoted_df = df.groupBy("store").pivot("month").agg(sum("sales"))

# It is good practice to fill nulls after a pivot, as missing combos become null
clean_pivoted_df = pivoted_df.na.fill(0)
```

## 2. Unpivoting / Melting (Wide to Long)

Unpivoting is the reverse. It takes multiple columns and collapses them into two new columns: a **Key** column (holding the old column names) and a **Value** column (holding the data).

While Spark 3.4+ introduced a native `.unpivot()` / `.melt()` method, the most universally compatible and powerful way to unpivot in PySpark is by using the SQL `stack` expression via the `expr()` function.

```python
from pyspark.sql.functions import expr

# Scenario: We have a wide table: | store | Jan_Sales | Feb_Sales | Mar_Sales |
# We want a long table: | store | month | sales |

# The `stack` string format:
# "stack(<number_of_columns_being_unpivoted>, 'Label1', Col1, 'Label2', Col2, ...) AS (KeyColName, ValueColName)"

unpivot_expr = """
    stack(3, 
        'Jan', Jan_Sales, 
        'Feb', Feb_Sales, 
        'Mar', Mar_Sales
    ) AS (month, sales)
"""

# We select the columns we want to keep normally, plus the unpivot expression
long_df = df.select("store", expr(unpivot_expr))
```

*Note: Unpivoting is an extremely common requirement when ingesting Excel reports or surveys into a proper data warehouse schema!*
