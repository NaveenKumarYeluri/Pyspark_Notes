# PySpark Learning Log: Part 6 - Joins

In data engineering, your data is rarely perfectly packaged in a single table. You usually have to combine data from multiple sources. If you are familiar with SQL `JOIN` or Pandas `pd.merge()`, PySpark joins will feel very familiar.

## 1. The PySpark Join Syntax

To combine two DataFrames, you use the `.join()` method on the left DataFrame. It takes three primary arguments:

1. The right DataFrame.
2. The column(s) to join on.
3. The type of join (the `how` parameter: `"inner"`, `"left"`, `"right"`, `"outer"`).

```python
# Standard Syntax
# joined_df = df1.join(df2, on="matching_column_name", how="join_type")
```

## 2. Inner Joins (The Default)

An inner join returns ONLY the rows where there is a match in **both** DataFrames. If you don't specify the `how` parameter, PySpark performs an inner join by default.

```python
# Assuming we have flights_df and airports_df that both share an 'airport_code' column
inner_joined_df = flights_df.join(airports_df, on="airport_code", how="inner")
inner_joined_df.show()
```

## 3. Left Joins

A left join returns **ALL** the rows from the left DataFrame (`flights_df`), and any matching rows from the right DataFrame (`airports_df`). If the right DataFrame doesn't have a match, PySpark fills those new columns with `null`.

```python
left_joined_df = flights_df.join(airports_df, on="airport_code", how="left")
left_joined_df.show()
```

## 4. Joining on Different Column Names

Sometimes the columns you want to join on have different names in each DataFrame (e.g., `departure_code` in the flights table, but `airport_id` in the airports table). 

Instead of passing a string to the `on` parameter, you pass a condition evaluating the specific columns.

```python
from pyspark.sql.functions import col

# Join where flights_df.departure_code == airports_df.airport_id
complex_join_df = flights_df.join(
    airports_df, 
    flights_df["departure_code"] == airports_df["airport_id"], 
    how="left"
)
```
*Note: When joining on differently named columns using this explicit syntax, PySpark will keep BOTH columns in the resulting DataFrame, so you might need to use `.drop()` to remove the duplicate column later.*
