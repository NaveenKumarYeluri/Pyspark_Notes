# PySpark Learning Log: Part 22 - Advanced Aggregations (Rollup and Cube)

Standard `.groupBy()` operations are great for finding totals at a specific grain. However, Business Intelligence (BI) dashboards often require hierarchical subtotals (e.g., total sales by Country, AND total sales by State, AND a Grand Total).

Instead of writing three separate `groupBy` DataFrames and using `union` to stack them together, PySpark provides `.rollup()` and `.cube()`.

## 1. Rollup (Hierarchical Subtotals)

`.rollup()` calculates subtotals based on the *hierarchy* of the columns you provide, reading from left to right, plus a grand total.

```python
from pyspark.sql.functions import sum

# Assuming columns: "country", "state", "city"
df.rollup("country", "state").agg(sum("sales"))
```

This generates:

1. Totals grouped by `Country` + `State`.
2. Subtotals grouped by `Country` (where `State` will be `null`).
3. A Grand Total (where both `Country` and `State` will be `null`).

## 2. Cube (All Combinations)

`.cube()` takes it a step further. It calculates subtotals for *every single possible combination* of the columns provided, regardless of hierarchy.

```python
df.cube("country", "state").agg(sum("sales"))
```

This generates:

1. Totals grouped by `Country` + `State`.
2. Subtotals grouped by `Country` (where `State` is `null`).
3. **Subtotals grouped by `State` (where `Country` is `null`).** -> *This is the difference from Rollup!*
4. A Grand Total (where both `Country` and `State` will be `null`).

## 3. Cleaning the Aggregation Nulls

Because `rollup` and `cube` generate `null` values in the grouping columns to represent "All" or "Total", you typically need to clean these up using `.na.fill()` or `coalesce()` so the final report reads cleanly to stakeholders.
