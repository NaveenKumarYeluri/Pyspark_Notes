# PySpark Learning Log: Part 30 - Advanced Delta Lake (MERGE & UPSERT)

In Part 25, we used `.mode("overwrite")` to completely replace a Delta table. But what if you have a massive table of 100 million customers, and today you received a daily batch of just 500 updates? 

Overwriting 100 million rows every day just to update 500 rows is incredibly expensive and slow. 
Instead, we perform an **Upsert** (Update + Insert) using Delta Lake's `MERGE` feature.

## 1. Change Data Capture (CDC)

In modern architectures, source databases (like PostgreSQL) stream out their changes daily. This is called CDC data. Your daily CDC batch will usually contain:

1.  **New Users:** (Needs to be INSERTED into the Delta table).
2.  **Updated Users:** (Needs to UPDATE the existing row in the Delta table).

## 2. The DeltaTable Merge Syntax

To do an Upsert, you must load your target table as a `DeltaTable` object, not just a standard DataFrame. Then, you define a joining condition to match your new daily data against the historical data.

```python
from delta.tables import DeltaTable

# 1. Load the historical target table
target_table = DeltaTable.forPath(spark, "/path/to/delta/customers")

# 2. Load the daily CDC batch (a standard DataFrame)
daily_updates_df = spark.read.parquet("/path/to/today/batch")

# 3. Perform the Upsert
target_table.alias("target") \
    .merge(
        daily_updates_df.alias("updates"),
        "target.customer_id = updates.customer_id" # The matching condition
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

### What does this do?

*   `.whenMatchedUpdateAll()`: If the `customer_id` already exists in the target table, it overwrites that row with the fresh data from the daily batch.
*   `.whenNotMatchedInsertAll()`: If the `customer_id` from the daily batch is completely new, it inserts it as a brand new row at the bottom of the target table.
