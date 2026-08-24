# PySpark Learning Log: Bonus Part 31 - Delta Lake Maintenance

In Part 25 and Part 30, we learned that Delta Lake creates new versions of your data every time you `MERGE`, `UPDATE`, or `OVERWRITE`. It achieves this by writing new Parquet files while leaving the old ones intact (allowing for Time Travel).

However, this creates two massive problems in production over time:

1.  **The Small File Problem:** Frequent CDC merges or streaming appends create thousands of tiny kilobytes-sized Parquet files. Spark is optimized to read large files (128MB - 1GB). Reading 10,000 tiny files will make your queries painfully slow.
2.  **Storage Costs:** If you update a 1TB table every day for a year, Delta Lake will store 365TB of historical data on your cloud bill. 

We solve these using Delta's built-in maintenance commands.

## 1. OPTIMIZE (Compaction)

The `OPTIMIZE` command reads all the tiny fragmented Parquet files in your Delta table and squashes them together into perfectly sized, large Parquet files. This drastically improves read performance.

```python
from delta.tables import DeltaTable

# Target the Delta table
delta_table = DeltaTable.forPath(spark, "/path/to/table")

# Compact the small files into large files
delta_table.optimize().executeCompaction()
```

## 2. VACUUM (Data Retention)

The `VACUUM` command permanently deletes old historical files that are no longer needed, saving you massive amounts of money on AWS S3 or Azure Blob storage. 

By default, `VACUUM` deletes files older than 7 days (168 hours) to prevent accidentally breaking running queries. *Note: Once you vacuum, you can no longer Time Travel back to those deleted versions!*

```python
# Delete historical Parquet files older than 168 hours (7 days)
delta_table.vacuum(168)

# DANGER: To vacuum files created recently (e.g., 0 hours ago for testing), 
# you must bypass the safety check first:
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
delta_table.vacuum(0)
```
