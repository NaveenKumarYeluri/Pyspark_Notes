# PySpark Learning Log: Bonus Part 33 - Handling Corrupt Records

In the real world, upstream data systems (especially legacy ones exporting CSV or JSON) will occasionally send you garbage data. You might have a strict schema expecting an `Integer` for an `age` column, but a user typed `"TWENTY"`.

By default, if PySpark hits a row that violates your explicit schema, it behaves according to the `mode` option in the `.read` command.

## 1. The Three Read Modes

When reading data, you can specify `option("mode", "<MODE>")`:

* **`PERMISSIVE` (The Default):** PySpark tries to parse the row. If a column's data type doesn't match the schema, it inserts a `null` for that specific column and tries to salvage the rest of the row. 
* **`DROPMALFORMED`:** If a row has *any* data type mismatches or schema violations, PySpark throws the entire row in the trash and does not load it into your DataFrame.
* **`FAILFAST`:** If PySpark encounters even a single broken row, it immediately crashes the entire job and throws a massive error. (Best used for critical financial data where 100% accuracy is required).

```python
# Drops any rows that don't perfectly match the schema
clean_df = spark.read \
    .schema(my_strict_schema) \
    .option("mode", "DROPMALFORMED") \
    .csv("path/to/data.csv")
```

## 2. The `_corrupt_record` Column (Quarantine)

If you use `PERMISSIVE` mode, you often still want to know *which* rows were broken so you can send a report back to the source team. 

PySpark allows you to add a special string column to your schema (traditionally named `_corrupt_record`). If a row is broken, PySpark will dump the entire raw, unparsed string of that broken row into this column.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Add the special _corrupt_record column to the end of your schema
quarantine_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True) # The Quarantine zone
])

# 2. Tell PySpark to use this column for broken records
df = spark.read \
    .schema(quarantine_schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("path/to/data.csv")

# 3. Filter to find the bad data!
bad_data_df = df.filter(col("_corrupt_record").isNotNull())
```
