# PySpark Challenge 33: The Quarantine Zone (Boss Level)

**The Scenario:**

You are ingesting a daily batch of user profiles from a legacy third-party marketing system. The data is notoriously dirty. 

Your manager has requested that you ingest the data using a strict schema. You must capture the valid records for the analytics team, but you must also isolate the broken records into a separate "Quarantine" DataFrame so the data governance team can investigate them.

**The Setup:**

Run this code to generate a messy CSV file on your local disk containing deliberate schema violations.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("Challenge_33").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create a dirty CSV file
file_path = "dirty_profiles.csv"
csv_data = """id,name,age
1,Alice,25
2,Bob,TWENTY
3,Charlie,30
FOUR,David,40
5,Eve,22"""

with open(file_path, "w") as f:
    f.write(csv_data)

print("Dirty CSV created on disk!")
```

## Challenge 33 Task:

Write a PySpark script to separate the good data from the bad data.

**Requirements:**

1. Define a strict schema named `profile_schema` with the following structure:
    * `id` (IntegerType)
    * `name` (StringType)
    * `age` (IntegerType)
    * `_corrupt_record` (StringType)
2. Read the `"dirty_profiles.csv"` file into a DataFrame named `raw_df`. 
    * You must apply your `profile_schema`.
    * Ensure you set `header=True`.
    * Set the `mode` to `"PERMISSIVE"`.
    * Use the `"columnNameOfCorruptRecord"` option to map to your `_corrupt_record` column.
3. Create a `clean_df` that contains ONLY the rows where `_corrupt_record` is `null`. Drop the `_corrupt_record` column from this final clean DataFrame.
4. Create a `quarantine_df` that contains ONLY the rows where `_corrupt_record` is NOT `null`.
5. Show both DataFrames!

**Expected Output (`clean_df` and `quarantine_df`):**

```text
Clean Data:
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  1|  Alice| 25|
|  3|Charlie| 30|
|  5|    Eve| 22|
+---+-------+---+

Quarantined Data:
+----+-----+----+---------------+
|  id| name| age|_corrupt_record|
+----+-----+----+---------------+
|   2|  Bob|NULL|   2,Bob,TWENTY|
|NULL|David|  40|  FOUR,David,40|
+----+-----+----+---------------+
```
