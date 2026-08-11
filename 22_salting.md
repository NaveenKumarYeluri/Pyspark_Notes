# PySpark Learning Log: Part 20 - Handling Data Skew (Salting)

In distributed computing, data is split into partitions and sent to different worker nodes. A **Shuffle** happens during Joins or GroupBys, where PySpark sends all rows with the *same key* to the *same worker node*.

**Data Skew** occurs when your data is unevenly distributed. For example, if you are joining web logs by `ip_address`, and 95% of your traffic comes from a single bot IP, one worker node will receive 95% of the data. This creates a "straggler" task that takes hours to finish, or crashes entirely with an Out-Of-Memory (OOM) error, while all other workers sit idle.

## The Solution: Salting

To fix this, we "salt" the skewed key. Salting means appending a random number to the key to artificially break it into multiple smaller keys. This distributes the massive chunk of data across multiple worker nodes.

### Step 1: Salt the Skewed Fact Table

Add a random integer (e.g., 0 to 4) to the join key in the massive table.

```python
from pyspark.sql.functions import col, lit, rand, round, concat

# Create a random salt between 0 and 4, then append it to the join key
salt_df = massive_df.withColumn(
    "salted_key", 
    concat(col("skewed_key"), lit("_"), round(rand() * 4).cast("int"))
)
# "Bot_IP" becomes "Bot_IP_0", "Bot_IP_1", etc.
```

### Step 2: Explode the Dimension Table

Because the fact table now has salted keys (e.g., `Bot_IP_0` through `Bot_IP_4`), the smaller lookup table must have matching keys for *every* possible salt value so the join succeeds.

```python
from pyspark.sql.functions import array, explode, sequence

# 1. Create an array [0, 1, 2, 3, 4] using sequence()
# 2. Explode it so every lookup row is duplicated 5 times
# 3. Concatenate the key with the exploded salt
exploded_dim_df = tiny_df \
    .withColumn("salt_array", sequence(lit(0), lit(4))) \
    .withColumn("salt_val", explode(col("salt_array"))) \
    .withColumn("salted_key", concat(col("lookup_key"), lit("_"), col("salt_val")))
```

### Step 3: Join and Aggregate

Now, you join on the new `salted_key`. The data is processed in parallel without crashing. Finally, you group by the *original* real key to get your final aggregate.

```python
# The join is now perfectly distributed!
distributed_df = salt_df.join(exploded_dim_df, on="salted_key", how="inner")

# Group by the real key to consolidate the salted pieces
final_df = distributed_df.groupBy("original_key").agg(sum("sales"))
```
