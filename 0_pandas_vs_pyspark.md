# 0_pandas_vs_pyspark.md
# Pandas vs. PySpark Syntax Translation

While Pandas operates in single-node memory (RAM) and PySpark distributes execution across a cluster (JVM), their syntax structures are highly similar. This table provides direct 1:1 translations for common data engineering transformations.

| Operation | Pandas (Single Node Python) | PySpark (Distributed Cluster) |
| :--- | :--- | :--- |
| **Engine Initialization** | `import pandas as pd` | `spark = SparkSession.builder.getOrCreate()` |
| **Read CSV** | `df = pd.read_csv("file.csv")` | `df = spark.read.option("header", True).csv("file.csv")` |
| **Write Parquet** | `df.to_parquet("file.parquet")` | `df.write.mode("overwrite").parquet("file.parquet")` |
| **Select Columns** | `df[['name', 'age']]` | `df.select("name", "age")` |
| **Drop Column** | `df.drop(columns=['token'])` | `df.drop("token")` |
| **Filter Rows** | `df[df['age'] >= 18]` | `df.filter(F.col("age") >= 18)` |
| **Filter by List** | `df[df['status'].isin(['A', 'B'])]` | `df.filter(F.col("status").isin(["A", "B"]))` |
| **Create/Mutate Column** | `df['tax'] = df['price'] * 0.2` | `df = df.withColumn("tax", F.col("price") * 0.2)` |
| **Rename Column** | `df.rename(columns={'old': 'new'})` | `df.withColumnRenamed("old", "new")` |
| **Conditional Logic**| `np.where(df['age'] > 18, 'Y', 'N')` | `F.when(F.col("age") > 18, 'Y').otherwise('N')` |
| **Type Casting** | `df['age'].astype('float64')` | `F.col("age").cast("double")` |
| **Drop Nulls** | `df.dropna(subset=['email'])` | `df.dropna(subset=["email"])` |
| **Fill Nulls** | `df['age'].fillna(0)` | `df.fillna({"age": 0})` |
| **Aggregation (Group By)**| `df.groupby('region').agg(total=('sales', 'sum'))`| `df.groupBy("region").agg(F.sum("sales").alias("total"))` |
| **String Replace** | `df['col'].str.replace('old', 'new')` | `F.regexp_replace(F.col("col"), "old", "new")` |
| **Sort / Order By** | `df.sort_values(by='age', ascending=False)` | `df.orderBy(F.col("age").desc())` |
| **Join Tables** | `pd.merge(df1, df2, on='id', how='left')` | `df1.join(df2, on="id", how="left")` |
| **Window: Shift/Lag** | `df.groupby('id')['val'].shift(1)` | `F.lag("val", 1).over(Window.partitionBy("id").orderBy("date"))` |
| **Window: Running Total** | `df.groupby('id')['val'].cumsum()` | `F.sum("val").over(Window.partitionBy("id").orderBy("date"))` |

### Key Differences to Remember:
1. **Immutability:** PySpark DataFrames are strictly immutable. You must always reassign the variable (`df = df.withColumn(...)`). You cannot perform "in-place" mutations like `df.drop(inplace=True)`.
2. **Lazy Evaluation:** PySpark will not execute a single transformation until you call an "Action" (like `.show()`, `.count()`, or `.write`). It waits to build the most efficient execution plan possible. Pandas executes every line of code instantly.
3. **The `F.col()` wrapper:** In Pandas, you access vectors directly via string keys (`df['column']`). In PySpark, you must explicitly wrap strings in the `col()` function when doing math or logical evaluations so the Catalyst engine knows you are referencing a distributed vector.
