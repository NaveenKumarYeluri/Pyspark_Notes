# PySpark Learning Log: Part 28 - The Low-Level RDD API

Before PySpark introduced DataFrames (which look like SQL tables), Data Engineers used **Resilient Distributed Datasets (RDDs)**. An RDD is simply a distributed collection of objects (like a massive, partitioned Python list) without any named columns or schema.

While DataFrames are heavily optimized by Spark's Catalyst Optimizer, RDDs give you absolute, fine-grained control over your data. Modern engineers typically only drop down to the RDD API when parsing highly unstructured text or complex custom objects.

## 1. Converting Between DataFrames and RDDs

You can freely switch between the high-level DataFrame API and the low-level RDD API.

```python
# Convert a DataFrame into an RDD of Row objects
my_rdd = df.rdd

# Convert an RDD back into a DataFrame (requires a schema or column names)
new_df = my_rdd.toDF(["column_1", "column_2"])
```

## 2. RDD Transformations (`map` and `flatMap`)

Unlike DataFrames where you manipulate columns, in an RDD, you manipulate the *entire row* using Python `lambda` functions.

*   `map()`: Takes one item in, returns exactly **one** item out.
*   `flatMap()`: Takes one item in, returns **many** items out (flattens lists).

```python
# Scenario: We have an RDD of sentences
sentences_rdd = spark.sparkContext.parallelize(["Hello world", "PySpark is great"])

# map: Returns a list of lists -> [["Hello", "world"], ["PySpark", "is", "great"]]
words_nested_rdd = sentences_rdd.map(lambda sentence: sentence.split(" "))

# flatMap: Flattens it into one giant list -> ["Hello", "world", "PySpark", "is", "great"]
words_flat_rdd = sentences_rdd.flatMap(lambda sentence: sentence.split(" "))
```

## 3. Key-Value Pairs and Aggregation (`reduceByKey`)

To group and aggregate data in an RDD, you must format your data into **Key-Value pairs** (tuples where the first element is the key). Then, you use `.reduceByKey()` to define how to combine the values for matching keys.

```python
# 1. Map words to a tuple: (word, 1)
# Output: [("Hello", 1), ("world", 1), ("PySpark", 1), ...]
paired_rdd = words_flat_rdd.map(lambda word: (word, 1))

# 2. Reduce by key to add the 1s together
# 'a' is the running total, 'b' is the new value being added
word_counts_rdd = paired_rdd.reduceByKey(lambda a, b: a + b)
```
