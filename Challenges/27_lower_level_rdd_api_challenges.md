# PySpark Challenge 28: The Legacy Log Parser (RDD Boss Level)

**The Scenario:**

You have inherited a legacy Spark pipeline. A cybersecurity system dumps raw, unstructured server logs into a text file. Each line contains a mix of IP addresses, error codes, and system messages. 

The security team needs to know the exact frequency of every unique word/token across all the logs to detect sudden spikes in specific error codes (like "404" or "TIMEOUT"). Because this is purely unstructured text, you decide the RDD API is the best tool for the job.

**The Setup:**

Run this code to initialize Spark and load a raw Python list directly into a distributed RDD using `sparkContext.parallelize()`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Challenge_28").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Raw, unstructured log data
log_data = [
    "ERROR 404 SERVER_TIMEOUT IP_192",
    "INFO 200 USER_LOGIN IP_192",
    "ERROR 500 DB_CRASH IP_10",
    "ERROR 404 SERVER_TIMEOUT IP_192",
    "WARNING 403 UNAUTHORIZED IP_10",
    "ERROR 500 DB_CRASH IP_192"
]

# Create a raw RDD directly from the Python list
logs_rdd = spark.sparkContext.parallelize(log_data)
```

## Challenge 28 Task:

Write a PySpark RDD pipeline to perform a distributed word count, then convert it back to a DataFrame for clean reporting.

**Requirements:**

1.  **Split (`flatMap`):** Use `.flatMap()` and a lambda function to split each string in `logs_rdd` by spaces (`" "`), turning the dataset into a single flattened RDD of individual words.
2.  **Pair (`map`):** Use `.map()` to convert each word into a key-value tuple where the word is the key and `1` is the value (e.g., `("ERROR", 1)`).
3.  **Aggregate (`reduceByKey`):** Use `.reduceByKey()` to sum up the counts for each unique word.
4.  **Convert (`toDF`):** Convert the aggregated RDD back into a PySpark DataFrame. Name the columns `["token", "frequency"]`.
5.  **Format:** Use DataFrame methods to order the final report by `frequency` (descending). 
6.  Show the resulting DataFrame!

**Expected Output (`word_count_df.show()`):**

```text
+--------------+---------+
|         token|frequency|
+--------------+---------+
|         ERROR|        4|
|        IP_192|        4|
|           404|        2|
|SERVER_TIMEOUT|        2|
|           500|        2|
|      DB_CRASH|        2|
|         IP_10|        2|
|          INFO|        1|
|           200|        1|
|    USER_LOGIN|        1|
|       WARNING|        1|
|           403|        1|
|  UNAUTHORIZED|        1|
+--------------+---------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Challenge_27").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Raw, unstructured log data
log_data = [
    "ERROR 404 SERVER_TIMEOUT IP_192",
    "INFO 200 USER_LOGIN IP_192",
    "ERROR 500 DB_CRASH IP_10",
    "ERROR 404 SERVER_TIMEOUT IP_192",
    "WARNING 403 UNAUTHORIZED IP_10",
    "ERROR 500 DB_CRASH IP_192"
]

# Create a raw RDD directly from the Python list
logs_rdd = spark.sparkContext.parallelize(log_data)

words_flat_rdd = logs_rdd.flatMap(lambda sentence : sentence.split(" "))

paired_rdd = words_flat_rdd.map(lambda word : (word, 1))
word_counts_rdd = paired_rdd.reduceByKey(lambda a, b : a+ b)

new_df = word_counts_rdd.toDF(["token", "frequency"])
new_df = new_df.orderBy(col("frequency").desc())
new_df.show()
```

### My Output Verification:

```
+--------------+---------+
|         token|frequency|
+--------------+---------+
|         ERROR|        4|
|        IP_192|        4|
|           404|        2|
|           500|        2|
|SERVER_TIMEOUT|        2|
|      DB_CRASH|        2|
|         IP_10|        2|
|           200|        1|
|          INFO|        1|
|       WARNING|        1|
|    USER_LOGIN|        1|
|           403|        1|
|  UNAUTHORIZED|        1|
+--------------+---------+
```
