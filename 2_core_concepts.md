# PySpark Learning Log: Part 1 - Core Concepts & Setup

## 1. The Core Data Structures: RDDs vs. DataFrames

Before writing code, it is critical to understand how PySpark holds data in memory across a cluster. There are two main data structures you will encounter:

### Resilient Distributed Datasets (RDDs)

* **What they are:** The original, low-level data structure of Spark. An RDD is a fault-tolerant collection of elements partitioned across the nodes of the cluster that can be operated on in parallel.
* **The Catch:** They do not have a schema (no distinct columns or data types like a SQL table). You have to write low-level functional programming (like map, filter, reduce) to manipulate them.
* **Verdict:** You should know they exist, but you will rarely write RDD code today.

### PySpark DataFrames

* **What they are:** The modern, standard way to write PySpark. A DataFrame is a distributed collection of data organized into named columns.
* **Why they win:** They look and feel very similar to Pandas DataFrames or SQL tables. Under the hood, PySpark uses a sophisticated engine (the Catalyst Optimizer) to automatically optimize DataFrame queries before executing them. 
* **Use Case:** When processing massive batches of records for a Flight Analytics System or building robust ETL pipelines, the DataFrame API is your primary tool.

---

## 2. The Entry Point: SparkSession

In Pandas, you simply `import pandas as pd` and start reading files. In PySpark, you must establish a connection to the Spark cluster (even if that "cluster" is just your local machine). You do this by creating a **SparkSession**.

The SparkSession is the unified entry point for reading data, executing SQL queries, and managing cluster resources.

### The Boilerplate Code

```python
# Import the SparkSession module
from pyspark.sql import SparkSession

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .master("local[*]") \
    .getOrCreate()

# Print the session details to verify it's running
print(spark)
```

**Breaking down the code:**

* `appName()`: Gives your application a name, which shows up in the Spark Web UI (a dashboard for monitoring your jobs).

* `master("local[*]")`: Tells Spark where to run. local[*] means "run on my local machine and use as many CPU cores as I have available." In a production cloud environment, this would point to a cluster manager URL.

* `getOrCreate()`: A safety feature. It gets an existing SparkSession if one is already running, or creates a new one if it isn't.

---

## 3. Challenge 1: Conceptual Check

**Question:** If PySpark utilizes "Lazy Evaluation," what happens immediately when you write a line of code to filter out all rows where status == 'cancelled'?

**Answer:** Since PySpark utilizes Lazy Evaluation, Until Action step is called it is going to create an execution plan which is called DAG (Directed Acyclic Graph)
