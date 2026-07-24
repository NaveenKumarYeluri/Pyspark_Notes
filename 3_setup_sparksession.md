# PySpark Learning Log: Part 1 - Colab Setup & The SparkSession

## 1. Setting Up PySpark in Google Colab

Google Colab provides a free, cloud-based Jupyter Notebook environment. However, it does not come with PySpark pre-installed. You have to install it at the top of your notebook every time you start a new session.

* **Step 1:** Open a new notebook in [Google Colab](https://colab.research.google.com/).
* **Step 2:** Run the following command in the very first cell. The `!` tells Colab to run this as a terminal command rather than Python code.

```python
!pip install pyspark
```

## 2. Initializing the SparkSession

Before you can process any data, you need to launch the PySpark application. You do this by creating a **SparkSession**, which acts as the central control node (the Driver) for your operations.

```python
from pyspark.sql import SparkSession

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("FlightAnalytics_Ingestion") \
    .master("local[*]") \
    .getOrCreate()

# Verify it's running
print("Spark Version:", spark.version)
```

## 3. Reading Your First Dataset

PySpark DataFrames are designed to easily ingest structured data. For this example, imagine we have a CSV file containing flight records (`flights.csv`). 

Here is how you read a CSV file into a PySpark DataFrame. Notice how we explicitly tell Spark that our file has a header row and that it should try to infer the data types (like integers for flight numbers and strings for airlines).

```python
df = spark.read.csv(
    "flights.csv", 
    header=True,       # Uses the first row as column names
    inferSchema=True   # Automatically detects if a column is a string, integer, etc.
)
```

## 4. Inspecting the Data (Actions)

Running `spark.read.csv` doesn't actually process the data yet; it just builds the execution plan (Lazy Evaluation). To actually see the data, we must use an **Action**.

### Viewing the Schema

Before looking at the rows, it is critical to understand the shape and data types of your DataFrame. 

```python
# Prints the column names and their data types (String, Integer, etc.)
df.printSchema()
```

### Viewing the Rows

To look at the actual data, we use the `show()` command. By default, it prints the first 20 rows.

```python
# Displays the first 5 rows of the DataFrame
df.show(5)
```
