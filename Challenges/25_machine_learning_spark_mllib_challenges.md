# PySpark Challenge 26: The Churn Predictor (ML Basics)

**The Scenario:**

You are working for a SaaS company. The Data Science team wants to build a model to predict if a user will "Churn" (cancel their subscription). 

They have provided you with a training dataset containing historical user metrics. Your task is to use PySpark MLlib to prepare the features, train a Logistic Regression classification model, and then use that model to predict churn on a new list of active users.

**The Setup:**

Run this code to generate your training and testing DataFrames.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Delta-enabled SparkSession (re-used from previous challenge)
builder = SparkSession.builder.appName("Challenge_26") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Historical Training Data (We know who churned)
train_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("days_active", DoubleType(), True),
    StructField("support_tickets", DoubleType(), True),
    StructField("churn_label", IntegerType(), True) # 1 = Churned, 0 = Stayed
])

train_data = [
    ("U_01", 300.0, 1.0, 0),
    ("U_02", 45.0, 5.0, 1), # High tickets, low tenure -> Churn
    ("U_03", 800.0, 0.0, 0),
    ("U_04", 10.0, 8.0, 1),
    ("U_05", 150.0, 2.0, 0),
    ("U_06", 20.0, 6.0, 1)
]
train_df = spark.createDataFrame(train_data, train_schema)

# 2. New Active Users (We need to PREDICT if they will churn)
test_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("days_active", DoubleType(), True),
    StructField("support_tickets", DoubleType(), True)
])

test_data = [
    ("U_99", 500.0, 1.0), # Likely safe
    ("U_98", 15.0, 9.0),  # High churn risk!
    ("U_97", 60.0, 2.0)
]
test_df = spark.createDataFrame(test_data, test_schema)
```

## Challenge 26 Task:

Write a PySpark MLlib script to train a model and make predictions.

**Requirements:**

1. Initialize a `VectorAssembler` to combine `days_active` and `support_tickets` into a single column called `"features"`.
2. Use the assembler to `.transform()` the `train_df`. Save this as `train_features_df`.
3. Initialize a `LogisticRegression` algorithm. Set the `featuresCol` to `"features"` and the `labelCol` to `"churn_label"`.
4. Train the model by calling `.fit()` on the `train_features_df`.
5. Prepare the test data: Use your existing assembler to `.transform()` the `test_df`. Save this as `test_features_df`.
6. Make predictions: Use your trained model to `.transform()` the `test_features_df`.
7. Select only the `user_id` and `prediction` columns from the resulting DataFrame, and show the results!

**Expected Output (`predictions_df.show()`):**
*(Note: Because ML algorithms use math under the hood, your predictions should map perfectly to 0.0 or 1.0 based on the obvious patterns in the training data).*

```text
+-------+----------+
|user_id|prediction|
+-------+----------+
|   U_99|       0.0|
|   U_98|       1.0|
|   U_97|       0.0|
+-------+----------+
```

### My Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

# Delta-enabled SparkSession (re-used from previous challenge)
builder = SparkSession.builder.appName("Challenge_25") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.3.1") \
    .master("local[*]")
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Historical Training Data (We know who churned)
train_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("days_active", DoubleType(), True),
    StructField("support_tickets", DoubleType(), True),
    StructField("churn_label", IntegerType(), True) # 1 = Churned, 0 = Stayed
])

train_data = [
    ("U_01", 300.0, 1.0, 0),
    ("U_02", 45.0, 5.0, 1), # High tickets, low tenure -> Churn
    ("U_03", 800.0, 0.0, 0),
    ("U_04", 10.0, 8.0, 1),
    ("U_05", 150.0, 2.0, 0),
    ("U_06", 20.0, 6.0, 1)
]
train_df = spark.createDataFrame(train_data, train_schema)

# 2. New Active Users (We need to PREDICT if they will churn)
test_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("days_active", DoubleType(), True),
    StructField("support_tickets", DoubleType(), True)
])

test_data = [
    ("U_99", 500.0, 1.0), # Likely safe
    ("U_98", 15.0, 9.0),  # High churn risk!
    ("U_97", 60.0, 2.0)
]
test_df = spark.createDataFrame(test_data, test_schema)

feature_columns = ["days_active", "support_tickets"]

assembler = VectorAssembler(inputCols = feature_columns, outputCol = "features")

train_features_df = assembler.transform(train_df)

train_features_df.show()

lr_algo = LogisticRegression(featuresCol="features", labelCol="churn_label")
model = lr_algo.fit(train_features_df)

test_features_df = assembler.transform(test_df)

predictions_df = model.transform(test_features_df)

predictions_df.select("user_id", "prediction").show(truncate=False)
```

### My Output Verification:

```
+-------+-----------+---------------+-----------+-----------+
|user_id|days_active|support_tickets|churn_label|   features|
+-------+-----------+---------------+-----------+-----------+
|   U_01|      300.0|            1.0|          0|[300.0,1.0]|
|   U_02|       45.0|            5.0|          1| [45.0,5.0]|
|   U_03|      800.0|            0.0|          0|[800.0,0.0]|
|   U_04|       10.0|            8.0|          1| [10.0,8.0]|
|   U_05|      150.0|            2.0|          0|[150.0,2.0]|
|   U_06|       20.0|            6.0|          1| [20.0,6.0]|
+-------+-----------+---------------+-----------+-----------+

+-------+----------+
|user_id|prediction|
+-------+----------+
|U_99   |0.0       |
|U_98   |1.0       |
|U_97   |0.0       |
+-------+----------+
```
