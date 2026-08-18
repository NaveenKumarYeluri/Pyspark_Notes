# PySpark Learning Log: Part 26 - Machine Learning (Spark MLlib)

Data Engineering and Data Science go hand-in-hand. Once you have built a clean, optimized "Gold" data layer, Data Scientists will use that data to train predictive models. PySpark has a massive built-in library called **MLlib** designed specifically to train Machine Learning models across distributed clusters.

MLlib relies on three main concepts:

1. **Transformers:** Algorithms that transform one DataFrame into another (e.g., converting text to numbers, or combining columns).
2. **Estimators:** Machine Learning algorithms that train on a DataFrame and produce a Model (e.g., Random Forest, Logistic Regression).
3. **Pipelines:** A chain of Transformers and Estimators bundled together.

## 1. Feature Engineering: The `VectorAssembler`

Unlike standard Python ML libraries (like Scikit-Learn) that accept a grid of columns, PySpark MLlib requires all of your input "features" (the columns you are using to make a prediction) to be combined into a **single Array/Vector column**. 

We do this using a Transformer called `VectorAssembler`.

```python
from pyspark.ml.feature import VectorAssembler

# We want to predict something based on age, income, and engagement score
feature_columns = ["age", "income", "engagement_score"]

# 1. Initialize the Assembler
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# 2. Transform the DataFrame
ml_ready_df = assembler.transform(clean_data_df)
```

## 2. Training a Model (Estimator)

Once your features are bundled into a single `"features"` vector column, you can pass the DataFrame to an Estimator to learn from the data.

```python
from pyspark.ml.classification import LogisticRegression

# 1. Initialize the Algorithm
# We tell it which column holds our features, and which column holds the true answer (label)
lr_algo = LogisticRegression(featuresCol="features", labelCol="churned")

# 2. Train (Fit) the Model on the data
model = lr_algo.fit(ml_ready_df)
```

## 3. Making Predictions

Once the model is trained (`fit`), it becomes a Transformer itself! You can now use it to make predictions on new data.

```python
# 3. Make predictions on new, unseen data
predictions_df = model.transform(new_data_df)

# The model automatically adds a 'prediction' column and a 'probability' column!
predictions_df.select("user_id", "features", "prediction", "probability").show()
```
