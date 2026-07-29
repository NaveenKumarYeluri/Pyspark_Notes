# PySpark Learning Log: Part 7 - Writing and Saving Data

Once you have finished cleaning, transforming, and joining your DataFrames, you need to save the results. In the ETL (Extract, Transform, Load) lifecycle, this is the **Load** phase. 

PySpark uses the `df.write` interface to save data. 

## 1. Writing to CSV

CSV is human-readable and great for small outputs or sharing data with business analysts who use Excel.

```python
# Write the DataFrame to a folder called 'output_csv'
df.write.csv("output_csv", header=True)
```
*Note: PySpark is a distributed system. When you write to a folder, PySpark doesn't create one single CSV file. It creates a folder containing multiple smaller CSV files (one for each partition/worker node). This is perfectly normal in Big Data!*

## 2. Writing to Parquet (The Big Data Standard)

While CSV is common, **Parquet** is the gold standard for Big Data.

* **Columnar:** It stores data by columns rather than rows, making analytics queries blazing fast.
* **Compressed:** It automatically compresses the data, saving massive amounts of storage space.
* **Schema-Preserving:** Unlike CSVs, Parquet remembers your data types (Integers stay Integers, Booleans stay Booleans).

```python
# Write the DataFrame to a folder called 'output_parquet'
df.write.parquet("output_parquet")
```

## 3. Save Modes (Handling Existing Files)

What happens if you run your script twice? PySpark will crash because the output folder already exists. To fix this, you must specify a **Save Mode**.

* `"error"` (Default): Crashes if the folder exists.
* `"overwrite"`: Deletes the existing folder and writes the new data.
* `"append"`: Adds the new data into the existing folder.
* `"ignore"`: Does nothing if the folder already exists.

```python
# Safely overwrite the data every time the script runs
df.write.mode("overwrite").parquet("output_parquet")
```

## 4. Partitioning Data on Disk (`partitionBy`)

If you have a massive dataset of global flights, you might want to physically organize the files on your hard drive by a specific column (like `flight_type` or `year`). This makes future filtering incredibly fast.

```python
# This creates sub-folders like: 
# /output_partitioned/flight_type=Domestic/
# /output_partitioned/flight_type=International/
df.write.mode("overwrite").partitionBy("flight_type").parquet("output_partitioned")
```
