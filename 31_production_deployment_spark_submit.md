# PySpark Learning Log: Part 29 - Production Deployment (spark-submit)

You've mastered the code. Now, how do you actually deploy it in the real world?
Data Engineers rarely run production pipelines inside notebooks. Instead, they package their code into standard Python scripts (`.py` files) and submit them to a cluster using the command line.

## 1. The `spark-submit` Command

When you are ready to run your script on a live cluster (like AWS EMR, Google Dataproc, or an on-premise Hadoop cluster), you use a terminal tool called `spark-submit`.

This tool ships your Python file to the master node and allocates the hardware required to run it.

```bash
# A standard production spark-submit command executed in the terminal
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-memory 8G \
  --executor-cores 4 \
  my_etl_pipeline.py
```

* `--master yarn`: Tells Spark to use a cluster manager (like YARN or Kubernetes) instead of your local laptop.
* `--num-executors`: How many worker machines to allocate to the job.
* `--executor-memory`: How much RAM to give each worker node.

## 2. The Spark UI (Port 4040)

Whenever a SparkSession is active, Spark hosts a live web dashboard on your machine (usually accessible in your browser at `http://localhost:4040`).

* **Jobs/Stages/Tasks:** You can watch your distributed tasks execute across the cluster in real-time.
* **SQL/DataFrame:** You can see the visual DAG (Directed Acyclic Graph) of the physical execution plan we discussed in Part 14.
* **Storage:** Shows how much RAM your `.cache()` operations are actively consuming.

## 3. Packaging Production Code

To make a script `spark-submit` ready, you must ensure your logic is wrapped in a main execution block so it doesn't execute wildly during imports or testing.

Furthermore, **you should never hardcode `.master("local[*]")` in production code**. You leave the master blank, and let the `spark-submit` command inject the cluster parameters dynamically!

```python
from pyspark.sql import SparkSession

def process_data(spark):
    # Your ETL pipeline logic goes here
    pass

def main():
    # We DO NOT use .master() here. spark-submit will handle it.
    spark = SparkSession.builder.appName("ProdJob").getOrCreate()
    process_data(spark)

if __name__ == "__main__":
    main()
```
