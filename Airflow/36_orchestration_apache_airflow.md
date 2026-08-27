# Data Engineering Learning Log: Part 34 - Orchestration (Apache Airflow)

You have mastered PySpark! You can extract, clean, aggregate, and save massive datasets. But in the real world, who runs these scripts every night at 2:00 AM? 

Enter **Apache Airflow**. Airflow is an open-source orchestration tool created by Airbnb. It allows you to schedule, monitor, and manage your data pipelines.

## 1. What is a DAG?

In Airflow, a pipeline is called a **DAG** (Directed Acyclic Graph). 

* **Directed:** The tasks have a specific order (Task A -> Task B).
* **Acyclic:** The pipeline flows in one direction and never loops back on itself.

## 2. Operators (The Tasks)

A DAG is made up of Tasks. You define Tasks using **Operators**.

* `BashOperator`: Executes a bash command in the terminal.
* `PythonOperator`: Executes a standard Python function.
* `SparkSubmitOperator`: Executes a `spark-submit` command (this is how you run your PySpark scripts!).

## 3. A Basic Airflow DAG

Here is what a standard Airflow Python script looks like:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Define default arguments
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Instantiate the DAG
with DAG(
    dag_id='my_first_pyspark_orchestration',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@daily', # Run once a day
    catchup=False
) as dag:

    # 3. Define the Tasks
    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data from API..."'
    )

    run_pyspark_task = BashOperator(
        task_id='run_spark_job',
        bash_command='spark-submit /path/to/your/script.py'
    )

    # 4. Set the Dependencies (Order of Operations)
    extract_task >> run_pyspark_task
```
