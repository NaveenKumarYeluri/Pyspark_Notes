# Airflow Challenge 8: The Data Chain

**The Scenario:**

You manage two completely separate DAGs. The first DAG extracts raw order data and saves it as a trusted dataset. The second DAG generates an executive summary report, but it should *only* run after the trusted dataset has been successfully refreshed.

**The Task:**

Write a single Python file that contains two distinct DAGs linked by a Dataset.

**Requirements:**

1. Import `Dataset` from `airflow.datasets`.
2. Define a dataset called `trusted_orders` pointing to the URI `"s3://data-lake/trusted_orders.csv"`.
3. Create the **Producer DAG** named `extract_orders_dag`:
    * Schedule it to run `@daily`.
    * Create a `@task` called `clean_orders()` that prints `"Orders cleaned and saved."`
    * Assign `outlets=[trusted_orders]` to this task.
4. Create the **Consumer DAG** named `generate_report_dag`:
    * Set its schedule to `[trusted_orders]`.
    * Create a `@task` called `build_report()` that prints `"Generating report from trusted orders."`
5. Execute the DAGs. 
    * *Hint: You will need to trigger the Producer DAG manually in your Airflow UI or terminal. Once it succeeds, Airflow will automatically trigger the Consumer DAG without you doing anything!*
    

### My Solution:

```python
from datetime import datetime
from airflow.sdk import dag, task, Asset


trusted_orders = Asset("s3://data-lake/trusted_orders.csv")


@dag(
    dag_id="orders_producer",
    schedule="@daily",
    start_date=datetime(2026, 8, 25),
    catchup=False,
    tags=["practice"],
)
def extract_orders_dag():

    @task(outlets=[trusted_orders])
    def clean_orders():
        print("Orders cleaned and saved.")

    clean_orders()


extract_orders_dag()


@dag(
    dag_id="orders_consumer",
    schedule=[trusted_orders],
    start_date=datetime(2026, 8, 25),
    catchup=False,
    tags=["practice"],
)
def generate_report_dag():

    @task()
    def build_report():
        print("Generating report from trusted orders.")

    build_report()


generate_report_dag()
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::clean_orders:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=orders_producer/run_id=scheduled__2026-08-25T00:00:00+00:00/task_id=clean_orders/attempt=1.log
::endgroup::
[2026-08-25T06:54:08.195463Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a037b2-9ca5-77a3-b907-2618d10baaee dag_id=orders_producer task_id=clean_orders run_id=scheduled__2026-08-25T00:00:00+00:00 try_number=1 map_index=-1
[2026-08-25T06:54:16.266380Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-25T06:54:16.267315Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/40. The Data Chain.py
[2026-08-25T06:54:16.316850Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=40. The Data Chain.py  bundle_prepare_ms=3  dag_file_parse_ms=49 
[2026-08-25T06:54:16.447100Z] INFO - Task instance is in running state
[2026-08-25T06:54:16.447495Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-25T06:54:16.447789Z] INFO - Current task name:clean_orders
[2026-08-25T06:54:16.447932Z] INFO - Dag name:orders_producer
[2026-08-25T06:54:16.447689Z] INFO - ::endgroup::
[2026-08-25T06:54:16.449134Z] INFO - Orders cleaned and saved.
[2026-08-25T06:54:16.449148Z] INFO - Done. Returned value was: None
[2026-08-25T06:54:16.449286Z] INFO - ::group::Post Execute
[2026-08-25T06:54:16.509108Z] INFO - Task instance in success state
[2026-08-25T06:54:16.509367Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-25T06:54:16.509699Z] INFO - Task operator:<Task(_PythonDecoratedOperator): clean_orders>
[2026-08-25T06:54:16.509535Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::build_report:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=orders_consumer/run_id=asset_triggered__2026-08-25T06:54:16.499921+00:00_uEZS379C/task_id=build_report/attempt=1.log
::endgroup::
[2026-08-25T06:54:17.300894Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a037b2-bfdf-73f9-9275-112c823b97a9 dag_id=orders_consumer task_id=build_report run_id=asset_triggered__2026-08-25T06:54:16.499921+00:00_uEZS379C try_number=1 map_index=-1
[2026-08-25T06:54:17.533507Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-25T06:54:17.534310Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/40. The Data Chain.py
[2026-08-25T06:54:17.573747Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=40. The Data Chain.py  bundle_prepare_ms=1  dag_file_parse_ms=39 
[2026-08-25T06:54:17.607554Z] INFO - Task instance is in running state
[2026-08-25T06:54:17.607947Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-25T06:54:17.608329Z] INFO - Current task name:build_report
[2026-08-25T06:54:17.608581Z] INFO - Dag name:orders_consumer
[2026-08-25T06:54:17.608197Z] INFO - ::endgroup::
[2026-08-25T06:54:17.610014Z] INFO - Generating report from trusted orders.
[2026-08-25T06:54:17.610040Z] INFO - Done. Returned value was: None
[2026-08-25T06:54:17.610221Z] INFO - ::group::Post Execute
[2026-08-25T06:54:17.625363Z] INFO - Task instance in success state
[2026-08-25T06:54:17.625664Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-25T06:54:17.625952Z] INFO - Task operator:<Task(_PythonDecoratedOperator): build_report>
[2026-08-25T06:54:17.625752Z] INFO - ::endgroup::
```
