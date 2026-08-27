# Airflow Challenge 1: The First DAG

**The Scenario:**

You have written three scripts: one to extract data, one to run a PySpark Delta Lake merge, and one to send an email alert if it succeeds. You need to orchestrate them so they run automatically every day.

**The Task:**

Write a Python script that defines an Airflow DAG named `daily_sales_pipeline`.

**Requirements:**

1. Import `DAG` and `BashOperator`.
2. Create a DAG that runs `@daily`, starting from `datetime(2026, 8, 1)`.
3. Create three `BashOperator` tasks:
    * `task_1`: Task ID should be `extract_sales`, command: `echo "Extracting Sales"`
    * `task_2`: Task ID should be `process_spark`, command: `echo "Running PySpark"`
    * `task_3`: Task ID should be `send_email`, command: `echo "Email Sent"`
4. Set the dependencies so that `task_1` runs first, followed by `task_2`, followed by `task_3` (using the `>>` operator).


### My Solution:

```python
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Define default arguments
default_args = {"owner": "data_engineer", "retries": 1, "retry_delay": timedelta(minutes=5)}

# 2. Instantiate the DAG
with DAG(
    dag_id="daily_sales_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    # schedule_interval='@daily', this option does not exist.
    schedule="@daily",
    catchup=False,
    tags=["practice"],
) as dag:

    # 3. Define tasks
    extract_sales_task = BashOperator(
        task_id="extract_sales", bash_command='echo "Extracting Sales"'
    )

    process_spark_task = BashOperator(
        task_id="process_spark", bash_command='echo "Running PySpark"'
    )

    send_email_task = BashOperator(
        task_id="send_email", bash_command='echo "Email Sent"'
    )

    # 4. Set the Dependencies (Order of Operations)
    extract_sales_task >> process_spark_task >> send_email_task
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_sales:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=daily_sales_pipeline/run_id=scheduled__2026-08-19T00:00:00+00:00/task_id=extract_sales/attempt=1.log
::endgroup::
[2026-08-19T05:44:45.933218Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0188c-f383-7f5b-aaed-a63d85cf5e05 dag_id=daily_sales_pipeline task_id=extract_sales run_id=scheduled__2026-08-19T00:00:00+00:00 try_number=1 map_index=-1
[2026-08-19T05:44:46.521491Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-19T05:44:46.522952Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/33. The First DAG.py
[2026-08-19T05:44:46.536554Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=33. The First DAG.py  bundle_prepare_ms=2  dag_file_parse_ms=14 
[2026-08-19T05:44:46.576469Z] INFO - ::endgroup::
[2026-08-19T05:44:46.577850Z] INFO - Task instance is in running state
[2026-08-19T05:44:46.577452Z] INFO - Tmp dir root location: /tmp
[2026-08-19T05:44:46.578252Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-19T05:44:46.578555Z] INFO - Current task name:extract_sales
[2026-08-19T05:44:46.578818Z] INFO - Dag name:daily_sales_pipeline
[2026-08-19T05:44:46.579354Z] INFO - Running command: ['/usr/bin/bash', '-c', 'echo "Extracting Sales"']
[2026-08-19T05:44:46.580564Z] INFO - Output:
[2026-08-19T05:44:46.582595Z] INFO - Extracting Sales
[2026-08-19T05:44:46.583157Z] INFO - Command exited with return code 0
[2026-08-19T05:44:46.583554Z] INFO - ::group::Post Execute
[2026-08-19T05:44:46.583872Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a0188c-f383-7f5b-aaed-a63d85cf5e05'), task_id='extract_sales', dag_id='daily_sales_pipeline', run_id='scheduled__2026-08-19T00:00:00+00:00', try_number=1, dag_version_id=UUID('01a0188b-109e-762b-9130-bbee28dddc71'), map_index=-1, hostname='myhostname', context_carrier={'traceparent': '00-76943741cf3702bdc9244bf7c70e48ea-4a6ef182b57d5b5c-00'}, queue='default', task=<Task(BashOperator): extract_sales>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=1, start_date=datetime.datetime(2026, 8, 19, 5, 44, 46, 6159, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-19T05:44:46.636679Z] INFO - Task instance in success state
[2026-08-19T05:44:46.636974Z] INFO - ::endgroup::
[2026-08-19T05:44:46.637008Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-19T05:44:46.637363Z] INFO - Task operator:<Task(BashOperator): extract_sales>


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::process_spark:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=daily_sales_pipeline/run_id=scheduled__2026-08-19T00:00:00+00:00/task_id=process_spark/attempt=1.log
::endgroup::
[2026-08-19T05:44:47.241014Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0188c-f384-7053-bcbc-1c0e6a0f01fb dag_id=daily_sales_pipeline task_id=process_spark run_id=scheduled__2026-08-19T00:00:00+00:00 try_number=1 map_index=-1
[2026-08-19T05:44:47.572876Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-19T05:44:47.573960Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/33. The First DAG.py
[2026-08-19T05:44:47.586410Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=33. The First DAG.py  bundle_prepare_ms=2  dag_file_parse_ms=12 
[2026-08-19T05:44:47.619419Z] INFO - ::endgroup::
[2026-08-19T05:44:47.620511Z] INFO - Task instance is in running state
[2026-08-19T05:44:47.620890Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-19T05:44:47.620204Z] INFO - Tmp dir root location: /tmp
[2026-08-19T05:44:47.620666Z] INFO - Running command: ['/usr/bin/bash', '-c', 'echo "Running PySpark"']
[2026-08-19T05:44:47.621099Z] INFO - Current task name:process_spark
[2026-08-19T05:44:47.621335Z] INFO - Dag name:daily_sales_pipeline
[2026-08-19T05:44:47.621811Z] INFO - Output:
[2026-08-19T05:44:47.624285Z] INFO - Running PySpark
[2026-08-19T05:44:47.624573Z] INFO - Command exited with return code 0
[2026-08-19T05:44:47.624970Z] INFO - ::group::Post Execute
[2026-08-19T05:44:47.625331Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a0188c-f384-7053-bcbc-1c0e6a0f01fb'), task_id='process_spark', dag_id='daily_sales_pipeline', run_id='scheduled__2026-08-19T00:00:00+00:00', try_number=1, dag_version_id=UUID('01a0188b-109e-762b-9130-bbee28dddc71'), map_index=-1, hostname='myhostname', context_carrier={'traceparent': '00-76943741cf3702bdc9244bf7c70e48ea-4a6ef182b57d5b5c-00'}, queue='default', task=<Task(BashOperator): process_spark>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=1, start_date=datetime.datetime(2026, 8, 19, 5, 44, 47, 252519, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-19T05:44:47.667254Z] INFO - Task instance in success state
[2026-08-19T05:44:47.667576Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-19T05:44:47.667456Z] INFO - ::endgroup::
[2026-08-19T05:44:47.668335Z] INFO - Task operator:<Task(BashOperator): process_spark>


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::send_email:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=daily_sales_pipeline/run_id=scheduled__2026-08-19T00:00:00+00:00/task_id=send_email/attempt=1.log
::endgroup::
[2026-08-19T05:44:48.633905Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0188c-f385-7f53-965b-cc0e136109e4 dag_id=daily_sales_pipeline task_id=send_email run_id=scheduled__2026-08-19T00:00:00+00:00 try_number=1 map_index=-1
[2026-08-19T05:44:49.272407Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-19T05:44:49.274756Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/33. The First DAG.py
[2026-08-19T05:44:49.291560Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=33. The First DAG.py  bundle_prepare_ms=3  dag_file_parse_ms=17 
[2026-08-19T05:44:49.348345Z] INFO - ::endgroup::
[2026-08-19T05:44:49.350046Z] INFO - Task instance is in running state
[2026-08-19T05:44:49.350458Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-19T05:44:49.351807Z] INFO - Current task name:send_email
[2026-08-19T05:44:49.351974Z] INFO - Tmp dir root location: /tmp
[2026-08-19T05:44:49.352224Z] INFO - Dag name:daily_sales_pipeline
[2026-08-19T05:44:49.352520Z] INFO - Running command: ['/usr/bin/bash', '-c', 'echo "Email Sent"']
[2026-08-19T05:44:49.353877Z] INFO - Output:
[2026-08-19T05:44:49.358179Z] INFO - Email Sent
[2026-08-19T05:44:49.359552Z] INFO - Command exited with return code 0
[2026-08-19T05:44:49.360082Z] INFO - ::group::Post Execute
[2026-08-19T05:44:49.360462Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a0188c-f385-7f53-965b-cc0e136109e4'), task_id='send_email', dag_id='daily_sales_pipeline', run_id='scheduled__2026-08-19T00:00:00+00:00', try_number=1, dag_version_id=UUID('01a0188b-109e-762b-9130-bbee28dddc71'), map_index=-1, hostname='myhostname', context_carrier={'traceparent': '00-76943741cf3702bdc9244bf7c70e48ea-4a6ef182b57d5b5c-00'}, queue='default', task=<Task(BashOperator): send_email>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=1, start_date=datetime.datetime(2026, 8, 19, 5, 44, 48, 733017, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-19T05:44:49.471289Z] INFO - ::endgroup::
[2026-08-19T05:44:49.472983Z] INFO - Task instance in success state
[2026-08-19T05:44:49.473680Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-19T05:44:49.474517Z] INFO - Task operator:<Task(BashOperator): send_email>

```
