# Airflow Challenge 2: The Metadata Pipeline

**The Scenario:**

Your data pipeline involves two steps. First, an API extraction task runs and returns the total number of records it successfully downloaded. Second, an auditing task reads that number and prints a confirmation message.

You need to use `PythonOperator` and XComs to pass the record count from Task A to Task B.

**The Task:**

Write a Python script that defines an Airflow DAG named `xcom_audit_pipeline`.

**Requirements:**

1. Define a Python function `extract_api_data()`:
   * It should declare a variable `records_downloaded = 4500`.
   * It should print a message saying it is downloading data.
   * It **must return** the `records_downloaded` integer.
2. Define a Python function `audit_data(ti)`:
   * It must use `ti.xcom_pull` to fetch the integer returned by the first task.
   * It should print: `"Audit Complete: Successfully received [X] records."` (where [X] is the pulled integer).
3. Create the DAG:
   * Name it `xcom_audit_pipeline`.
   * Set `schedule="@daily"` and `catchup=False`.
4. Create the two `PythonOperator` tasks:
   * `extract_task` (calls the first function).
   * `audit_task` (calls the second function).
5. Set the dependency so extraction runs before auditing.

*Hint: If you are testing this locally, Airflow will output the print statements directly into the task logs, just like you saw in Challenge 1!*

### My Solution:

```python
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


# Generates the data and returns it (Auto-Pushes to XCom)
def extract_api_data():
    records_downloaded = 4500
    print("Downloading data ...")
    return records_downloaded


# Pulls the data using the 'ti' context variable
def audit_data(ti):
    # Pull the return value from Task 1
    api_data = ti.xcom_pull(task_ids="get_api_data")
    print(f"Audit Complete: Successfully received {api_data} records.")


with DAG(
    dag_id="xcom_audit_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 8, 20),
    tags=["practice"],
) as dag:
    task_1 = PythonOperator(task_id="get_api_data", python_callable=extract_api_data)

    task_2 = PythonOperator(task_id="check_data", python_callable=audit_data)

    task_1 >> task_2
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::get_api_data:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=xcom_audit_pipeline/run_id=scheduled__2026-08-20T00:00:00+00:00/task_id=get_api_data/attempt=1.log
::endgroup::
[2026-08-20T07:41:11.506367Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a01e1d-e72a-7da1-9ceb-dcfdacffd83f dag_id=xcom_audit_pipeline task_id=get_api_data run_id=scheduled__2026-08-20T00:00:00+00:00 try_number=1 map_index=-1
[2026-08-20T07:41:11.892001Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-20T07:41:11.894085Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/34. The Metadata Pipeline.py
[2026-08-20T07:41:11.942452Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=34. The Metadata Pipeline.py  bundle_prepare_ms=2  dag_file_parse_ms=48 
[2026-08-20T07:41:11.970087Z] INFO - Task instance is in running state
[2026-08-20T07:41:11.970540Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-20T07:41:11.970764Z] INFO - ::endgroup::
[2026-08-20T07:41:11.971136Z] INFO - Current task name:get_api_data
[2026-08-20T07:41:11.971547Z] INFO - Dag name:xcom_audit_pipeline
[2026-08-20T07:41:11.973083Z] INFO - Downloading data ...
[2026-08-20T07:41:11.973110Z] INFO - Done. Returned value was: 4500
[2026-08-20T07:41:11.973280Z] INFO - ::group::Post Execute
[2026-08-20T07:41:11.973545Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a01e1d-e72a-7da1-9ceb-dcfdacffd83f'), task_id='get_api_data', dag_id='xcom_audit_pipeline', run_id='scheduled__2026-08-20T00:00:00+00:00', try_number=1, dag_version_id=UUID('01a01e11-2c36-7e17-a4b0-03ff26db8d41'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-dc61b823f3c1f9fe8d6c0b339d4c4b48-6bc8a4f25b1b6d58-00'}, queue='default', task=<Task(PythonOperator): get_api_data>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 8, 20, 7, 41, 11, 532558, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-20T07:41:12.019636Z] INFO - Task instance in success state
[2026-08-20T07:41:12.020091Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-20T07:41:12.020485Z] INFO - Task operator:<Task(PythonOperator): get_api_data>
[2026-08-20T07:41:12.020116Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::check_data:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=xcom_audit_pipeline/run_id=scheduled__2026-08-20T00:00:00+00:00/task_id=check_data/attempt=1.log
::endgroup::
[2026-08-20T07:41:12.286112Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a01e1d-e72b-7df2-9da5-7de3d19a8e09 dag_id=xcom_audit_pipeline task_id=check_data run_id=scheduled__2026-08-20T00:00:00+00:00 try_number=1 map_index=-1
[2026-08-20T07:41:12.626184Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-20T07:41:12.628462Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/34. The Metadata Pipeline.py
[2026-08-20T07:41:12.684504Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=34. The Metadata Pipeline.py  bundle_prepare_ms=2  dag_file_parse_ms=56 
[2026-08-20T07:41:12.714469Z] INFO - Task instance is in running state
[2026-08-20T07:41:12.715032Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-20T07:41:12.715458Z] INFO - Current task name:check_data
[2026-08-20T07:41:12.715823Z] INFO - Dag name:xcom_audit_pipeline
[2026-08-20T07:41:12.715122Z] INFO - ::endgroup::
[2026-08-20T07:41:12.729162Z] INFO - Audit Complete: Successfully received 4500 records.
[2026-08-20T07:41:12.729189Z] INFO - Done. Returned value was: None
[2026-08-20T07:41:12.729342Z] INFO - ::group::Post Execute
[2026-08-20T07:41:12.744308Z] INFO - Task instance in success state
[2026-08-20T07:41:12.744848Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-20T07:41:12.745392Z] INFO - Task operator:<Task(PythonOperator): check_data>
[2026-08-20T07:41:12.745363Z] INFO - ::endgroup::
```
