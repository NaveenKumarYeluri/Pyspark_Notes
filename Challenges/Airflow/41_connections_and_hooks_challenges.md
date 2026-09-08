# Airflow Challenge 9: The External Hook

**The Scenario:**

You are orchestrating a pipeline that moves flight data into Amazon Redshift. Before triggering the Redshift load, you need to write a quick task to verify the required data file actually exists in S3. Since you are testing locally without live AWS credentials, we will use a dummy class to mock the Hook's behavior.

**The Task:**

Write a DAG named `s3_hook_validation`.

**Requirements:**
1. Create a simple mock class at the top of your script called `MockS3Hook`. 
   * Give it a method `check_for_key(self, key, bucket_name)` that simply prints the key and bucket it is searching for, and then returns `True`.
2. Create a `@task` called `verify_s3_file()`.
3. Inside the task, instantiate your `MockS3Hook`.
4. Call the `.check_for_key()` method, looking for `key="flight_data_2026.csv"` in `bucket_name="trusted-flight-data"`.
5. Print `"File verified in S3. Proceeding to Redshift load."` if it returns `True`.
6. Create the `@dag` and execute the pipeline.

### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


class MockS3Hook:

    def __init__(self, key, bucket_name):
        self.key = key
        self.bucket_name = bucket_name

    def check_for_key(self):
        print(f"Key: {self.key}, Bucket Name: {self.bucket_name}")
        return True


@task
def verify_s3_file():
    my_hook = MockS3Hook(key="flight_data_2026.csv", bucket_name="trusted-flight-data")
    my_hook.check_for_key()
    print("File verified in S3. Proceeding to Redshift load.")
    return True


@dag(
    dag_id="s3_hook_validation",
    schedule="@daily",
    start_date=datetime(2026, 8, 25),
    catchup=False,
    tags=["practice"],
)
def s3_hook_validation_fun():
    verify_s3_file()


s3_hook_validation_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::verify_s3_file::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=s3_hook_validation/run_id=manual__2026-08-25T07:55:56.056030+00:00/task_id=verify_s3_file/attempt=1.log
::endgroup::
[2026-08-25T07:55:58.277130Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a037eb-3350-7993-8adb-d8f9ffcb6c57 dag_id=s3_hook_validation task_id=verify_s3_file run_id=manual__2026-08-25T07:55:56.056030+00:00 try_number=1 map_index=-1
[2026-08-25T07:56:05.826088Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-25T07:56:05.826896Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/41. The External Hook.py
[2026-08-25T07:56:05.897223Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=41. The External Hook.py  bundle_prepare_ms=2  dag_file_parse_ms=70 
[2026-08-25T07:56:05.962405Z] INFO - ::endgroup::
[2026-08-25T07:56:05.963843Z] INFO - Task instance is in running state
[2026-08-25T07:56:05.964253Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-25T07:56:05.964533Z] INFO - Current task name:verify_s3_file
[2026-08-25T07:56:05.964747Z] INFO - Dag name:s3_hook_validation
[2026-08-25T07:56:05.971631Z] INFO - Done. Returned value was: True
[2026-08-25T07:56:05.971867Z] INFO - ::group::Post Execute
[2026-08-25T07:56:05.972147Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a037eb-3350-7993-8adb-d8f9ffcb6c57'), task_id='verify_s3_file', dag_id='s3_hook_validation', run_id='manual__2026-08-25T07:55:56.056030+00:00', try_number=1, dag_version_id=UUID('01a037db-2b77-7bc8-96d4-ce5d77e70aa9'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-d4d7527ae402f0d768f9e246c71efea8-17b40bbe6eae5c1a-00'}, queue='default', task=<Task(_PythonDecoratedOperator): verify_s3_file>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 8, 25, 7, 56, 5, 423389, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-25T07:56:05.974513Z] INFO - Key: flight_data_2026.csv, Bucket Name: trusted-flight-data
[2026-08-25T07:56:05.974889Z] INFO - File verified in S3. Proceeding to Redshift load.
[2026-08-25T07:56:06.078732Z] INFO - ::endgroup::
[2026-08-25T07:56:06.082658Z] INFO - Task instance in success state
[2026-08-25T07:56:06.092724Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-25T07:56:06.093146Z] INFO - Task operator:<Task(_PythonDecoratedOperator): verify_s3_file>

```
