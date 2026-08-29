# Airflow Challenge 3: The Modern Refactor

**The Scenario:**

Your Senior Data Engineer reviewed your `xcom_audit_pipeline` from Challenge 2. They love the logic, but they want the codebase modernized. They have asked you to refactor the exact same pipeline using the TaskFlow API.

**The Task:**

Write a Python script that defines an Airflow DAG named `taskflow_audit_pipeline`.

**Requirements:**

1. Import `dag` and `task` from `airflow.decorators`.
2. Define a `@task` called `extract_api_data()`:
   * It should declare `records_downloaded = 4500`.
   * It should print "Downloading data ...".
   * It must return the integer.
3. Define a `@task` called `audit_data(records)`:
   * Notice it takes `records` as an argument (no `ti`!).
   * It should print: `"Audit Complete: Successfully received [X] records."`
4. Define a `@dag` function named `taskflow_audit_pipeline()`:
   * Set `schedule="@daily"`, `start_date`, and `catchup=False`.
   * Inside this function, execute the tasks by passing the output of the extraction task directly into the audit task.
5. Finally, call `taskflow_audit_pipeline()` at the bottom of the script to generate the DAG.

### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@task
def extract_api_data():
    records_downloaded = 4500
    print("Downloading data ...")
    return records_downloaded


@task
def audit_data(records):
    print(f"Audit Complete: Successfully received {records} records.")


@dag(
    dag_id="taskflow_audit_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 8, 21),
    catchup=False,
    tags=["practice"],
)
def taskflow_audit_pipeline():
    records = extract_api_data()
    audit_data(records)


taskflow_audit_pipeline()
```

### My Output Verification:

```

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_api_data:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=taskflow_audit_pipeline/run_id=manual__2026-08-21T04:40:47.268743+00:00/task_id=extract_api_data/attempt=1.log
::endgroup::
[2026-08-21T04:40:48.999511Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0229f-19b2-773b-bc86-cf759ba30448 dag_id=taskflow_audit_pipeline task_id=extract_api_data run_id=manual__2026-08-21T04:40:47.268743+00:00 try_number=1 map_index=-1
[2026-08-21T04:40:49.357353Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-21T04:40:49.358752Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/35. The Modern Refactor.py
[2026-08-21T04:40:49.449621Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=35. The Modern Refactor.py  bundle_prepare_ms=2  dag_file_parse_ms=91 
[2026-08-21T04:40:49.522071Z] INFO - ::endgroup::
[2026-08-21T04:40:49.523261Z] INFO - Task instance is in running state
[2026-08-21T04:40:49.523712Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-21T04:40:49.524326Z] INFO - Current task name:extract_api_data
[2026-08-21T04:40:49.524789Z] INFO - Dag name:taskflow_audit_pipeline
[2026-08-21T04:40:49.532390Z] INFO - Done. Returned value was: 4500
[2026-08-21T04:40:49.532659Z] INFO - ::group::Post Execute
[2026-08-21T04:40:49.534517Z] INFO - Downloading data ...
[2026-08-21T04:40:49.536841Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a0229f-19b2-773b-bc86-cf759ba30448'), task_id='extract_api_data', dag_id='taskflow_audit_pipeline', run_id='manual__2026-08-21T04:40:47.268743+00:00', try_number=1, dag_version_id=UUID('01a02291-9ce5-73a5-b07e-0ef938db3833'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-f54f2e934c7190a6381b522aa361558a-bd8e966db24e5250-00'}, queue='default', task=<Task(_PythonDecoratedOperator): extract_api_data>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 8, 21, 4, 40, 49, 45140, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-21T04:40:49.707712Z] INFO - Task instance in success state
[2026-08-21T04:40:49.720744Z] INFO - ::endgroup::
[2026-08-21T04:40:49.734216Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-21T04:40:49.740386Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_api_data>


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::audit_data:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=taskflow_audit_pipeline/run_id=manual__2026-08-21T04:40:47.268743+00:00/task_id=audit_data/attempt=1.log
::endgroup::
[2026-08-21T04:40:50.013078Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0229f-19b3-7ee9-8979-6ed9a0c3dc0e dag_id=taskflow_audit_pipeline task_id=audit_data run_id=manual__2026-08-21T04:40:47.268743+00:00 try_number=1 map_index=-1
[2026-08-21T04:40:50.470651Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-21T04:40:50.471931Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/35. The Modern Refactor.py
[2026-08-21T04:40:50.560191Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=35. The Modern Refactor.py  bundle_prepare_ms=2  dag_file_parse_ms=88 
[2026-08-21T04:40:50.643688Z] INFO - Task instance is in running state
[2026-08-21T04:40:50.643334Z] INFO - ::endgroup::
[2026-08-21T04:40:50.644259Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-21T04:40:50.645718Z] INFO - Current task name:audit_data
[2026-08-21T04:40:50.646217Z] INFO - Dag name:taskflow_audit_pipeline
[2026-08-21T04:40:50.648825Z] INFO - Done. Returned value was: None
[2026-08-21T04:40:50.649102Z] INFO - ::group::Post Execute
[2026-08-21T04:40:50.650965Z] INFO - Audit Complete: Successfully received 4500 records.
[2026-08-21T04:40:50.673713Z] INFO - Task instance in success state
[2026-08-21T04:40:50.673815Z] INFO - ::endgroup::
[2026-08-21T04:40:50.674137Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-21T04:40:50.674575Z] INFO - Task operator:<Task(_PythonDecoratedOperator): audit_data>

```
