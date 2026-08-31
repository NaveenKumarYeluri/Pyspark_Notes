# Airflow Challenge 4: The Traffic Cop

**The Scenario:**

You are orchestrating a nightly file extraction. Sometimes the vendor doesn't upload a file, meaning the extraction returns `0` records. If it returns records, you want to process them. If it returns 0, you want to halt the pipeline and log an alert.

**The Task:**

Write an Airflow DAG named `conditional_file_pipeline`.

**Requirements:**

1. Create a `@task` called `extract_file()`. Make it return `0` (simulating an empty file delivery).
2. Create two standard `@task`s:
    * `process_data()`: Prints `"Processing data..."`
    * `log_empty_file()`: Prints `"Alert: No data received today."`
3. Create a `@task.branch` called `route_pipeline(record_count)`. 
    * If `record_count > 0`, return `"process_data"`.
    * If `record_count == 0`, return `"log_empty_file"`.
4. Create the `@dag` function.
    * Pull the count from `extract_file()`.
    * Pass the count into `route_pipeline()`.
    * Use the `>>` operator to set `route_pipeline()` upstream of *both* `process_data()` and `log_empty_file()`.
5. Run the DAG and verify in the logs that `log_empty_file` executes, and `process_data` is skipped!

*(Expert Tip: Since you are on Airflow 3, feel free to try importing from `airflow.sdk` instead of `airflow.decorators` if you want to silence that deprecation warning!)*


### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@task
def extract_file():
    return 0


@task
def process_data():
    print("Processing data...")


@task
def log_empty_file():
    print("Alert: No data received today.")


@task.branch
def route_pipeline(record_count):
    if record_count > 0:
        return "process_data"
    elif record_count == 0:
        return "log_empty_file"


@dag(
    dag_id="conditional_file_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 8, 21),
    catchup=False,
    tags=["practice"],
)
def conditional_file_pipeline_fun():
    cnt = extract_file()
    route = route_pipeline(cnt)
    route >> [process_data(), log_empty_file()]


conditional_file_pipeline_fun()
```

### My Output Verification:

```

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_file::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline/run_id=manual__2026-08-21T05:07:29.772632+00:00/task_id=extract_file/attempt=1.log
::endgroup::
[2026-08-21T05:07:31.954947Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a022b7-8db1-7321-81f0-98ac18d915fd dag_id=conditional_file_pipeline task_id=extract_file run_id=manual__2026-08-21T05:07:29.772632+00:00 try_number=1 map_index=-1
[2026-08-21T05:07:39.946158Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-21T05:07:39.947244Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/36. The Traffic Cop.py
[2026-08-21T05:07:39.999657Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=36. The Traffic Cop.py  bundle_prepare_ms=1  dag_file_parse_ms=52 
[2026-08-21T05:07:40.056456Z] INFO - Task instance is in running state
[2026-08-21T05:07:40.056807Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-21T05:07:40.056905Z] INFO - ::endgroup::
[2026-08-21T05:07:40.057144Z] INFO - Current task name:extract_file
[2026-08-21T05:07:40.057378Z] INFO - Dag name:conditional_file_pipeline
[2026-08-21T05:07:40.059046Z] INFO - Done. Returned value was: 0
[2026-08-21T05:07:40.059259Z] INFO - ::group::Post Execute
[2026-08-21T05:07:40.059498Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a022b7-8db1-7321-81f0-98ac18d915fd'), task_id='extract_file', dag_id='conditional_file_pipeline', run_id='manual__2026-08-21T05:07:29.772632+00:00', try_number=1, dag_version_id=UUID('01a022b7-234f-79f4-8f7b-02999cd50df3'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-2e49784033a8bf193fe15b28b11423f8-400f5b75aa6056e1-00'}, queue='default', task=<Task(_PythonDecoratedOperator): extract_file>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 8, 21, 5, 7, 39, 584997, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-21T05:07:40.140362Z] INFO - Task instance in success state
[2026-08-21T05:07:40.140592Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-21T05:07:40.140897Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_file>
[2026-08-21T05:07:40.140798Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::route_pipeline:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline/run_id=manual__2026-08-21T05:07:29.772632+00:00/task_id=route_pipeline/attempt=1.log
::endgroup::
[2026-08-21T05:07:40.662354Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a022b7-8db2-7f3d-b516-0ae686f5c645 dag_id=conditional_file_pipeline task_id=route_pipeline run_id=manual__2026-08-21T05:07:29.772632+00:00 try_number=1 map_index=-1
[2026-08-21T05:07:40.960500Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-21T05:07:40.961559Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/36. The Traffic Cop.py
[2026-08-21T05:07:41.012899Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=36. The Traffic Cop.py  bundle_prepare_ms=1  dag_file_parse_ms=51 
[2026-08-21T05:07:41.072768Z] INFO - Task instance is in running state
[2026-08-21T05:07:41.073192Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-21T05:07:41.073770Z] INFO - Current task name:route_pipeline
[2026-08-21T05:07:41.073596Z] INFO - ::endgroup::
[2026-08-21T05:07:41.074192Z] INFO - Dag name:conditional_file_pipeline
[2026-08-21T05:07:41.076035Z] INFO - Done. Returned value was: log_empty_file
[2026-08-21T05:07:41.076438Z] INFO - Branch into log_empty_file
[2026-08-21T05:07:41.076782Z] INFO - Following branch {'log_empty_file'}
[2026-08-21T05:07:41.077240Z] INFO - Skipping tasks [('process_data', -1)]
[2026-08-21T05:07:41.107407Z] INFO - ::group::Post Execute
[2026-08-21T05:07:41.107563Z] INFO - Skipping downstream tasks.
[2026-08-21T05:07:41.145078Z] INFO - Task instance in success state
[2026-08-21T05:07:41.145430Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-21T05:07:41.145783Z] INFO - Task operator:<Task(_BranchPythonDecoratedOperator): route_pipeline>
[2026-08-21T05:07:41.145439Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::log_empty_file:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline/run_id=manual__2026-08-21T05:07:29.772632+00:00/task_id=log_empty_file/attempt=1.log
::endgroup::
[2026-08-21T05:07:41.779469Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a022b7-8db4-7fdc-826e-f8ad1a236f4e dag_id=conditional_file_pipeline task_id=log_empty_file run_id=manual__2026-08-21T05:07:29.772632+00:00 try_number=1 map_index=-1
[2026-08-21T05:07:42.079476Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-21T05:07:42.080778Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/36. The Traffic Cop.py
[2026-08-21T05:07:42.137301Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=36. The Traffic Cop.py  bundle_prepare_ms=2  dag_file_parse_ms=56 
[2026-08-21T05:07:42.166652Z] INFO - Task instance is in running state
[2026-08-21T05:07:42.167049Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-21T05:07:42.167407Z] INFO - Current task name:log_empty_file
[2026-08-21T05:07:42.167739Z] INFO - Dag name:conditional_file_pipeline
[2026-08-21T05:07:42.167280Z] INFO - ::endgroup::
[2026-08-21T05:07:42.169897Z] INFO - Alert: No data received today.
[2026-08-21T05:07:42.169930Z] INFO - Done. Returned value was: None
[2026-08-21T05:07:42.170140Z] INFO - ::group::Post Execute
[2026-08-21T05:07:42.200043Z] INFO - Task instance in success state
[2026-08-21T05:07:42.200366Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-21T05:07:42.200701Z] INFO - Task operator:<Task(_PythonDecoratedOperator): log_empty_file>
[2026-08-21T05:07:42.200460Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::process_data:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

No run

```
