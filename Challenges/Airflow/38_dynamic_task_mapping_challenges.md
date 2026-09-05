# Airflow Challenge 6: The Parallel Batch Processor

**The Scenario:**

You need to process daily partitions for three regional data stores (`["north_region", "south_region", "east_region"]`). Instead of processing them sequentially in a single Python task, you must process each region as an independent, parallel task instance so that a failure in one region doesn't block the others.

**The Task:**

Write an Airflow DAG named `dynamic_partition_pipeline`.

**Requirements:**

1. Create a `@task` called `get_regions()` that returns a list: `["north_region", "south_region", "east_region"]`.
2. Create a `@task` called `process_region(region_name: str)`. Inside, print: `f"Completed processing for partition: {region_name}"`.
3. Create a `@task` called `notify_completion()`. Inside, print: `"All regional partitions processed successfully."`.
4. In the `@dag` function:
   * Retrieve the regions list using `get_regions()`.
   * Dynamically map `process_region` across the regions using `.expand(region_name=...)`.
   * Ensure `notify_completion()` runs after all dynamic task instances finish.
5. Execute the DAG and inspect the mapped instances in your task execution logs.


### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@task
def get_regions() -> list[str]:
    return ["north_region", "south_region", "east_region"]


@task
def process_region(region_name: str):
    print(f"Completed processing for partition: {region_name}")


@task
def notify_completion():
    print("All regional partitions processed successfully.")


@dag(
    dag_id="dynamic_partition_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 8, 24),
    catchup=False,
    tags=["practice"],
)
def dynamic_partition_pipeline_fun():
    regions = get_regions()
    process_region.expand(region_name=regions) >> notify_completion()


dynamic_partition_pipeline_fun()
```

### My Output Verification:

```

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::get_regions:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=dynamic_partition_pipeline/run_id=manual__2026-08-24T05:22:27.406720+00:00/task_id=get_regions/attempt=1.log
::endgroup::
[2026-08-24T05:22:28.505526Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03238-5413-7d66-b4b4-e434d2e608e2 dag_id=dynamic_partition_pipeline task_id=get_regions run_id=manual__2026-08-24T05:22:27.406720+00:00 try_number=1 map_index=-1
[2026-08-24T05:22:29.426395Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:22:29.428782Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/38. The Parallel Batch Processor.py
[2026-08-24T05:22:29.548350Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=38. The Parallel Batch Processor.py  bundle_prepare_ms=7  dag_file_parse_ms=119 
[2026-08-24T05:22:29.600897Z] INFO - Task instance is in running state
[2026-08-24T05:22:29.601438Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:22:29.601968Z] INFO - Current task name:get_regions
[2026-08-24T05:22:29.601326Z] INFO - ::endgroup::
[2026-08-24T05:22:29.602324Z] INFO - Dag name:dynamic_partition_pipeline
[2026-08-24T05:22:29.604445Z] INFO - Done. Returned value was: ['north_region', 'south_region', 'east_region']
[2026-08-24T05:22:29.604770Z] INFO - ::group::Post Execute
[2026-08-24T05:22:29.605348Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a03238-5413-7d66-b4b4-e434d2e608e2'), task_id='get_regions', dag_id='dynamic_partition_pipeline', run_id='manual__2026-08-24T05:22:27.406720+00:00', try_number=1, dag_version_id=UUID('01a03233-8883-7e7a-8f72-a3014b31c9ee'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-95afc8ce7b96dcca52be523bf7dca906-9fa453e0f072fad5-00'}, queue='default', task=<Task(_PythonDecoratedOperator): get_regions>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 8, 24, 5, 22, 28, 577792, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-24T05:22:29.690656Z] INFO - Task instance in success state
[2026-08-24T05:22:29.690552Z] INFO - ::endgroup::
[2026-08-24T05:22:29.691246Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:22:29.692047Z] INFO - Task operator:<Task(_PythonDecoratedOperator): get_regions>



:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::process_region::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=dynamic_partition_pipeline/run_id=manual__2026-08-24T05:22:27.406720+00:00/task_id=process_region/map_index=0/attempt=1.log
::endgroup::
[2026-08-24T05:22:30.597180Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03238-5414-7b2c-a1f2-80c05386c846 dag_id=dynamic_partition_pipeline task_id=process_region run_id=manual__2026-08-24T05:22:27.406720+00:00 try_number=1 map_index=0
[2026-08-24T05:22:31.840514Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:22:31.843291Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/38. The Parallel Batch Processor.py
[2026-08-24T05:22:31.993465Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=38. The Parallel Batch Processor.py  bundle_prepare_ms=4  dag_file_parse_ms=150 
[2026-08-24T05:22:32.107518Z] INFO - ::endgroup::
[2026-08-24T05:22:32.111922Z] INFO - Task instance is in running state
[2026-08-24T05:22:32.112511Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:22:32.112827Z] INFO - Current task name:process_region
[2026-08-24T05:22:32.113083Z] INFO - Dag name:dynamic_partition_pipeline
[2026-08-24T05:22:32.114033Z] INFO - Completed processing for partition: north_region
[2026-08-24T05:22:32.114025Z] INFO - Done. Returned value was: None
[2026-08-24T05:22:32.114292Z] INFO - ::group::Post Execute
[2026-08-24T05:22:32.183600Z] INFO - ::endgroup::
[2026-08-24T05:22:32.185425Z] INFO - Task instance in success state
[2026-08-24T05:22:32.186433Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:22:32.187012Z] INFO - Task operator:<Task(_PythonDecoratedOperator): process_region>


::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=dynamic_partition_pipeline/run_id=manual__2026-08-24T05:22:27.406720+00:00/task_id=process_region/map_index=1/attempt=1.log
::endgroup::
[2026-08-24T05:22:31.085580Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03238-5f2a-7667-876e-a64fa0ae7d36 dag_id=dynamic_partition_pipeline task_id=process_region run_id=manual__2026-08-24T05:22:27.406720+00:00 try_number=1 map_index=1
[2026-08-24T05:22:32.093765Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:22:32.098782Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/38. The Parallel Batch Processor.py
[2026-08-24T05:22:32.177606Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=38. The Parallel Batch Processor.py  bundle_prepare_ms=4  dag_file_parse_ms=81 
[2026-08-24T05:22:32.285290Z] INFO - Task instance is in running state
[2026-08-24T05:22:32.285687Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:22:32.285870Z] INFO - ::endgroup::
[2026-08-24T05:22:32.286039Z] INFO - Current task name:process_region
[2026-08-24T05:22:32.286316Z] INFO - Dag name:dynamic_partition_pipeline
[2026-08-24T05:22:32.287808Z] INFO - Completed processing for partition: south_region
[2026-08-24T05:22:32.287809Z] INFO - Done. Returned value was: None
[2026-08-24T05:22:32.287965Z] INFO - ::group::Post Execute
[2026-08-24T05:22:32.309718Z] INFO - ::endgroup::
[2026-08-24T05:22:32.310594Z] INFO - Task instance in success state
[2026-08-24T05:22:32.311035Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:22:32.311307Z] INFO - Task operator:<Task(_PythonDecoratedOperator): process_region>


::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=dynamic_partition_pipeline/run_id=manual__2026-08-24T05:22:27.406720+00:00/task_id=process_region/map_index=2/attempt=1.log
::endgroup::
[2026-08-24T05:22:30.709816Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03238-5f2b-7375-b549-4e6874e8d155 dag_id=dynamic_partition_pipeline task_id=process_region run_id=manual__2026-08-24T05:22:27.406720+00:00 try_number=1 map_index=2
[2026-08-24T05:22:31.976609Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:22:31.978073Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/38. The Parallel Batch Processor.py
[2026-08-24T05:22:32.126941Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=38. The Parallel Batch Processor.py  bundle_prepare_ms=4  dag_file_parse_ms=148 
[2026-08-24T05:22:32.193363Z] INFO - Task instance is in running state
[2026-08-24T05:22:32.193947Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:22:32.193936Z] INFO - ::endgroup::
[2026-08-24T05:22:32.194656Z] INFO - Current task name:process_region
[2026-08-24T05:22:32.195054Z] INFO - Dag name:dynamic_partition_pipeline
[2026-08-24T05:22:32.201852Z] INFO - Completed processing for partition: east_region
[2026-08-24T05:22:32.202551Z] INFO - Done. Returned value was: None
[2026-08-24T05:22:32.202826Z] INFO - ::group::Post Execute
[2026-08-24T05:22:32.255556Z] INFO - ::endgroup::
[2026-08-24T05:22:32.258315Z] INFO - Task instance in success state
[2026-08-24T05:22:32.260771Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:22:32.261239Z] INFO - Task operator:<Task(_PythonDecoratedOperator): process_region>


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::notify_completion:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=dynamic_partition_pipeline/run_id=manual__2026-08-24T05:22:27.406720+00:00/task_id=notify_completion/attempt=1.log
::endgroup::
[2026-08-24T05:22:33.411813Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03238-5415-704f-bec4-ed0ada37fa12 dag_id=dynamic_partition_pipeline task_id=notify_completion run_id=manual__2026-08-24T05:22:27.406720+00:00 try_number=1 map_index=-1
[2026-08-24T05:22:34.103808Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:22:34.105985Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/38. The Parallel Batch Processor.py
[2026-08-24T05:22:34.192482Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=38. The Parallel Batch Processor.py  bundle_prepare_ms=3  dag_file_parse_ms=86 
[2026-08-24T05:22:34.241545Z] INFO - Task instance is in running state
[2026-08-24T05:22:34.242084Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:22:34.242596Z] INFO - Current task name:notify_completion
[2026-08-24T05:22:34.242165Z] INFO - ::endgroup::
[2026-08-24T05:22:34.243121Z] INFO - Dag name:dynamic_partition_pipeline
[2026-08-24T05:22:34.244893Z] INFO - All regional partitions processed successfully.
[2026-08-24T05:22:34.244909Z] INFO - Done. Returned value was: None
[2026-08-24T05:22:34.245152Z] INFO - ::group::Post Execute
[2026-08-24T05:22:34.289810Z] INFO - Task instance in success state
[2026-08-24T05:22:34.290199Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:22:34.290883Z] INFO - Task operator:<Task(_PythonDecoratedOperator): notify_completion>
[2026-08-24T05:22:34.290237Z] INFO - ::endgroup::

```
