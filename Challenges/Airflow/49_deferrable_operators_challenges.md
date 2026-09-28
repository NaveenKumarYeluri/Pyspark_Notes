# Airflow Challenge 17: The Sleeping Giant

**The Scenario:**

You are querying a vendor API that strict rate-limits connections. You must enforce a mandatory 1-minute delay between the DAG starting and the API extraction kicking off. However, your cluster is heavily utilized, and you cannot afford to block a worker slot for 60 seconds just to wait.

**The Task:**

Write an Airflow DAG named `async_delay_pipeline`.

**Requirements:**

1. Import `TimeDeltaSensorAsync` from `airflow.sensors.time_delta`.
2. Import `timedelta` from `datetime`.
3. Inside your `@dag` function, instantiate the `TimeDeltaSensorAsync`.
   * Name the `task_id="async_1_min_delay"`.
   * Set the `delta` parameter to 1 minute.
4. Define a standard `@task` called `call_rate_limited_api()` that prints `"Proceeding with API extraction!"`.
5. Set the asynchronous delay sensor upstream of the API task.
6. Execute the DAG!

***CRITICAL LOCAL TESTING NOTE:*** *Because this uses the asynchronous engine, your local Airflow environment MUST be running the Triggerer process. If you are running Airflow locally via terminal commands, you need to open a new terminal window and run `airflow triggerer`. If the triggerer isn't running, your async task will get stuck in the `deferred` state forever!*

```python
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from datetime import datetime, timedelta


@dag(
    dag_id="async_delay_pipeline",
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["practice"],
    schedule="@daily",
)
def async_delay_pipeline_fun():
    wait_task = TimeDeltaSensorAsync(task_id="async_1_min_delay", delta=timedelta(minutes=1))

    @task
    def call_rate_limited_api():
        print("Proceeding with API extraction!")

    wait_task >> call_rate_limited_api()


async_delay_pipeline_fun()
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::async_1_min_delay:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=async_delay_pipeline/run_id=manual__2026-09-19T02:48:20.208056+00:00/task_id=async_1_min_delay/attempt=1.log
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=async_delay_pipeline/run_id=manual__2026-09-19T02:48:20.208056+00:00/task_id=async_1_min_delay/attempt=1.log.trigger.57.log
::endgroup::
[2026-09-19T02:48:21.434761Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0b790-9217-7239-9699-e1c4afa65049 dag_id=async_delay_pipeline task_id=async_1_min_delay run_id=manual__2026-09-19T02:48:20.208056+00:00 try_number=1 map_index=-1
[2026-09-19T02:48:22.001697Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-19T02:48:22.004254Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/49. The Sleeping Giant.py
[2026-09-19T02:48:22.161057Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=49. The Sleeping Giant.py  bundle_prepare_ms=4  dag_file_parse_ms=156 
[2026-09-19T02:48:22.181572Z] INFO - Task instance is in running state
[2026-09-19T02:48:22.182119Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-19T02:48:22.182511Z] INFO - Current task name:async_1_min_delay
[2026-09-19T02:48:22.182887Z] INFO - ::endgroup::
[2026-09-19T02:48:22.183162Z] INFO - Dag name:async_delay_pipeline
[2026-09-19T02:48:22.194716Z] INFO - ::group::Post Execute
[2026-09-19T02:48:22.195929Z] INFO - Pausing task as DEFERRED. 
[2026-09-19T02:48:22.260574Z] INFO - ::endgroup::
[2026-09-19T02:48:23.410909Z] INFO - trigger async_delay_pipeline/manual__2026-09-19T02:48:20.208056+00:00/async_1_min_delay/-1/1 (ID 1) starting
[2026-09-19T02:48:23.415400Z] INFO - trigger starting
[2026-09-19T02:48:23.417374Z] INFO - 54 seconds remaining; sleeping 10 seconds
[2026-09-19T02:48:33.419112Z] INFO - 44 seconds remaining; sleeping 10 seconds
[2026-09-19T02:48:43.427485Z] INFO - 34 seconds remaining; sleeping 10 seconds
[2026-09-19T02:48:53.428841Z] INFO - 24 seconds remaining; sleeping 10 seconds
[2026-09-19T02:49:03.430233Z] INFO - sleeping 1 second...
[2026-09-19T02:49:04.431250Z] INFO - sleeping 1 second...
[2026-09-19T02:49:05.432384Z] INFO - sleeping 1 second...
[2026-09-19T02:49:06.434220Z] INFO - sleeping 1 second...
[2026-09-19T02:49:07.437527Z] INFO - sleeping 1 second...
[2026-09-19T02:49:08.438493Z] INFO - sleeping 1 second...
[2026-09-19T02:49:09.439359Z] INFO - sleeping 1 second...
[2026-09-19T02:49:10.440394Z] INFO - sleeping 1 second...
[2026-09-19T02:49:11.441853Z] INFO - sleeping 1 second...
[2026-09-19T02:49:12.442798Z] INFO - sleeping 1 second...
[2026-09-19T02:49:13.444504Z] INFO - sleeping 1 second...
[2026-09-19T02:49:14.445061Z] INFO - sleeping 1 second...
[2026-09-19T02:49:15.445904Z] INFO - sleeping 1 second...
[2026-09-19T02:49:16.447195Z] INFO - sleeping 1 second...
[2026-09-19T02:49:17.447983Z] INFO - sleeping 1 second...
[2026-09-19T02:49:18.450984Z] INFO - yielding event with payload DateTime(2026, 9, 19, 2, 49, 18, tzinfo=Timezone('UTC'))
[2026-09-19T02:49:18.463841Z] INFO - Trigger fired event name=async_delay_pipeline/manual__2026-09-19T02:48:20.208056+00:00/async_1_min_delay/-1/1 (ID 1)  result=TriggerEvent<DateTime(2026, 9, 19, 2, 49, 18, tzinfo=Timezone('UTC'))> 
[2026-09-19T02:49:18.468072Z] INFO - trigger completed name=async_delay_pipeline/manual__2026-09-19T02:48:20.208056+00:00/async_1_min_delay/-1/1 (ID 1) 
[2026-09-19T02:49:19.388516Z] INFO - ::group::Pre Execute
[2026-09-19T02:49:19.665945Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-19T02:49:19.667456Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/49. The Sleeping Giant.py
[2026-09-19T02:49:19.740219Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=49. The Sleeping Giant.py  bundle_prepare_ms=2  dag_file_parse_ms=73 
[2026-09-19T02:49:19.745518Z] INFO - Task instance is in running state
[2026-09-19T02:49:19.745893Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-19T02:49:19.746295Z] INFO - Current task name:async_1_min_delay
[2026-09-19T02:49:19.746815Z] INFO - Dag name:async_delay_pipeline
[2026-09-19T02:49:19.747467Z] INFO - ::endgroup::
[2026-09-19T02:49:19.747744Z] INFO - ::group::Post Execute
[2026-09-19T02:49:19.784071Z] INFO - Task instance in success state
[2026-09-19T02:49:19.784423Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-19T02:49:19.784790Z] INFO - Task operator:<Task(TimeDeltaSensorAsync): async_1_min_delay>
[2026-09-19T02:49:19.784577Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::async_1_min_delay:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=async_delay_pipeline/run_id=manual__2026-09-19T02:48:20.208056+00:00/task_id=call_rate_limited_api/attempt=1.log
::endgroup::
[2026-09-19T02:49:20.514511Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0b790-9218-7869-b0ad-2587cc504b7e dag_id=async_delay_pipeline task_id=call_rate_limited_api run_id=manual__2026-09-19T02:48:20.208056+00:00 try_number=1 map_index=-1
[2026-09-19T02:49:20.910712Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-19T02:49:20.911927Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/49. The Sleeping Giant.py
[2026-09-19T02:49:20.989929Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=49. The Sleeping Giant.py  bundle_prepare_ms=2  dag_file_parse_ms=78 
[2026-09-19T02:49:21.024940Z] INFO - Task instance is in running state
[2026-09-19T02:49:21.025452Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-19T02:49:21.025902Z] INFO - Current task name:call_rate_limited_api
[2026-09-19T02:49:21.025657Z] INFO - ::endgroup::
[2026-09-19T02:49:21.026241Z] INFO - Dag name:async_delay_pipeline
[2026-09-19T02:49:21.027877Z] INFO - Proceeding with API extraction!
[2026-09-19T02:49:21.027898Z] INFO - Done. Returned value was: None
[2026-09-19T02:49:21.028085Z] INFO - ::group::Post Execute
[2026-09-19T02:49:21.049901Z] INFO - ::endgroup::
[2026-09-19T02:49:21.050655Z] INFO - Task instance in success state
[2026-09-19T02:49:21.051031Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-19T02:49:21.051350Z] INFO - Task operator:<Task(_PythonDecoratedOperator): call_rate_limited_api>

```
