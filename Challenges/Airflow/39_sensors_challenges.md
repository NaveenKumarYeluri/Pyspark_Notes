# Airflow Challenge 7: The Patient Watcher

**The Scenario:**

You are building a pipeline that processes an external API. The API is notoriously slow and sometimes isn't ready when your DAG starts. You need to build a sensor that "pokes" the system until it returns a specific ready code before allowing your extraction task to run.

**The Task:**

Write an Airflow DAG named `api_sensor_pipeline`.

**Requirements:**

1. Import `PokeReturnValue` from `airflow.sensors.base`.
2. Define a `@task.sensor` named `check_api_status()`.
   * Set it to poke every `3` seconds, with a timeout of `15` seconds.
   * Inside the function, generate a random integer between 1 and 5 (using `import random`).
   * If the number is `5`, print `"API is ready!"` and return `PokeReturnValue(is_done=True)`.
   * Otherwise, print `"API not ready yet. Poking again..."` and return `PokeReturnValue(is_done=False)`.
3. Define a standard `@task` called `download_payload()` that prints `"Downloading data from API..."`.
4. In the `@dag` function:
   * Set `check_api_status()` upstream of `download_payload()`.
5. Execute the DAG. Look at the logs for your sensor task; you should see it "poke" multiple times, sleep, and try again until it hits the number 5!


### My Solution:

```python
import random
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from datetime import datetime


@task.sensor(poke_interval=3, timeout=15, mode="poke")
def check_api_status():
    number = random.randint(1, 5)

    if number == 5:
        print("API is ready!")
        return PokeReturnValue(is_done=True)
    else:
        print("API not ready yet. Poking again...")
        return PokeReturnValue(is_done=False)


@task
def download_payload():
    print("Downloading data from API...")


@dag(
    dag_id="api_sensor_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 8, 24),
    catchup=False,
    tags=["practice"],
)
def api_sensor_pipeline_fun():
    check_api_status() >> download_payload()


api_sensor_pipeline_fun()
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::check_api_status:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=api_sensor_pipeline/run_id=manual__2026-08-24T05:53:36.668748+00:00/task_id=check_api_status/attempt=1.log
::endgroup::
[2026-08-24T05:53:37.766293Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03254-d9bf-7aff-9791-3af0b9e85fd3 dag_id=api_sensor_pipeline task_id=check_api_status run_id=manual__2026-08-24T05:53:36.668748+00:00 try_number=1 map_index=-1
[2026-08-24T05:53:38.406059Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:53:38.407863Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/39. The Patient Watcher.py
[2026-08-24T05:53:38.526206Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-08-24T05:53:38.528907Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=39. The Patient Watcher.py  bundle_prepare_ms=3  dag_file_parse_ms=121 
[2026-08-24T05:53:38.572871Z] INFO - Task instance is in running state
[2026-08-24T05:53:38.573704Z] INFO - ::endgroup::
[2026-08-24T05:53:38.574210Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:53:38.574930Z] INFO - Current task name:check_api_status
[2026-08-24T05:53:38.574300Z] INFO - Poking callable: <function check_api_status at 0x7efc5fd86400>
[2026-08-24T05:53:38.575230Z] INFO - Dag name:api_sensor_pipeline
[2026-08-24T05:53:38.575630Z] INFO - API not ready yet. Poking again...
[2026-08-24T05:53:41.575088Z] INFO - Poking callable: <function check_api_status at 0x7efc5fd86400>
[2026-08-24T05:53:41.575836Z] INFO - API not ready yet. Poking again...
[2026-08-24T05:53:44.575533Z] INFO - Poking callable: <function check_api_status at 0x7efc5fd86400>
[2026-08-24T05:53:44.586237Z] INFO - API not ready yet. Poking again...
[2026-08-24T05:53:47.576172Z] INFO - Poking callable: <function check_api_status at 0x7efc5fd86400>
[2026-08-24T05:53:47.577281Z] INFO - API not ready yet. Poking again...
[2026-08-24T05:53:50.576659Z] INFO - Poking callable: <function check_api_status at 0x7efc5fd86400>
[2026-08-24T05:53:50.587589Z] INFO - API not ready yet. Poking again...
[2026-08-24T05:53:53.577205Z] INFO - Poking callable: <function check_api_status at 0x7efc5fd86400>
[2026-08-24T05:53:53.577662Z] INFO - Success criteria met. Exiting.
[2026-08-24T05:53:53.578694Z] INFO - API is ready!
[2026-08-24T05:53:53.578211Z] INFO - ::group::Post Execute
[2026-08-24T05:53:53.615342Z] INFO - Task instance in success state
[2026-08-24T05:53:53.615660Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:53:53.615934Z] INFO - Task operator:<Task(DecoratedSensorOperator): check_api_status>
[2026-08-24T05:53:53.615718Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::download_payload::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=api_sensor_pipeline/run_id=manual__2026-08-24T05:53:36.668748+00:00/task_id=download_payload/attempt=1.log
::endgroup::
[2026-08-24T05:53:54.454429Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a03254-d9c0-7b8f-a76e-c8e235c98fee dag_id=api_sensor_pipeline task_id=download_payload run_id=manual__2026-08-24T05:53:36.668748+00:00 try_number=1 map_index=-1
[2026-08-24T05:53:54.915069Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-24T05:53:54.916332Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/39. The Patient Watcher.py
[2026-08-24T05:53:54.975869Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-08-24T05:53:54.977281Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=39. The Patient Watcher.py  bundle_prepare_ms=3  dag_file_parse_ms=61 
[2026-08-24T05:53:55.014007Z] INFO - ::endgroup::
[2026-08-24T05:53:55.015274Z] INFO - Task instance is in running state
[2026-08-24T05:53:55.016214Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-24T05:53:55.016467Z] INFO - Current task name:download_payload
[2026-08-24T05:53:55.016682Z] INFO - Dag name:api_sensor_pipeline
[2026-08-24T05:53:55.016864Z] INFO - Done. Returned value was: None
[2026-08-24T05:53:55.017503Z] INFO - Downloading data from API...
[2026-08-24T05:53:55.017104Z] INFO - ::group::Post Execute
[2026-08-24T05:53:55.036856Z] INFO - Task instance in success state
[2026-08-24T05:53:55.037126Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-24T05:53:55.037469Z] INFO - Task operator:<Task(_PythonDecoratedOperator): download_payload>
[2026-08-24T05:53:55.037259Z] INFO - ::endgroup::

```
