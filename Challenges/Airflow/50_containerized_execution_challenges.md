# Airflow Challenge 18: The Isolated Container

**The Scenario:**

You need to run a highly sensitive data transformation that requires a pristine, isolated Python environment so it doesn't conflict with any other packages installed on your Airflow workers.

**The Task:**

Write an Airflow DAG named `isolated_docker_pipeline`.

**Requirements:**

1. Import the `@task` decorator (which contains the nested `.docker` decorator). 
2. Create a task using `@task.docker(image="python:3.10-slim")`.
3. Inside the task, write a simple Python function that prints `"Executing secure transformation inside Docker!"` and returns `True`.
4. Create the `@dag` function, set it to run `@daily`, and execute the pipeline.

***CRITICAL LOCAL TESTING NOTE:*** *To successfully run this task, your local machine MUST have a Docker daemon running (like Docker Desktop). If you do not have Docker installed, Airflow will successfully parse the DAG but the task will fail when it attempts to pull the image. Even if it fails, share your code and logs—validating the architecture is what counts here!*


### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="isolated_docker_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 2),
    tags=["practice"],
    catchup=False,
)
def isolated_docker_pipeline_fun():

    @task.docker(image="python:3.10-slim")
    def custom_py_package():
        print("Executing secure transformation inside Docker!")
        return True

    custom_py_package()


isolated_docker_pipeline_fun()
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::custom_py_package:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=isolated_docker_pipeline/run_id=scheduled__2026-09-20T00:00:00+00:00/task_id=custom_py_package/attempt=1.log
::endgroup::
[2026-09-20T06:37:22.481118Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0bd88-9b07-709d-bc1c-06578a6ad295 dag_id=isolated_docker_pipeline task_id=custom_py_package run_id=scheduled__2026-09-20T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-20T06:37:33.747351Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-20T06:37:33.748873Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/50. The Isolated Container.py
[2026-09-20T06:37:34.031352Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=50. The Isolated Container.py  bundle_prepare_ms=2  dag_file_parse_ms=282 
[2026-09-20T06:37:34.099879Z] INFO - Task instance is in running state
[2026-09-20T06:37:34.100015Z] INFO - ::endgroup::
[2026-09-20T06:37:34.101224Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-20T06:37:34.103008Z] INFO - Current task name:custom_py_package
[2026-09-20T06:37:34.103442Z] INFO - Dag name:isolated_docker_pipeline
[2026-09-20T06:37:34.143725Z] ERROR - Failed to establish connection to Docker host unix://var/run/docker.sock: Error while fetching server API version: ('Connection aborted.', FileNotFoundError(2, 'No such file or directory'))
[2026-09-20T06:37:34.144227Z] ERROR - Task failed with exceptionAirflowException: Failed to establish connection to any given Docker hosts.
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 1579 in _run_task_and_map_outcome
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2234 in _execute_task
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2192 in _run_execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/docker/decorators/docker.py, line 189 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/decorator.py, line 398 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/docker/operators/docker.py, line 498 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/docker/operators/docker.py, line 365 in cli
    File /usr/lib/python3.14/functools.py, line 1126 in __get__
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/docker/hooks/docker.py, line 160 in api_client

[2026-09-20T06:37:34.144782Z] INFO - ::group::Post Execute
[2026-09-20T06:37:34.149989Z] INFO - Task instance in failure state
[2026-09-20T06:37:34.149789Z] INFO - ::endgroup::
[2026-09-20T06:37:34.150504Z] INFO - Task start
[2026-09-20T06:37:34.150828Z] INFO - Task:<Task(_DockerDecoratedOperator): custom_py_package>
[2026-09-20T06:37:34.151096Z] INFO - Failure caused by Failed to establish connection to any given Docker hosts.

```
