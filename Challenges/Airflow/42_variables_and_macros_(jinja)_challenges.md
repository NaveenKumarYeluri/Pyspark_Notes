# Airflow Challenge 10: The Dynamic Configurator

**The Scenario:**

You are building an extraction pipeline that hits a weather API. The API requires you to pass the specific date you want data for. Additionally, your data science team frequently changes the target "city" they want you to pull data for, so you want to store the city name as a global Airflow Variable rather than hardcoding it in the DAG.

**The Task:**

Write a DAG named `dynamic_weather_pipeline`.

**Requirements:**

1. Import `Variable` from `airflow.models`.
2. Define a `@task` called `extract_weather_data()`.
3. Configure the task to automatically receive Airflow's `logical_date` string as a parameter.
4. Inside the task, use `Variable.get()` to fetch a variable named `"target_city"`. Provide a `default_var="Hyderabad"` so the code doesn't crash if the variable isn't set in your local UI yet.
5. Have the task print this exact message: `"Extracting weather data for {city} on {date}."` (injecting your two dynamic variables).
6. Create the `@dag` function, set it to run `@daily`, and execute the pipeline. 
7. Check your logs to ensure the date (which should match your run date) and the city (Hyderabad) successfully populated the print statement!


### My Solution:

```python
from airflow.sdk import dag, task, Variable
from datetime import datetime


@task
def extract_weather_data(logical_date: str):
    city = Variable.get("target_city", default="Hyderabad")
    print(f"Extracting weather data for {city} on {logical_date}.")


@dag(
    dag_id="dynamic_weather_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["practice"],
)
def dynamic_weather_pipeline_fun():
    extract_weather_data()


dynamic_weather_pipeline_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_weather_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=dynamic_weather_pipeline/run_id=scheduled__2026-09-05T00:00:00+00:00/task_id=extract_weather_data/attempt=1.log
::endgroup::
[2026-09-05T03:09:13.039833Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06f8a-a74d-7678-848b-399be416db14 dag_id=dynamic_weather_pipeline task_id=extract_weather_data run_id=scheduled__2026-09-05T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-05T03:09:13.363052Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T03:09:13.363931Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/42. The Dynamic Configurator.py
[2026-09-05T03:09:13.433775Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=42. The Dynamic Configurator.py  bundle_prepare_ms=1  dag_file_parse_ms=69 
[2026-09-05T03:09:13.465458Z] INFO - Task instance is in running state
[2026-09-05T03:09:13.465899Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T03:09:13.466279Z] INFO - Current task name:extract_weather_data
[2026-09-05T03:09:13.466527Z] INFO - Dag name:dynamic_weather_pipeline
[2026-09-05T03:09:13.466028Z] INFO - ::endgroup::
[2026-09-05T03:09:13.486121Z] INFO - Done. Returned value was: None
[2026-09-05T03:09:13.486338Z] INFO - ::group::Post Execute
[2026-09-05T03:09:13.487168Z] INFO - Extracting weather data for Guntur on 2026-09-05 00:00:00+00:00.
[2026-09-05T03:09:13.513477Z] INFO - Task instance in success state
[2026-09-05T03:09:13.513839Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T03:09:13.514301Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_weather_data>
[2026-09-05T03:09:13.514291Z] INFO - ::endgroup::
[2026-09-05T03:16:04.324118Z] INFO - ::group::Pre Execute
[2026-09-05T03:16:04.654197Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T03:16:04.655263Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/42. The Dynamic Configurator.py
[2026-09-05T03:16:04.695532Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=42. The Dynamic Configurator.py  bundle_prepare_ms=2  dag_file_parse_ms=40 
[2026-09-05T03:16:04.742466Z] INFO - Task instance is in running state
[2026-09-05T03:16:04.742817Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T03:16:04.743171Z] INFO - Current task name:extract_weather_data
[2026-09-05T03:16:04.743406Z] INFO - Dag name:dynamic_weather_pipeline
[2026-09-05T03:16:04.743100Z] INFO - ::endgroup::
[2026-09-05T03:16:04.755261Z] INFO - Extracting weather data for Guntur on 2026-09-05 00:00:00+00:00.
[2026-09-05T03:16:04.755293Z] INFO - Done. Returned value was: None
[2026-09-05T03:16:04.755491Z] INFO - ::group::Post Execute
[2026-09-05T03:16:04.771057Z] INFO - Task instance in success state
[2026-09-05T03:16:04.771365Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T03:16:04.771719Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_weather_data>
[2026-09-05T03:16:04.771503Z] INFO - ::endgroup::
```
