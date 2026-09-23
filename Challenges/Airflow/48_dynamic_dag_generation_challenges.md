# Airflow Challenge 16: The Factory

**The Scenario:**

You have three identical AWS S3-to-Redshift load jobs that run on different schedules. Instead of writing three separate DAG files, you will write one factory script to generate all three.

**The Task:**

Write a script named `dynamic_dag_factory`.

**Requirements:**

1. Define a configuration dictionary with the following data:
   * `flight_analytics`: schedule `@daily`
   * `education_system`: schedule `@weekly`
   * `order_management`: schedule `@monthly`
2. Create a factory function that accepts a `dag_id` and a `schedule` string as arguments.
3. Inside the factory, define a `@dag` that uses those dynamic parameters.
4. Inside the DAG, define a single `@task` that prints: `"Processing pipeline for {dag_id}"`.
5. Loop through your configuration dictionary, call the factory function for each item, and assign the output to the `globals()` dictionary.
6. Run the script!

*(Hint: To verify this locally without the UI, you can usually run `airflow dags list` in your terminal to see if your three new DAGs successfully registered!)*


### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


report_configs = {
    "flight_analytics": {"schedule": "@daily"},
    "education_system": {"schedule": "@weekly"},
    "order_management": {"schedule": "@monthly"},
}


def create_dynamic_dag(dag_id, schedule_str):

    @dag(
        dag_id=dag_id,
        schedule=schedule_str,
        start_date=datetime(2026, 9, 2),
        catchup=False,
        tags=["practice"],
    )
    def dynamic_generated_dag():

        @task
        def processing_dag():
            print(f"Processing pipeline for {dag_id}")

        processing_dag()

    return dynamic_generated_dag()


for report_name, config in report_configs.items():
    globals()[report_name] = create_dynamic_dag(
        dag_id=report_name, schedule_str=config["schedule"]
    )
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::::flight_analytics:processing_dag::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=order_management/run_id=manual__2026-09-15T02:02:39.675183+00:00/task_id=processing_dag/attempt=1.log
::endgroup::
[2026-09-15T02:02:40.273808Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0a2cd-50e9-7a4d-90c8-5d9daf6275f7 dag_id=order_management task_id=processing_dag run_id=manual__2026-09-15T02:02:39.675183+00:00 try_number=1 map_index=-1
[2026-09-15T02:02:40.858028Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-15T02:02:40.860291Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/48. The Factory.py
[2026-09-15T02:02:40.957779Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=48. The Factory.py  bundle_prepare_ms=3  dag_file_parse_ms=97 
[2026-09-15T02:02:41.007986Z] INFO - ::endgroup::
[2026-09-15T02:02:41.008451Z] INFO - Task instance is in running state
[2026-09-15T02:02:41.008864Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-15T02:02:41.009371Z] INFO - Current task name:processing_dag
[2026-09-15T02:02:41.009685Z] INFO - Dag name:order_management
[2026-09-15T02:02:41.014958Z] INFO - Processing pipeline for order_management
[2026-09-15T02:02:41.014970Z] INFO - Done. Returned value was: None
[2026-09-15T02:02:41.015227Z] INFO - ::group::Post Execute
[2026-09-15T02:02:41.047768Z] INFO - ::endgroup::
[2026-09-15T02:02:41.051669Z] INFO - Task instance in success state
[2026-09-15T02:02:41.052847Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-15T02:02:41.053217Z] INFO - Task operator:<Task(_PythonDecoratedOperator): processing_dag>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::flight_analytics:processing_dag::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics/run_id=scheduled__2026-09-15T00:00:00+00:00/task_id=processing_dag/attempt=1.log
::endgroup::
[2026-09-15T02:04:30.871913Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0a2cf-02a0-72d8-bad6-e868ecf19d0b dag_id=flight_analytics task_id=processing_dag run_id=scheduled__2026-09-15T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-15T02:04:31.203415Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-15T02:04:31.204156Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/48. The Factory.py
[2026-09-15T02:04:31.256832Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=48. The Factory.py  bundle_prepare_ms=1  dag_file_parse_ms=52 
[2026-09-15T02:04:31.281552Z] INFO - Task instance is in running state
[2026-09-15T02:04:31.281999Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-15T02:04:31.281897Z] INFO - ::endgroup::
[2026-09-15T02:04:31.282369Z] INFO - Current task name:processing_dag
[2026-09-15T02:04:31.282630Z] INFO - Dag name:flight_analytics
[2026-09-15T02:04:31.283876Z] INFO - Processing pipeline for flight_analytics
[2026-09-15T02:04:31.283848Z] INFO - Done. Returned value was: None
[2026-09-15T02:04:31.283994Z] INFO - ::group::Post Execute
[2026-09-15T02:04:31.299959Z] INFO - Task instance in success state
[2026-09-15T02:04:31.300193Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-15T02:04:31.300467Z] INFO - Task operator:<Task(_PythonDecoratedOperator): processing_dag>
[2026-09-15T02:04:31.300210Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::education_system:processing_dag:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=education_system/run_id=scheduled__2026-09-13T00:00:00+00:00/task_id=processing_dag/attempt=1.log
::endgroup::
[2026-09-15T02:06:26.925925Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0a2d0-c7cd-7aa6-a4eb-b455cb150a3f dag_id=education_system task_id=processing_dag run_id=scheduled__2026-09-13T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-15T02:06:27.150982Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-15T02:06:27.152033Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/48. The Factory.py
[2026-09-15T02:06:27.201804Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=48. The Factory.py  bundle_prepare_ms=2  dag_file_parse_ms=49 
[2026-09-15T02:06:27.222968Z] INFO - Task instance is in running state
[2026-09-15T02:06:27.222966Z] INFO - ::endgroup::
[2026-09-15T02:06:27.223401Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-15T02:06:27.223691Z] INFO - Current task name:processing_dag
[2026-09-15T02:06:27.223939Z] INFO - Dag name:education_system
[2026-09-15T02:06:27.225194Z] INFO - Done. Returned value was: None
[2026-09-15T02:06:27.225653Z] INFO - Processing pipeline for education_system
[2026-09-15T02:06:27.225377Z] INFO - ::group::Post Execute
[2026-09-15T02:06:27.240862Z] INFO - Task instance in success state
[2026-09-15T02:06:27.241991Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-15T02:06:27.241291Z] INFO - ::endgroup::
[2026-09-15T02:06:27.242268Z] INFO - Task operator:<Task(_PythonDecoratedOperator): processing_dag>

```
