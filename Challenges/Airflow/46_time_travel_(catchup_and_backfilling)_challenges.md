# Airflow Challenge 14: The Missing Weekend

**The Scenario:**

Your team realized that a critical daily sales aggregation pipeline was accidentally paused right before the weekend. Today is September 7, 2026, and you need to process the missing data for September 4, 5, and 6. Instead of running a manual script, you will leverage Airflow's Catchup feature to process the backlog automatically.

**The Task:**

Write an Airflow DAG named `weekend_catchup_pipeline`.

**Requirements:**

1. Define a `@task` called `aggregate_sales_data(logical_date: str)`.
2. Inside the task, print exactly: `f"Running historical sales aggregation for: {logical_date}"`
3. Define the `@dag` function.
    * Set `schedule="@daily"`.
    * Set `start_date=datetime(2026, 9, 4)`.
    * Set `catchup=True`.
4. Execute the DAG. (Once the DAG is active, Airflow should automatically trigger multiple runs).
5. Check your UI or logs. You should see distinct executions for the 4th, 5th, and 6th, proving that the pipeline correctly processed the historical backlog without you having to change the code!


### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@task
def aggregate_sales_data(logical_date: str):
    print(f"Running historical sales aggregation for: {logical_date}")


@dag(
    dag_id="weekend_catchup_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 4),
    tags=["practice"],
    catchup=True,
)
def weekend_catchup_pipeline_fun():
    aggregate_sales_data()


weekend_catchup_pipeline_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::aggregate_sales_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=weekend_catchup_pipeline/run_id=scheduled__2026-09-04T00:00:00+00:00/task_id=aggregate_sales_data/attempt=1.log
::endgroup::
[2026-09-08T05:08:19.117016Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07f6a-c219-7d21-8d5e-095ba1e144b0 dag_id=weekend_catchup_pipeline task_id=aggregate_sales_data run_id=scheduled__2026-09-04T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-08T05:08:28.676120Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T05:08:28.678204Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/46. The Missing Weekend.py
[2026-09-08T05:08:28.827241Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=46. The Missing Weekend.py  bundle_prepare_ms=3  dag_file_parse_ms=149 
[2026-09-08T05:08:29.461878Z] INFO - Task instance is in running state
[2026-09-08T05:08:29.465403Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T05:08:29.465803Z] INFO - Current task name:aggregate_sales_data
[2026-09-08T05:08:29.466000Z] INFO - ::endgroup::
[2026-09-08T05:08:29.467628Z] INFO - Dag name:weekend_catchup_pipeline
[2026-09-08T05:08:29.474718Z] INFO - Running historical sales aggregation for: 2026-09-04 00:00:00+00:00
[2026-09-08T05:08:29.476078Z] INFO - Done. Returned value was: None
[2026-09-08T05:08:29.476340Z] INFO - ::group::Post Execute
[2026-09-08T05:08:29.783449Z] INFO - ::endgroup::
[2026-09-08T05:08:29.785935Z] INFO - Task instance in success state
[2026-09-08T05:08:29.786457Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T05:08:29.786730Z] INFO - Task operator:<Task(_PythonDecoratedOperator): aggregate_sales_data>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::aggregate_sales_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=weekend_catchup_pipeline/run_id=scheduled__2026-09-05T00:00:00+00:00/task_id=aggregate_sales_data/attempt=1.log
::endgroup::
[2026-09-08T05:08:19.414271Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07f6a-c315-7d1e-81b6-e1529598a6a0 dag_id=weekend_catchup_pipeline task_id=aggregate_sales_data run_id=scheduled__2026-09-05T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-08T05:08:28.714148Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T05:08:28.715498Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/46. The Missing Weekend.py
[2026-09-08T05:08:28.895603Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=46. The Missing Weekend.py  bundle_prepare_ms=3  dag_file_parse_ms=180 
[2026-09-08T05:08:29.495849Z] INFO - Task instance is in running state
[2026-09-08T05:08:29.495356Z] INFO - ::endgroup::
[2026-09-08T05:08:29.499657Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T05:08:29.500187Z] INFO - Current task name:aggregate_sales_data
[2026-09-08T05:08:29.500640Z] INFO - Dag name:weekend_catchup_pipeline
[2026-09-08T05:08:29.504368Z] INFO - Running historical sales aggregation for: 2026-09-05 00:00:00+00:00
[2026-09-08T05:08:29.504248Z] INFO - Done. Returned value was: None
[2026-09-08T05:08:29.504509Z] INFO - ::group::Post Execute
[2026-09-08T05:08:29.955597Z] INFO - Task instance in success state
[2026-09-08T05:08:29.958252Z] INFO - ::endgroup::
[2026-09-08T05:08:29.959700Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T05:08:29.961154Z] INFO - Task operator:<Task(_PythonDecoratedOperator): aggregate_sales_data>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::aggregate_sales_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=weekend_catchup_pipeline/run_id=scheduled__2026-09-06T00:00:00+00:00/task_id=aggregate_sales_data/attempt=1.log
::endgroup::
[2026-09-08T05:08:20.336653Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07f6a-c41c-7021-a7cf-6c5099bdbaec dag_id=weekend_catchup_pipeline task_id=aggregate_sales_data run_id=scheduled__2026-09-06T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-08T05:08:28.928828Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T05:08:28.930548Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/46. The Missing Weekend.py
[2026-09-08T05:08:29.020841Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=46. The Missing Weekend.py  bundle_prepare_ms=2  dag_file_parse_ms=88 
[2026-09-08T05:08:29.542757Z] INFO - ::endgroup::
[2026-09-08T05:08:29.546286Z] INFO - Task instance is in running state
[2026-09-08T05:08:29.548108Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T05:08:29.550502Z] INFO - Current task name:aggregate_sales_data
[2026-09-08T05:08:29.553467Z] INFO - Done. Returned value was: None
[2026-09-08T05:08:29.554003Z] INFO - Dag name:weekend_catchup_pipeline
[2026-09-08T05:08:29.558844Z] INFO - Running historical sales aggregation for: 2026-09-06 00:00:00+00:00
[2026-09-08T05:08:29.560372Z] INFO - ::group::Post Execute
[2026-09-08T05:08:29.937808Z] INFO - ::endgroup::
[2026-09-08T05:08:29.940811Z] INFO - Task instance in success state
[2026-09-08T05:08:29.941109Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T05:08:29.941340Z] INFO - Task operator:<Task(_PythonDecoratedOperator): aggregate_sales_data>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::aggregate_sales_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=weekend_catchup_pipeline/run_id=scheduled__2026-09-07T00:00:00+00:00/task_id=aggregate_sales_data/attempt=1.log
::endgroup::
[2026-09-08T05:08:20.903342Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07f6a-c5b4-721e-8580-57af5945e03c dag_id=weekend_catchup_pipeline task_id=aggregate_sales_data run_id=scheduled__2026-09-07T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-08T05:08:28.651993Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T05:08:28.661194Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/46. The Missing Weekend.py
[2026-09-08T05:08:28.811056Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=46. The Missing Weekend.py  bundle_prepare_ms=13  dag_file_parse_ms=150 
[2026-09-08T05:08:29.709164Z] INFO - Task instance is in running state
[2026-09-08T05:08:29.715970Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T05:08:29.722396Z] INFO - ::endgroup::
[2026-09-08T05:08:29.727504Z] INFO - Current task name:aggregate_sales_data
[2026-09-08T05:08:29.730857Z] INFO - Dag name:weekend_catchup_pipeline
[2026-09-08T05:08:29.740158Z] INFO - Done. Returned value was: None
[2026-09-08T05:08:29.740458Z] INFO - ::group::Post Execute
[2026-09-08T05:08:29.743198Z] INFO - Running historical sales aggregation for: 2026-09-07 00:00:00+00:00
[2026-09-08T05:08:30.056479Z] INFO - Task instance in success state
[2026-09-08T05:08:30.057003Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T05:08:30.057529Z] INFO - Task operator:<Task(_PythonDecoratedOperator): aggregate_sales_data>
[2026-09-08T05:08:30.057021Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::aggregate_sales_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=weekend_catchup_pipeline/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=aggregate_sales_data/attempt=1.log
::endgroup::
[2026-09-08T05:08:20.849665Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07f6a-c7ed-7cdf-822c-8aa02d6b8f34 dag_id=weekend_catchup_pipeline task_id=aggregate_sales_data run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-08T05:08:28.654181Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T05:08:28.659569Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/46. The Missing Weekend.py
[2026-09-08T05:08:28.810395Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=46. The Missing Weekend.py  bundle_prepare_ms=9  dag_file_parse_ms=150 
[2026-09-08T05:08:29.359685Z] INFO - ::endgroup::
[2026-09-08T05:08:29.361037Z] INFO - Task instance is in running state
[2026-09-08T05:08:29.361477Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T05:08:29.361843Z] INFO - Current task name:aggregate_sales_data
[2026-09-08T05:08:29.362671Z] INFO - Dag name:weekend_catchup_pipeline
[2026-09-08T05:08:29.365784Z] INFO - Done. Returned value was: None
[2026-09-08T05:08:29.366427Z] INFO - Running historical sales aggregation for: 2026-09-08 00:00:00+00:00
[2026-09-08T05:08:29.366050Z] INFO - ::group::Post Execute
[2026-09-08T05:08:29.753519Z] INFO - ::endgroup::
[2026-09-08T05:08:29.754014Z] INFO - Task instance in success state
[2026-09-08T05:08:29.755496Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T05:08:29.756820Z] INFO - Task operator:<Task(_PythonDecoratedOperator): aggregate_sales_data>

```
