# Airflow Challenge 13: The Gentle Extraction

**The Scenario:**

You need to extract data from 5 different tables in a critical production database. The database administrators have mandated that your Airflow instance can never hold more than 2 active connections at the same time to avoid degrading performance for business users. 

*Note: Since you are running this locally without setting up the UI first, Airflow will technically throw a warning that the pool doesn't exist and fallback to the `default_pool`. For this challenge, simply write the code as if the pool exists!*

**The Task:**

Write an Airflow DAG named `throttled_extraction_pipeline`.

**Requirements:**

1. Define a `@task` called `get_table_names()` that returns a list of 5 tables: `["sales", "customers", "inventory", "employees", "stores"]`.
2. Define a `@task` called `extract_table(table_name: str)`. 
   * **Crucial:** Assign this task to a pool named `"restricted_db_pool"`.
   * Inside, print `"Extracting {table_name} using 1 connection slot..."`
3. In the `@dag` function:
   * Retrieve the table list.
   * Dynamically map `extract_table` across the list of tables.
4. Execute the DAG. Check your logs to ensure the parameter passed correctly to the decorator!

### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@task
def get_table_names():
    return ["sales", "customers", "inventory", "employees", "stores"]


@task(pool="restricted_db_pool")
def extract_table(table_name: str):
    print(f"Extracting {table_name} using 1 connection slot...")


@dag(
    dag_id="throttled_extraction_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["practice"],
)
def throttled_extraction_pipeline_fun():
    tab_name = get_table_names()
    extract_table.expand(table_name=tab_name)


throttled_extraction_pipeline_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::get_table_names::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=throttled_extraction_pipeline/run_id=manual__2026-09-07T04:31:53.098084+00:00/task_id=get_table_names/attempt=1.log
::endgroup::
[2026-09-07T04:31:54.726592Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07a23-0f65-7d94-8739-d3511e1137cb dag_id=throttled_extraction_pipeline task_id=get_table_names run_id=manual__2026-09-07T04:31:53.098084+00:00 try_number=1 map_index=-1
[2026-09-07T04:31:55.213911Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-07T04:31:55.216361Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/45. The Gentle Extraction.py
[2026-09-07T04:31:55.303844Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=45. The Gentle Extraction.py  bundle_prepare_ms=4  dag_file_parse_ms=87 
[2026-09-07T04:31:55.378830Z] INFO - Task instance is in running state
[2026-09-07T04:31:55.379207Z] INFO - ::endgroup::
[2026-09-07T04:31:55.380543Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-07T04:31:55.380838Z] INFO - Current task name:get_table_names
[2026-09-07T04:31:55.381034Z] INFO - Dag name:throttled_extraction_pipeline
[2026-09-07T04:31:55.381851Z] INFO - Done. Returned value was: ['sales', 'customers', 'inventory', 'employees', 'stores']
[2026-09-07T04:31:55.382094Z] INFO - ::group::Post Execute
[2026-09-07T04:31:55.382645Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a07a23-0f65-7d94-8739-d3511e1137cb'), task_id='get_table_names', dag_id='throttled_extraction_pipeline', run_id='manual__2026-09-07T04:31:53.098084+00:00', try_number=1, dag_version_id=UUID('01a07a19-d9ae-7dd2-8ccb-6b1cddc1a7b5'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-9db451e085d0d0aff5ed5aba1de84d19-963602397b363c30-00'}, queue='default', task=<Task(_PythonDecoratedOperator): get_table_names>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 9, 7, 4, 31, 54, 730656, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-09-07T04:31:55.443325Z] INFO - Task instance in success state
[2026-09-07T04:31:55.443702Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-07T04:31:55.443956Z] INFO - Task operator:<Task(_PythonDecoratedOperator): get_table_names>
[2026-09-07T04:31:55.443195Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_table [0]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=throttled_extraction_pipeline/run_id=manual__2026-09-07T04:31:53.098084+00:00/task_id=extract_table/map_index=0/attempt=1.log
::endgroup::
[2026-09-07T04:31:56.643522Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07a23-0f66-7262-877e-9459fa2c5371 dag_id=throttled_extraction_pipeline task_id=extract_table run_id=manual__2026-09-07T04:31:53.098084+00:00 try_number=1 map_index=0
[2026-09-07T04:31:56.949572Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-07T04:31:56.951098Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/45. The Gentle Extraction.py
[2026-09-07T04:31:57.017362Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=45. The Gentle Extraction.py  bundle_prepare_ms=2  dag_file_parse_ms=66 
[2026-09-07T04:31:57.082988Z] INFO - Task instance is in running state
[2026-09-07T04:31:57.083586Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-07T04:31:57.083933Z] INFO - ::endgroup::
[2026-09-07T04:31:57.084037Z] INFO - Current task name:extract_table
[2026-09-07T04:31:57.084672Z] INFO - Dag name:throttled_extraction_pipeline
[2026-09-07T04:31:57.086551Z] INFO - Done. Returned value was: None
[2026-09-07T04:31:57.086784Z] INFO - ::group::Post Execute
[2026-09-07T04:31:57.089426Z] INFO - Extracting sales using 1 connection slot...
[2026-09-07T04:31:57.113949Z] INFO - ::endgroup::
[2026-09-07T04:31:57.116152Z] INFO - Task instance in success state
[2026-09-07T04:31:57.116726Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-07T04:31:57.117165Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_table>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_table [1]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=throttled_extraction_pipeline/run_id=manual__2026-09-07T04:31:53.098084+00:00/task_id=extract_table/map_index=1/attempt=1.log
::endgroup::
[2026-09-07T04:31:56.454646Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07a23-1bad-773e-99c3-21716149f20c dag_id=throttled_extraction_pipeline task_id=extract_table run_id=manual__2026-09-07T04:31:53.098084+00:00 try_number=1 map_index=1
[2026-09-07T04:31:56.815286Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-07T04:31:56.816241Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/45. The Gentle Extraction.py
[2026-09-07T04:31:56.868459Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=45. The Gentle Extraction.py  bundle_prepare_ms=1  dag_file_parse_ms=52 
[2026-09-07T04:31:56.932582Z] INFO - ::endgroup::
[2026-09-07T04:31:56.934190Z] INFO - Task instance is in running state
[2026-09-07T04:31:56.934835Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-07T04:31:56.935211Z] INFO - Current task name:extract_table
[2026-09-07T04:31:56.937823Z] INFO - Dag name:throttled_extraction_pipeline
[2026-09-07T04:31:56.940164Z] INFO - Done. Returned value was: None
[2026-09-07T04:31:56.940464Z] INFO - ::group::Post Execute
[2026-09-07T04:31:56.941266Z] INFO - Extracting customers using 1 connection slot...
[2026-09-07T04:31:56.967968Z] INFO - ::endgroup::
[2026-09-07T04:31:56.970211Z] INFO - Task instance in success state
[2026-09-07T04:31:56.970548Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-07T04:31:56.970753Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_table>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_table [2]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=throttled_extraction_pipeline/run_id=manual__2026-09-07T04:31:53.098084+00:00/task_id=extract_table/map_index=2/attempt=1.log
::endgroup::
[2026-09-07T04:31:57.670542Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07a23-1bae-738a-ac97-a72ca1c0cc21 dag_id=throttled_extraction_pipeline task_id=extract_table run_id=manual__2026-09-07T04:31:53.098084+00:00 try_number=1 map_index=2
[2026-09-07T04:31:58.214550Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-07T04:31:58.216985Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/45. The Gentle Extraction.py
[2026-09-07T04:31:58.362607Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=45. The Gentle Extraction.py  bundle_prepare_ms=4  dag_file_parse_ms=145 
[2026-09-07T04:31:58.440141Z] INFO - ::endgroup::
[2026-09-07T04:31:58.441245Z] INFO - Task instance is in running state
[2026-09-07T04:31:58.442009Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-07T04:31:58.442369Z] INFO - Current task name:extract_table
[2026-09-07T04:31:58.442652Z] INFO - Dag name:throttled_extraction_pipeline
[2026-09-07T04:31:58.448774Z] INFO - Done. Returned value was: None
[2026-09-07T04:31:58.449226Z] INFO - Extracting inventory using 1 connection slot...
[2026-09-07T04:31:58.450178Z] INFO - ::group::Post Execute
[2026-09-07T04:31:58.491787Z] INFO - ::endgroup::
[2026-09-07T04:31:58.492543Z] INFO - Task instance in success state
[2026-09-07T04:31:58.492899Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-07T04:31:58.493139Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_table>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_table [3]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=throttled_extraction_pipeline/run_id=manual__2026-09-07T04:31:53.098084+00:00/task_id=extract_table/map_index=3/attempt=1.log
::endgroup::
[2026-09-07T04:31:57.670906Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07a23-1bb0-7852-8747-40d5a0eac860 dag_id=throttled_extraction_pipeline task_id=extract_table run_id=manual__2026-09-07T04:31:53.098084+00:00 try_number=1 map_index=3
[2026-09-07T04:31:58.057515Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-07T04:31:58.059525Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/45. The Gentle Extraction.py
[2026-09-07T04:31:58.135167Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=45. The Gentle Extraction.py  bundle_prepare_ms=3  dag_file_parse_ms=75 
[2026-09-07T04:31:58.218769Z] INFO - ::endgroup::
[2026-09-07T04:31:58.220040Z] INFO - Task instance is in running state
[2026-09-07T04:31:58.220535Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-07T04:31:58.221692Z] INFO - Current task name:extract_table
[2026-09-07T04:31:58.222017Z] INFO - Dag name:throttled_extraction_pipeline
[2026-09-07T04:31:58.225843Z] INFO - Done. Returned value was: None
[2026-09-07T04:31:58.226006Z] INFO - Extracting employees using 1 connection slot...
[2026-09-07T04:31:58.226888Z] INFO - ::group::Post Execute
[2026-09-07T04:31:58.274190Z] INFO - ::endgroup::
[2026-09-07T04:31:58.298562Z] INFO - Task instance in success state
[2026-09-07T04:31:58.298983Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-07T04:31:58.299282Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_table>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_table [4]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=throttled_extraction_pipeline/run_id=manual__2026-09-07T04:31:53.098084+00:00/task_id=extract_table/map_index=4/attempt=1.log
::endgroup::
[2026-09-07T04:31:58.989676Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a07a23-1bb2-7406-a193-28e2723e5427 dag_id=throttled_extraction_pipeline task_id=extract_table run_id=manual__2026-09-07T04:31:53.098084+00:00 try_number=1 map_index=4
[2026-09-07T04:31:59.213481Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-07T04:31:59.214243Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/45. The Gentle Extraction.py
[2026-09-07T04:31:59.263308Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=45. The Gentle Extraction.py  bundle_prepare_ms=1  dag_file_parse_ms=49 
[2026-09-07T04:31:59.293938Z] INFO - Task instance is in running state
[2026-09-07T04:31:59.294318Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-07T04:31:59.294631Z] INFO - Current task name:extract_table
[2026-09-07T04:31:59.294833Z] INFO - Dag name:throttled_extraction_pipeline
[2026-09-07T04:31:59.294388Z] INFO - ::endgroup::
[2026-09-07T04:31:59.296593Z] INFO - Extracting stores using 1 connection slot...
[2026-09-07T04:31:59.296565Z] INFO - Done. Returned value was: None
[2026-09-07T04:31:59.296741Z] INFO - ::group::Post Execute
[2026-09-07T04:31:59.312970Z] INFO - Task instance in success state
[2026-09-07T04:31:59.313345Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-07T04:31:59.313688Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_table>
[2026-09-07T04:31:59.313708Z] INFO - ::endgroup::
```
