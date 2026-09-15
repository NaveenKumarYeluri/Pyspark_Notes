# Airflow Challenge 12: Organizing the Monolith

**The Scenario:**

You are building an ETL pipeline that extracts data from three different regional APIs (North, South, and East), merges the data into a single staging table, and then performs two parallel quality checks. Your manager wants the Airflow UI to remain clean and readable.

**The Task:**

Write an Airflow DAG named `organized_etl_pipeline` utilizing Task Groups.

**Requirements:**

1. Import `task_group` alongside `dag` and `task` from `airflow.sdk`.
2. Define three individual `@task` functions: `extract_north()`, `extract_south()`, and `extract_east()`. Have each print a simple message.
3. Define a `@task_group` named `regional_extractions()`. Inside it, call your three extraction tasks.
4. Define a `@task` called `merge_data()`.
5. Define two individual `@task` functions: `check_nulls()` and `check_schema()`.
6. Define a second `@task_group` named `quality_checks()`. Inside it, call your two check tasks.
7. In the `@dag` function, set the following workflow dependency using the groups:
   * `regional_extractions()` runs first.
   * `merge_data()` runs after the extractions.
   * `quality_checks()` runs after the merge.
8. Execute the DAG and review your Airflow logs to verify the execution order!

### My Solution:

```python
from airflow.sdk import dag, task, task_group
from datetime import datetime


@task
def extract_north():
    print("North Task...")


@task
def extract_south():
    print("South Task...")


@task
def extract_east():
    print("East Task...")


@task_group(group_id="Extraction")
def regional_extractions():
    extract_north()
    extract_south()
    extract_east()


@task
def merge_data():
    print("Mergeing Data....")


@task
def check_nulls():
    print("Checking NULLs...")


@task
def check_schema():
    print("Checking Schema...")


@task_group(group_id="Checking_tasks")
def quality_checks():
    check_nulls()
    check_schema()


@dag(
    dag_id="organized_etl_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    tags=["practice"],
    catchup=False,
)
def organized_etl_pipeline_fun():
    regional_extractions() >> merge_data() >> quality_checks()


organized_etl_pipeline_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::Extraction.extract_north:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=organized_etl_pipeline/run_id=manual__2026-09-05T04:43:13.924321+00:00/task_id=Extraction.extract_north/attempt=1.log
::endgroup::
[2026-09-05T04:43:14.233368Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fe0-bab7-7521-8b54-983115b30bd8 dag_id=organized_etl_pipeline task_id=Extraction.extract_north run_id=manual__2026-09-05T04:43:13.924321+00:00 try_number=1 map_index=-1
[2026-09-05T04:43:15.141768Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:43:15.143611Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/44. Organizing the Monolith.py
[2026-09-05T04:43:15.359990Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=44. Organizing the Monolith.py  bundle_prepare_ms=3  dag_file_parse_ms=216 
[2026-09-05T04:43:15.579260Z] INFO - ::endgroup::
[2026-09-05T04:43:15.580585Z] INFO - Task instance is in running state
[2026-09-05T04:43:15.582365Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:43:15.582879Z] INFO - Current task name:Extraction.extract_north
[2026-09-05T04:43:15.585497Z] INFO - Dag name:organized_etl_pipeline
[2026-09-05T04:43:15.588382Z] INFO - Done. Returned value was: None
[2026-09-05T04:43:15.588692Z] INFO - ::group::Post Execute
[2026-09-05T04:43:15.589135Z] INFO - North Task...
[2026-09-05T04:43:15.716030Z] INFO - Task instance in success state
[2026-09-05T04:43:15.716962Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T04:43:15.717650Z] INFO - ::endgroup::
[2026-09-05T04:43:15.718514Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Extraction.extract_north>


::::::::::::::::::::::::::::::::::::::::::::::::::::::Extraction.extract_south:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=organized_etl_pipeline/run_id=manual__2026-09-05T04:43:13.924321+00:00/task_id=Extraction.extract_south/attempt=1.log
::endgroup::
[2026-09-05T04:43:14.329772Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fe0-bab8-77b1-b489-92efe49a528b dag_id=organized_etl_pipeline task_id=Extraction.extract_south run_id=manual__2026-09-05T04:43:13.924321+00:00 try_number=1 map_index=-1
[2026-09-05T04:43:15.607585Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:43:15.610598Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/44. Organizing the Monolith.py
[2026-09-05T04:43:15.757810Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=44. Organizing the Monolith.py  bundle_prepare_ms=4  dag_file_parse_ms=147 
[2026-09-05T04:43:15.804689Z] INFO - ::endgroup::
[2026-09-05T04:43:15.805923Z] INFO - Task instance is in running state
[2026-09-05T04:43:15.806404Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:43:15.806758Z] INFO - Current task name:Extraction.extract_south
[2026-09-05T04:43:15.807005Z] INFO - Dag name:organized_etl_pipeline
[2026-09-05T04:43:15.810515Z] INFO - South Task...
[2026-09-05T04:43:15.811223Z] INFO - Done. Returned value was: None
[2026-09-05T04:43:15.811496Z] INFO - ::group::Post Execute
[2026-09-05T04:43:15.878803Z] INFO - ::endgroup::
[2026-09-05T04:43:15.885963Z] INFO - Task instance in success state
[2026-09-05T04:43:15.886341Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T04:43:15.886611Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Extraction.extract_south>


::::::::::::::::::::::::::::::::::::::::::::::::::::::Extraction.extract_east:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=organized_etl_pipeline/run_id=manual__2026-09-05T04:43:13.924321+00:00/task_id=Extraction.extract_east/attempt=1.log
::endgroup::
[2026-09-05T04:43:14.311563Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fe0-bab9-7942-a66f-590381e9f842 dag_id=organized_etl_pipeline task_id=Extraction.extract_east run_id=manual__2026-09-05T04:43:13.924321+00:00 try_number=1 map_index=-1
[2026-09-05T04:43:15.132724Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:43:15.134991Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/44. Organizing the Monolith.py
[2026-09-05T04:43:15.222511Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=44. Organizing the Monolith.py  bundle_prepare_ms=3  dag_file_parse_ms=87 
[2026-09-05T04:43:15.380739Z] INFO - ::endgroup::
[2026-09-05T04:43:15.382899Z] INFO - Task instance is in running state
[2026-09-05T04:43:15.385009Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:43:15.387763Z] INFO - Current task name:Extraction.extract_east
[2026-09-05T04:43:15.390571Z] INFO - Dag name:organized_etl_pipeline
[2026-09-05T04:43:15.392925Z] INFO - Done. Returned value was: None
[2026-09-05T04:43:15.393194Z] INFO - ::group::Post Execute
[2026-09-05T04:43:15.404818Z] INFO - East Task...
[2026-09-05T04:43:15.673856Z] INFO - ::endgroup::
[2026-09-05T04:43:15.675854Z] INFO - Task instance in success state
[2026-09-05T04:43:15.678375Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T04:43:15.680617Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Extraction.extract_east>


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::merge_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=organized_etl_pipeline/run_id=manual__2026-09-05T04:43:13.924321+00:00/task_id=merge_data/attempt=1.log
::endgroup::
[2026-09-05T04:43:17.350559Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fe0-baba-7719-a230-72a1fe231233 dag_id=organized_etl_pipeline task_id=merge_data run_id=manual__2026-09-05T04:43:13.924321+00:00 try_number=1 map_index=-1
[2026-09-05T04:43:17.921777Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:43:17.922748Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/44. Organizing the Monolith.py
[2026-09-05T04:43:17.967689Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=44. Organizing the Monolith.py  bundle_prepare_ms=1  dag_file_parse_ms=45 
[2026-09-05T04:43:18.059980Z] INFO - ::endgroup::
[2026-09-05T04:43:18.064696Z] INFO - Task instance is in running state
[2026-09-05T04:43:18.065348Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:43:18.065830Z] INFO - Current task name:merge_data
[2026-09-05T04:43:18.066370Z] INFO - Dag name:organized_etl_pipeline
[2026-09-05T04:43:18.068999Z] INFO - Done. Returned value was: None
[2026-09-05T04:43:18.069710Z] INFO - Mergeing Data....
[2026-09-05T04:43:18.069266Z] INFO - ::group::Post Execute
[2026-09-05T04:43:18.115972Z] INFO - Task instance in success state
[2026-09-05T04:43:18.116464Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T04:43:18.116114Z] INFO - ::endgroup::
[2026-09-05T04:43:18.117110Z] INFO - Task operator:<Task(_PythonDecoratedOperator): merge_data>


::::::::::::::::::::::::::::::::::::::::::::::::::::::Checking_tasks.check_nulls:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=organized_etl_pipeline/run_id=manual__2026-09-05T04:43:13.924321+00:00/task_id=Checking_tasks.check_nulls/attempt=1.log
::endgroup::
[2026-09-05T04:43:18.611587Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fe0-babb-7e66-8438-2de0d4586858 dag_id=organized_etl_pipeline task_id=Checking_tasks.check_nulls run_id=manual__2026-09-05T04:43:13.924321+00:00 try_number=1 map_index=-1
[2026-09-05T04:43:19.470510Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:43:19.480755Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/44. Organizing the Monolith.py
[2026-09-05T04:43:19.640728Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=44. Organizing the Monolith.py  bundle_prepare_ms=5  dag_file_parse_ms=166 
[2026-09-05T04:43:19.690406Z] INFO - ::endgroup::
[2026-09-05T04:43:19.691621Z] INFO - Task instance is in running state
[2026-09-05T04:43:19.692306Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:43:19.693479Z] INFO - Current task name:Checking_tasks.check_nulls
[2026-09-05T04:43:19.694057Z] INFO - Dag name:organized_etl_pipeline
[2026-09-05T04:43:19.696154Z] INFO - Done. Returned value was: None
[2026-09-05T04:43:19.696471Z] INFO - ::group::Post Execute
[2026-09-05T04:43:19.697663Z] INFO - Checking NULLs...
[2026-09-05T04:43:19.721893Z] INFO - Task instance in success state
[2026-09-05T04:43:19.721046Z] INFO - ::endgroup::
[2026-09-05T04:43:19.722363Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T04:43:19.723943Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Checking_tasks.check_nulls>


::::::::::::::::::::::::::::::::::::::::::::::::::::::Checking_tasks.check_nulls:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=organized_etl_pipeline/run_id=manual__2026-09-05T04:43:13.924321+00:00/task_id=Checking_tasks.check_schema/attempt=1.log
::endgroup::
[2026-09-05T04:43:19.485574Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fe0-babc-72d6-8ac9-13053a6ad85b dag_id=organized_etl_pipeline task_id=Checking_tasks.check_schema run_id=manual__2026-09-05T04:43:13.924321+00:00 try_number=1 map_index=-1
[2026-09-05T04:43:20.016568Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:43:20.017750Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/44. Organizing the Monolith.py
[2026-09-05T04:43:20.096910Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=44. Organizing the Monolith.py  bundle_prepare_ms=2  dag_file_parse_ms=79 
[2026-09-05T04:43:20.165001Z] INFO - ::endgroup::
[2026-09-05T04:43:20.166442Z] INFO - Task instance is in running state
[2026-09-05T04:43:20.166846Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:43:20.167127Z] INFO - Current task name:Checking_tasks.check_schema
[2026-09-05T04:43:20.167365Z] INFO - Dag name:organized_etl_pipeline
[2026-09-05T04:43:20.169035Z] INFO - Checking Schema...
[2026-09-05T04:43:20.169541Z] INFO - Done. Returned value was: None
[2026-09-05T04:43:20.170429Z] INFO - ::group::Post Execute
[2026-09-05T04:43:20.213700Z] INFO - Task instance in success state
[2026-09-05T04:43:20.213341Z] INFO - ::endgroup::
[2026-09-05T04:43:20.214245Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-05T04:43:20.214713Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Checking_tasks.check_schema>

```
