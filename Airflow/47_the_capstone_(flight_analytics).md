# Data Engineering Learning Log: Part 48 - The Capstone (Flight Analytics)

It is time to put everything together. You are the sole Data Engineer responsible for migrating the legacy flight analytics reporting system.

You need to build a single Airflow DAG that orchestrates a complete ELT (Extract, Load, Transform) pipeline. It must wait for the data to arrive, extract it safely, load it into multiple zones, branch based on quality, and alert the team when it finishes.

---

# Airflow Challenge 15: The Final Exam

**The Scenario:**

Every night, an external vendor drops a batch of flight logs into a directory. You need to wait for the directory to be populated, process the logs into two separate regional zones in parallel, check the quality of the merged data, and then build a final executive report.

**The Task:**

Write an Airflow DAG named `flight_analytics_capstone`.

**Requirements:**

1. **The Sensor:**
   * Create a `@task.sensor` named `wait_for_files()`.
   * Have it simulate checking a directory by randomly returning `PokeReturnValue(is_done=True)` or `PokeReturnValue(is_done=False)`.

2. **The Extraction Group:**
   * Create a `@task_group` named `extract_zones()`.
   * Inside, create two parallel `@task` functions: `extract_domestic()` and `extract_international()`. Have each print a message.

3. **Dynamic Quality Check:**
   * Create a `@task` called `get_quality_rules()` that returns `["check_nulls", "check_duplicates"]`.
   * Create a `@task` called `run_rule(rule: str)` that prints `"Running rule: {rule}"`.
   * Dynamically map `run_rule` across the rules returned by `get_quality_rules()`.

4. **The Branch (Conditional Routing):**
   * Create a `@task.branch` called `evaluate_quality()`. 
   * Simulate a random success/failure. If successful, return the string `"build_final_report"`. If it fails, return `"quarantine_data"`.

5. **The Destinations:**
   * Create a `@task` called `build_final_report()`.
   * Create a `@task` called `quarantine_data()`.

6. **The Unskippable Alert:**
   * Create a `@task` called `send_status_email()`.
   * Configure it so that it *always* runs at the very end of the pipeline, regardless of which branch was taken (Hint: Trigger Rules).

7. **The Orchestration:**
   * Link all these tasks together in your `@dag` function in this exact order:
     `Sensor -> Extraction Group -> Get Rules -> Mapped Rules -> Branch Decision -> [Report OR Quarantine] -> Status Email`
   * Set the DAG to `schedule="@daily"` and `catchup=False`.

Execute your capstone pipeline and share the logs showing the full workflow!


### My Solution:

```python
from airflow.sdk import task, dag, task_group
from airflow.sdk.bases.sensor import PokeReturnValue
from datetime import datetime
from pathlib import Path
import random


@task.sensor(poke_interval=3, timeout=15, mode="poke")
def wait_for_files():
    pwd = Path.cwd()
    print(f"Checking Directory: {pwd}")
    for item in pwd.iterdir():
        if item.is_dir():
            return PokeReturnValue(is_done=True)
        else:
            return PokeReturnValue(is_done=False)


@task
def extract_international():
    print("Extracting International Passeneges list")


@task
def extract_domestic():
    print("Extracting Domestic Passeneges list")


@task_group(group_id="Extraction_Task")
def extract_zones():
    extract_domestic()
    extract_international()


@task
def get_quality_rules():
    return ["check_nulls", "check_duplicates"]


@task
def run_rule(rule: str):
    print(f"Running rule: {rule}")


@task.branch
def evaluate_quality():
    result = random.choice([True, False])
    if result is True:
        return "build_final_report"
    else:
        return "quarantine_data"


@task
def build_final_report():
    print("Final Report...")


@task
def quarantine_data():
    print("Quarantine Data...")


@task(trigger_rule="none_failed")
def send_status_email():
    print("Status mail....")


@dag(
    dag_id="flight_analytics_capstone",
    schedule="@daily",
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["practice"],
)
def flight_analytics_capstone_fun():
    q_rules = get_quality_rules()
    route = run_rule.expand(rule=q_rules)

    (
        wait_for_files()
        >> extract_zones()
        >> get_quality_rules()
        >> route
        >> evaluate_quality()
        >> [build_final_report(), quarantine_data()]
        >> send_status_email()
    )


flight_analytics_capstone_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::get_quality_rules::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=get_quality_rules/attempt=2.log
::endgroup::
[2026-09-08T08:47:57.448976Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d69b-7983-8252-b7d7fc91ac20 dag_id=flight_analytics_capstone task_id=get_quality_rules run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=-1
[2026-09-08T08:47:57.799750Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:47:57.801679Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:47:57.874443Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:47:57.875715Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=2  dag_file_parse_ms=75 
[2026-09-08T08:47:57.929785Z] INFO - Task instance is in running state
[2026-09-08T08:47:57.930201Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:47:57.930668Z] INFO - Current task name:get_quality_rules
[2026-09-08T08:47:57.930372Z] INFO - ::endgroup::
[2026-09-08T08:47:57.931061Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:47:57.932458Z] INFO - Done. Returned value was: ['check_nulls', 'check_duplicates']
[2026-09-08T08:47:57.932692Z] INFO - ::group::Post Execute
[2026-09-08T08:47:57.933054Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a08033-d69b-7983-8252-b7d7fc91ac20'), task_id='get_quality_rules', dag_id='flight_analytics_capstone', run_id='scheduled__2026-09-08T00:00:00+00:00', try_number=2, dag_version_id=UUID('01a0802c-7079-7463-9442-ca07d7cd6731'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-41f7868f096deb25f49b77f018629731-6a0e45bc6d8fb0e7-00'}, queue='default', task=<Task(_PythonDecoratedOperator): get_quality_rules>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=1, start_date=datetime.datetime(2026, 9, 8, 8, 47, 57, 524017, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-09-08T08:47:57.981385Z] INFO - Task instance in success state
[2026-09-08T08:47:57.981671Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:47:57.981966Z] INFO - Task operator:<Task(_PythonDecoratedOperator): get_quality_rules>
[2026-09-08T08:47:57.981761Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::wait_for_files::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=wait_for_files/attempt=2.log
::endgroup::
[2026-09-08T08:47:57.488270Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6ac-7968-8a89-0a7c8872dea2 dag_id=flight_analytics_capstone task_id=wait_for_files run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=-1
[2026-09-08T08:47:57.839874Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:47:57.840996Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:47:57.898189Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:47:57.899678Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=2  dag_file_parse_ms=58 
[2026-09-08T08:47:57.935461Z] INFO - Task instance is in running state
[2026-09-08T08:47:57.935794Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:47:57.936394Z] INFO - Current task name:wait_for_files
[2026-09-08T08:47:57.936755Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:47:57.936114Z] INFO - ::endgroup::
[2026-09-08T08:47:57.936936Z] INFO - Poking callable: <function wait_for_files at 0x7fe67f1556f0>
[2026-09-08T08:47:57.937809Z] INFO - Checking Directory: /home/laptop_user/Code/AWS_Learning
[2026-09-08T08:47:57.937315Z] INFO - Success criteria met. Exiting.
[2026-09-08T08:47:57.937505Z] INFO - ::group::Post Execute
[2026-09-08T08:47:57.971190Z] INFO - Task instance in success state
[2026-09-08T08:47:57.971437Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:47:57.971699Z] INFO - Task operator:<Task(DecoratedSensorOperator): wait_for_files>
[2026-09-08T08:47:57.971577Z] INFO - ::endgroup::

::::::::::::::::::::::::::::::::::::::::::::::::::::::Extraction_Task.extract_domestic:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=Extraction_Task.extract_domestic/attempt=2.log
::endgroup::
[2026-09-08T08:47:58.087862Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6b0-7d57-83f1-fa03b16cfd45 dag_id=flight_analytics_capstone task_id=Extraction_Task.extract_domestic run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=-1
[2026-09-08T08:47:58.356271Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:47:58.357100Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:47:58.440281Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:47:58.441828Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=1  dag_file_parse_ms=84 
[2026-09-08T08:47:58.469852Z] INFO - Task instance is in running state
[2026-09-08T08:47:58.470280Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:47:58.470534Z] INFO - Current task name:Extraction_Task.extract_domestic
[2026-09-08T08:47:58.470825Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:47:58.470400Z] INFO - ::endgroup::
[2026-09-08T08:47:58.472196Z] INFO - Extracting Domestic Passeneges list
[2026-09-08T08:47:58.472175Z] INFO - Done. Returned value was: None
[2026-09-08T08:47:58.472344Z] INFO - ::group::Post Execute
[2026-09-08T08:47:58.488296Z] INFO - Task instance in success state
[2026-09-08T08:47:58.488580Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:47:58.488886Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Extraction_Task.extract_domestic>
[2026-09-08T08:47:58.488710Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::Extraction_Task.extract_international:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=Extraction_Task.extract_international/attempt=2.log
::endgroup::
[2026-09-08T08:47:58.080801Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6b6-7e8a-afff-1a379007295b dag_id=flight_analytics_capstone task_id=Extraction_Task.extract_international run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=-1
[2026-09-08T08:47:58.342463Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:47:58.343247Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:47:58.400123Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:47:58.403805Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=1  dag_file_parse_ms=60 
[2026-09-08T08:47:58.435788Z] INFO - Task instance is in running state
[2026-09-08T08:47:58.436210Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:47:58.436832Z] INFO - Current task name:Extraction_Task.extract_international
[2026-09-08T08:47:58.436152Z] INFO - ::endgroup::
[2026-09-08T08:47:58.437200Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:47:58.438404Z] INFO - Extracting International Passeneges list
[2026-09-08T08:47:58.438425Z] INFO - Done. Returned value was: None
[2026-09-08T08:47:58.438610Z] INFO - ::group::Post Execute
[2026-09-08T08:47:58.455530Z] INFO - Task instance in success state
[2026-09-08T08:47:58.455888Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:47:58.456276Z] INFO - Task operator:<Task(_PythonDecoratedOperator): Extraction_Task.extract_international>
[2026-09-08T08:47:58.456088Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::get_quality_rules__1::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=get_quality_rules__1/attempt=2.log
::endgroup::
[2026-09-08T08:47:58.575330Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6bb-7b6f-828d-c8c0a299a94a dag_id=flight_analytics_capstone task_id=get_quality_rules__1 run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=-1
[2026-09-08T08:47:58.815610Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:47:58.816878Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:47:58.868073Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:47:58.869430Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=1  dag_file_parse_ms=52 
[2026-09-08T08:47:58.906100Z] INFO - Task instance is in running state
[2026-09-08T08:47:58.906424Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:47:58.906702Z] INFO - Current task name:get_quality_rules__1
[2026-09-08T08:47:58.906886Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:47:58.906587Z] INFO - ::endgroup::
[2026-09-08T08:47:58.908494Z] INFO - Done. Returned value was: ['check_nulls', 'check_duplicates']
[2026-09-08T08:47:58.908671Z] INFO - ::group::Post Execute
[2026-09-08T08:47:58.908934Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a08033-d6bb-7b6f-828d-c8c0a299a94a'), task_id='get_quality_rules__1', dag_id='flight_analytics_capstone', run_id='scheduled__2026-09-08T00:00:00+00:00', try_number=2, dag_version_id=UUID('01a0802c-7079-7463-9442-ca07d7cd6731'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-41f7868f096deb25f49b77f018629731-6a0e45bc6d8fb0e7-00'}, queue='default', task=<Task(_PythonDecoratedOperator): get_quality_rules__1>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=1, start_date=datetime.datetime(2026, 9, 8, 8, 47, 58, 580746, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-09-08T08:47:58.932670Z] INFO - Task instance in success state
[2026-09-08T08:47:58.933031Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:47:58.933392Z] INFO - Task operator:<Task(_PythonDecoratedOperator): get_quality_rules__1>
[2026-09-08T08:47:58.933005Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::run_rule [0]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=run_rule/map_index=0/attempt=2.log
::endgroup::
[2026-09-08T08:47:59.513610Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6a7-7330-b0bf-ed06a4376d9f dag_id=flight_analytics_capstone task_id=run_rule run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=0
[2026-09-08T08:48:00.032767Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:48:00.034858Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:48:00.110575Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:48:00.112039Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=3  dag_file_parse_ms=77 
[2026-09-08T08:48:00.154806Z] INFO - Task instance is in running state
[2026-09-08T08:48:00.155148Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:48:00.155393Z] INFO - Current task name:run_rule
[2026-09-08T08:48:00.155533Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:48:00.155314Z] INFO - ::endgroup::
[2026-09-08T08:48:00.157378Z] INFO - Running rule: check_nulls
[2026-09-08T08:48:00.157366Z] INFO - Done. Returned value was: None
[2026-09-08T08:48:00.157567Z] INFO - ::group::Post Execute
[2026-09-08T08:48:00.181675Z] INFO - Task instance in success state
[2026-09-08T08:48:00.182991Z] INFO - ::endgroup::
[2026-09-08T08:48:00.184182Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:48:00.184771Z] INFO - Task operator:<Task(_PythonDecoratedOperator): run_rule>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::run_rule [1]::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=run_rule/map_index=1/attempt=2.log
::endgroup::
[2026-09-08T08:47:59.389113Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6d6-75be-9e64-2338f486a65d dag_id=flight_analytics_capstone task_id=run_rule run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=1
[2026-09-08T08:47:59.855998Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:47:59.857783Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:47:59.978498Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:47:59.982218Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=2  dag_file_parse_ms=124 
[2026-09-08T08:48:00.043830Z] INFO - ::endgroup::
[2026-09-08T08:48:00.044232Z] INFO - Task instance is in running state
[2026-09-08T08:48:00.044657Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:48:00.044955Z] INFO - Current task name:run_rule
[2026-09-08T08:48:00.045214Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:48:00.046221Z] INFO - Running rule: check_duplicates
[2026-09-08T08:48:00.046246Z] INFO - Done. Returned value was: None
[2026-09-08T08:48:00.046449Z] INFO - ::group::Post Execute
[2026-09-08T08:48:00.070361Z] INFO - Task instance in success state
[2026-09-08T08:48:00.070712Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:48:00.070819Z] INFO - ::endgroup::
[2026-09-08T08:48:00.071153Z] INFO - Task operator:<Task(_PythonDecoratedOperator): run_rule>


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::evaluate_quality::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=evaluate_quality/attempt=3.log
::endgroup::
[2026-09-08T08:48:00.603734Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6c0-7e4a-b8c7-119d351ee0f5 dag_id=flight_analytics_capstone task_id=evaluate_quality run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=3 map_index=-1
[2026-09-08T08:48:00.909820Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:48:00.910531Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:48:00.966702Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:48:00.968704Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=1  dag_file_parse_ms=58 
[2026-09-08T08:48:01.016853Z] INFO - Task instance is in running state
[2026-09-08T08:48:01.017362Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:48:01.017728Z] INFO - Current task name:evaluate_quality
[2026-09-08T08:48:01.017531Z] INFO - ::endgroup::
[2026-09-08T08:48:01.018033Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:48:01.020103Z] INFO - Done. Returned value was: quarantine_data
[2026-09-08T08:48:01.020241Z] INFO - Branch into quarantine_data
[2026-09-08T08:48:01.020367Z] INFO - Following branch {'quarantine_data'}
[2026-09-08T08:48:01.020478Z] INFO - Skipping tasks [('build_final_report', -1)]
[2026-09-08T08:48:01.036313Z] INFO - ::group::Post Execute
[2026-09-08T08:48:01.036444Z] INFO - Skipping downstream tasks.
[2026-09-08T08:48:01.068681Z] INFO - Task instance in success state
[2026-09-08T08:48:01.069224Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:48:01.069681Z] INFO - Task operator:<Task(_BranchPythonDecoratedOperator): evaluate_quality>
[2026-09-08T08:48:01.069594Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::quarantine_data::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=quarantine_data/attempt=2.log
::endgroup::
[2026-09-08T08:48:01.943726Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6cb-71d2-80cf-a0854c1d0e41 dag_id=flight_analytics_capstone task_id=quarantine_data run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=2 map_index=-1
[2026-09-08T08:48:02.262839Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:48:02.264061Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:48:02.364202Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:48:02.366374Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=1  dag_file_parse_ms=102 
[2026-09-08T08:48:02.404749Z] INFO - Task instance is in running state
[2026-09-08T08:48:02.404194Z] INFO - ::endgroup::
[2026-09-08T08:48:02.405411Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:48:02.405707Z] INFO - Current task name:quarantine_data
[2026-09-08T08:48:02.405968Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:48:02.406692Z] INFO - Quarantine Data...
[2026-09-08T08:48:02.406714Z] INFO - Done. Returned value was: None
[2026-09-08T08:48:02.406966Z] INFO - ::group::Post Execute
[2026-09-08T08:48:02.424202Z] INFO - Task instance in success state
[2026-09-08T08:48:02.424425Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:48:02.424656Z] INFO - Task operator:<Task(_PythonDecoratedOperator): quarantine_data>
[2026-09-08T08:48:02.424453Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::build_final_report::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Skipped

::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::send_status_email::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=flight_analytics_capstone/run_id=scheduled__2026-09-08T00:00:00+00:00/task_id=send_status_email/attempt=1.log
::endgroup::
[2026-09-08T08:48:03.177581Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a08033-d6d0-7438-93e0-be03a61ebe19 dag_id=flight_analytics_capstone task_id=send_status_email run_id=scheduled__2026-09-08T00:00:00+00:00 try_number=1 map_index=-1
[2026-09-08T08:48:03.553581Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-08T08:48:03.555489Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/47. The Capstone.py
[2026-09-08T08:48:03.623547Z] WARNING - The `airflow.utils.operator_helpers.determine_kwargs` attribute is deprecated. Please use `'airflow.sdk.bases.decorator.determine_kwargs'`. category=UserWarning 
[2026-09-08T08:48:03.624763Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=47. The Capstone.py  bundle_prepare_ms=2  dag_file_parse_ms=69 
[2026-09-08T08:48:03.644429Z] INFO - Task instance is in running state
[2026-09-08T08:48:03.644741Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-08T08:48:03.644956Z] INFO - ::endgroup::
[2026-09-08T08:48:03.645059Z] INFO - Current task name:send_status_email
[2026-09-08T08:48:03.645399Z] INFO - Dag name:flight_analytics_capstone
[2026-09-08T08:48:03.646919Z] INFO - Status mail....
[2026-09-08T08:48:03.646947Z] INFO - Done. Returned value was: None
[2026-09-08T08:48:03.647121Z] INFO - ::group::Post Execute
[2026-09-08T08:48:03.661986Z] INFO - Task instance in success state
[2026-09-08T08:48:03.662268Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-09-08T08:48:03.662559Z] INFO - Task operator:<Task(_PythonDecoratedOperator): send_status_email>
[2026-09-08T08:48:03.662393Z] INFO - ::endgroup::

```
