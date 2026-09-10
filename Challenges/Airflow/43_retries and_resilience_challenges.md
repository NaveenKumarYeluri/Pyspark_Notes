# Airflow Challenge 11: The Stubborn Task

**The Scenario:**

You are hitting a legacy database that crashes frequently. You need to write a task that attempts to query the database, but you know it's going to fail. You need to configure the task to retry a few times to give the database a chance to recover. If it still fails, you want to trigger a custom alert function.

**The Task:**

Write an Airflow DAG named `retry_and_alert_pipeline`.

**Requirements:**

1. Import `timedelta` from the standard Python `datetime` library.
2. Define a standard python function called `send_pagerduty_alert(context)`. It should simply print: `"CRITICAL ALERT: Database is completely down. Paging on-call engineer."`
3. Define a `@task` called `query_fragile_database()`.
    * Configure it to retry **2 times**.
    * Set the delay between retries to **5 seconds** using `timedelta(seconds=5)`.
    * Attach your alert function using `on_failure_callback`.
4. Inside the task, write the following logic:
    * Print `"Attempting to connect to database..."`
    * Force the task to crash by deliberately raising an exception: `raise ValueError("Database timeout exception!")`
5. Create the `@dag` function, set it to run `@daily`, and execute the pipeline. 
6. Check your logs! You should see the initial failure, two retries happening 5 seconds apart, and finally your `CRITICAL ALERT` print statement executing from the callback.

### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime, timedelta


def send_pagerduty_alert(context):
    print("CRITICAL ALERT: Database is completely down. Paging on-call engineer.")


@task(retries=2, retry_delay=timedelta(seconds=5), on_failure_callback=send_pagerduty_alert)
def query_fragile_database():
    print("Attempting to connect to database...")

    raise ValueError("Database timeout exception!")


@dag(
    dag_id="retry_and_alert_pipeline",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    schedule="@daily",
    tags=["practice"],
)
def retry_and_alert_pipeline_fun():
    query_fragile_database()


retry_and_alert_pipeline_fun()
```

### My Output Verification:

```
::::::::::::::::::::::::::::::::::::::::::::::::::::::query_fragile_database(Run 1):::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=retry_and_alert_pipeline/run_id=manual__2026-09-05T04:07:23.051932+00:00/task_id=query_fragile_database/attempt=1.log
::endgroup::
[2026-09-05T04:07:24.271265Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fbf-e8d5-7c8e-9e98-dab93ffe7aff dag_id=retry_and_alert_pipeline task_id=query_fragile_database run_id=manual__2026-09-05T04:07:23.051932+00:00 try_number=1 map_index=-1
[2026-09-05T04:07:24.602671Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:07:24.603677Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/43. The Stubborn Task.py
[2026-09-05T04:07:24.655718Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=43. The Stubborn Task.py  bundle_prepare_ms=1  dag_file_parse_ms=52 
[2026-09-05T04:07:24.676205Z] INFO - Task instance is in running state
[2026-09-05T04:07:24.676534Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:07:24.676874Z] INFO - Current task name:query_fragile_database
[2026-09-05T04:07:24.676747Z] INFO - ::endgroup::
[2026-09-05T04:07:24.677074Z] INFO - Dag name:retry_and_alert_pipeline
[2026-09-05T04:07:24.678744Z] INFO - Attempting to connect to database...
[2026-09-05T04:07:24.678775Z] ERROR - Task failed with exceptionValueError: Database timeout exception!
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 1579 in _run_task_and_map_outcome
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2234 in _execute_task
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2192 in _run_execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/decorator.py, line 398 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/standard/operators/python.py, line 231 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/standard/operators/python.py, line 254 in execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/callback_runner.py, line 97 in run
    File /home/laptop_user/Code/AWS_Learning/airflow_home/dags/43. The Stubborn Task.py, line 13 in query_fragile_database

[2026-09-05T04:07:24.679261Z] INFO - ::group::Post Execute
[2026-09-05T04:07:24.702283Z] INFO - Task instance in failure state
[2026-09-05T04:07:24.702593Z] INFO - Task start
[2026-09-05T04:07:24.702827Z] INFO - Task:<Task(_PythonDecoratedOperator): query_fragile_database>
[2026-09-05T04:07:24.702602Z] INFO - ::endgroup::
[2026-09-05T04:07:24.703010Z] INFO - Failure caused by Database timeout exception!


::::::::::::::::::::::::::::::::::::::::::::::::::::::query_fragile_database(Retry 1):::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=retry_and_alert_pipeline/run_id=manual__2026-09-05T04:07:23.051932+00:00/task_id=query_fragile_database/attempt=2.log
::endgroup::
[2026-09-05T04:07:30.032614Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fbf-ef15-7787-a279-b46ba0bccddb dag_id=retry_and_alert_pipeline task_id=query_fragile_database run_id=manual__2026-09-05T04:07:23.051932+00:00 try_number=2 map_index=-1
[2026-09-05T04:07:30.235985Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:07:30.237218Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/43. The Stubborn Task.py
[2026-09-05T04:07:30.274670Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=43. The Stubborn Task.py  bundle_prepare_ms=2  dag_file_parse_ms=37 
[2026-09-05T04:07:30.294773Z] INFO - Task instance is in running state
[2026-09-05T04:07:30.294447Z] INFO - ::endgroup::
[2026-09-05T04:07:30.295379Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:07:30.296074Z] INFO - Current task name:query_fragile_database
[2026-09-05T04:07:30.296407Z] INFO - Dag name:retry_and_alert_pipeline
[2026-09-05T04:07:30.296825Z] INFO - Attempting to connect to database...
[2026-09-05T04:07:30.296826Z] ERROR - Task failed with exceptionValueError: Database timeout exception!
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 1579 in _run_task_and_map_outcome
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2234 in _execute_task
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2192 in _run_execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/decorator.py, line 398 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/standard/operators/python.py, line 231 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/standard/operators/python.py, line 254 in execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/callback_runner.py, line 97 in run
    File /home/laptop_user/Code/AWS_Learning/airflow_home/dags/43. The Stubborn Task.py, line 13 in query_fragile_database

[2026-09-05T04:07:30.297364Z] INFO - ::group::Post Execute
[2026-09-05T04:07:30.345730Z] INFO - Task instance in failure state
[2026-09-05T04:07:30.346485Z] INFO - Task start
[2026-09-05T04:07:30.347436Z] INFO - Task:<Task(_PythonDecoratedOperator): query_fragile_database>
[2026-09-05T04:07:30.347805Z] INFO - Failure caused by Database timeout exception!
[2026-09-05T04:07:30.347066Z] INFO - ::endgroup::


::::::::::::::::::::::::::::::::::::::::::::::::::::::query_fragile_database(Retry 2):::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=retry_and_alert_pipeline/run_id=manual__2026-09-05T04:07:23.051932+00:00/task_id=query_fragile_database/attempt=3.log
::endgroup::
[2026-09-05T04:07:35.397573Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a06fc0-0512-7fd2-80f1-cab630260a02 dag_id=retry_and_alert_pipeline task_id=query_fragile_database run_id=manual__2026-09-05T04:07:23.051932+00:00 try_number=3 map_index=-1
[2026-09-05T04:07:35.618722Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-09-05T04:07:35.619814Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/43. The Stubborn Task.py
[2026-09-05T04:07:35.665684Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=43. The Stubborn Task.py  bundle_prepare_ms=1  dag_file_parse_ms=45 
[2026-09-05T04:07:35.686967Z] INFO - Task instance is in running state
[2026-09-05T04:07:35.687351Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-09-05T04:07:35.687739Z] INFO - Current task name:query_fragile_database
[2026-09-05T04:07:35.687997Z] INFO - Dag name:retry_and_alert_pipeline
[2026-09-05T04:07:35.687547Z] INFO - ::endgroup::
[2026-09-05T04:07:35.689677Z] INFO - Attempting to connect to database...
[2026-09-05T04:07:35.689753Z] ERROR - Task failed with exceptionValueError: Database timeout exception!
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 1579 in _run_task_and_map_outcome
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2234 in _execute_task
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 197 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py, line 2192 in _run_execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/decorator.py, line 398 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/bases/operator.py, line 445 in wrapper
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/standard/operators/python.py, line 231 in execute
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/providers/standard/operators/python.py, line 254 in execute_callable
    File /home/laptop_user/Code/AWS_Learning/.venv/lib/python3.14/site-packages/airflow/sdk/execution_time/callback_runner.py, line 97 in run
    File /home/laptop_user/Code/AWS_Learning/airflow_home/dags/43. The Stubborn Task.py, line 13 in query_fragile_database

[2026-09-05T04:07:35.690391Z] INFO - ::group::Post Execute
[2026-09-05T04:07:35.694521Z] INFO - CRITICAL ALERT: Database is completely down. Paging on-call engineer.
[2026-09-05T04:07:35.695026Z] INFO - Task instance in failure state
[2026-09-05T04:07:35.695324Z] INFO - Task start
[2026-09-05T04:07:35.695574Z] INFO - Task:<Task(_PythonDecoratedOperator): query_fragile_database>
[2026-09-05T04:07:35.695808Z] INFO - Failure caused by Database timeout exception!
[2026-09-05T04:07:35.695018Z] INFO - ::endgroup::

```
