# Airflow Challenge 5: The Unskippable Task

**The Scenario:**

Building directly on your `conditional_file_pipeline`, your manager wants a final notification sent when the DAG finishes, no matter if data was processed or if the file was empty. 

If you just append a final task like this: `[process_data(), log_empty_file()] >> send_final_report()`, the report will be skipped. Why? Because one of its parents will *always* be skipped by the branch decision! You must use a Trigger Rule to force it to run.

**The Task:**

Update your DAG from Challenge 4 to include a final task that runs regardless of the branch taken.

**Requirements:**

1. Copy your working code from Challenge 4.
2. Add a new `@task` called `send_final_report()`.
3. Pass the argument `trigger_rule="none_failed"` into its decorator.
4. Inside the task, print `"Pipeline finished. Sending final report."`
5. In the `@dag` function, set `send_final_report()` to run *after* both `process_data()` and `log_empty_file()`. 
   *(Hint: `[process_data(), log_empty_file()] >> send_final_report()`)*
6. Run the DAG.

**Expected Output:**

In the logs, you should see `extract_file` run, `route_pipeline` choose the empty file route, `process_data` skipped, `log_empty_file` run, and finally `send_final_report` RUN (instead of being skipped)!


### My Solution:

```python
from airflow.sdk import dag, task
from datetime import datetime


@task
def extract_file():
    return 0


@task
def process_data():
    print("Processing data...")


@task
def log_empty_file():
    print("Alert: No data received today.")


@task.branch
def route_pipeline(record_count):
    if record_count > 0:
        return "process_data"
    elif record_count == 0:
        return "log_empty_file"


@task(trigger_rule="none_failed")
def send_final_report():
    print("Pipeline finished. Sending final report.")


@dag(
    dag_id="conditional_file_pipeline_triggers",
    schedule="@daily",
    start_date=datetime(2026, 8, 21),
    catchup=False,
    tags=["practice"],
)
def conditional_file_pipeline_triggers_fun():
    cnt = extract_file()
    route = route_pipeline(cnt)
    route >> [process_data(), log_empty_file()] >> send_final_report()


conditional_file_pipeline_triggers_fun()
```

### My Output Verification:

```
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::extract_file::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline_triggers/run_id=manual__2026-08-22T03:57:21.630064+00:00/task_id=extract_file/attempt=1.log
::endgroup::
[2026-08-22T03:57:22.618771Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0279d-b385-7b8b-ab28-1d75d382a732 dag_id=conditional_file_pipeline_triggers task_id=extract_file run_id=manual__2026-08-22T03:57:21.630064+00:00 try_number=1 map_index=-1
[2026-08-22T03:57:23.157256Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-22T03:57:23.158592Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/37. The Unskippable Task.py
[2026-08-22T03:57:23.261491Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=37. The Unskippable Task.py  bundle_prepare_ms=4  dag_file_parse_ms=99 
[2026-08-22T03:57:23.314968Z] INFO - Task instance is in running state
[2026-08-22T03:57:23.315564Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-22T03:57:23.315925Z] INFO - Current task name:extract_file
[2026-08-22T03:57:23.315108Z] INFO - ::endgroup::
[2026-08-22T03:57:23.316397Z] INFO - Dag name:conditional_file_pipeline_triggers
[2026-08-22T03:57:23.317436Z] INFO - Done. Returned value was: 0
[2026-08-22T03:57:23.317663Z] INFO - ::group::Post Execute
[2026-08-22T03:57:23.317914Z] INFO - Pushing xcom ti=RuntimeTaskInstance(id=UUID('01a0279d-b385-7b8b-ab28-1d75d382a732'), task_id='extract_file', dag_id='conditional_file_pipeline_triggers', run_id='manual__2026-08-22T03:57:21.630064+00:00', try_number=1, dag_version_id=UUID('01a0279d-3e77-7109-979a-29616eefec18'), map_index=-1, hostname='archlinux', context_carrier={'traceparent': '00-5eaa83e4639f1a27dacc295f25d4ede4-0cee64286d9a8a85-00'}, queue='default', task=<Task(_PythonDecoratedOperator): extract_file>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=0, start_date=datetime.datetime(2026, 8, 22, 3, 57, 22, 667302, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='') 
[2026-08-22T03:57:23.383363Z] INFO - Task instance in success state
[2026-08-22T03:57:23.383750Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-22T03:57:23.383980Z] INFO - Task operator:<Task(_PythonDecoratedOperator): extract_file>
[2026-08-22T03:57:23.383239Z] INFO - ::endgroup::


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::route_pipeline::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline_triggers/run_id=manual__2026-08-22T03:58:50.827199+00:00/task_id=route_pipeline/attempt=1.log
::endgroup::
[2026-08-22T03:58:53.296655Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0279f-0ff4-7164-80fb-cee7a540f480 dag_id=conditional_file_pipeline_triggers task_id=route_pipeline run_id=manual__2026-08-22T03:58:50.827199+00:00 try_number=1 map_index=-1
[2026-08-22T03:58:53.767949Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-22T03:58:53.769004Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/37. The Unskippable Task.py
[2026-08-22T03:58:53.818149Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=37. The Unskippable Task.py  bundle_prepare_ms=1  dag_file_parse_ms=49 
[2026-08-22T03:58:53.851524Z] INFO - Task instance is in running state
[2026-08-22T03:58:53.851987Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-22T03:58:53.852409Z] INFO - Current task name:route_pipeline
[2026-08-22T03:58:53.852733Z] INFO - Dag name:conditional_file_pipeline_triggers
[2026-08-22T03:58:53.852127Z] INFO - ::endgroup::
[2026-08-22T03:58:53.854593Z] INFO - Done. Returned value was: log_empty_file
[2026-08-22T03:58:53.854738Z] INFO - Branch into log_empty_file
[2026-08-22T03:58:53.854947Z] INFO - Following branch {'log_empty_file'}
[2026-08-22T03:58:53.855124Z] INFO - Skipping tasks [('process_data', -1)]
[2026-08-22T03:58:53.870658Z] INFO - ::group::Post Execute
[2026-08-22T03:58:53.870781Z] INFO - Skipping downstream tasks.
[2026-08-22T03:58:53.986071Z] INFO - ::endgroup::
[2026-08-22T03:58:53.987322Z] INFO - Task instance in success state
[2026-08-22T03:58:53.987823Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-22T03:58:53.988456Z] INFO - Task operator:<Task(_BranchPythonDecoratedOperator): route_pipeline>


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::log_empty_file::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline_triggers/run_id=manual__2026-08-22T03:58:50.827199+00:00/task_id=log_empty_file/attempt=1.log
::endgroup::
[2026-08-22T03:58:54.506723Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0279f-0ff6-71eb-93af-4ad4e160a97d dag_id=conditional_file_pipeline_triggers task_id=log_empty_file run_id=manual__2026-08-22T03:58:50.827199+00:00 try_number=1 map_index=-1
[2026-08-22T03:58:54.720929Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-22T03:58:54.721741Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/37. The Unskippable Task.py
[2026-08-22T03:58:54.770375Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=37. The Unskippable Task.py  bundle_prepare_ms=1  dag_file_parse_ms=48 
[2026-08-22T03:58:54.790540Z] INFO - Task instance is in running state
[2026-08-22T03:58:54.790852Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-22T03:58:54.790899Z] INFO - ::endgroup::
[2026-08-22T03:58:54.791123Z] INFO - Current task name:log_empty_file
[2026-08-22T03:58:54.791385Z] INFO - Dag name:conditional_file_pipeline_triggers
[2026-08-22T03:58:54.792454Z] INFO - Alert: No data received today.
[2026-08-22T03:58:54.792483Z] INFO - Done. Returned value was: None
[2026-08-22T03:58:54.792653Z] INFO - ::group::Post Execute
[2026-08-22T03:58:54.806890Z] INFO - Task instance in success state
[2026-08-22T03:58:54.807136Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-22T03:58:54.807456Z] INFO - Task operator:<Task(_PythonDecoratedOperator): log_empty_file>
[2026-08-22T03:58:54.807189Z] INFO - ::endgroup::



:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::process_data:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Skipped


:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::send_final_report:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

::group::Log message source details
/home/laptop_user/Code/AWS_Learning/airflow_home/logs/dag_id=conditional_file_pipeline_triggers/run_id=manual__2026-08-22T03:58:50.827199+00:00/task_id=send_final_report/attempt=1.log
::endgroup::
[2026-08-22T03:58:55.807637Z] INFO - ::group::Pre Execute
Task Identity ti_id=01a0279f-0ff7-7673-94e2-af0e47cb109a dag_id=conditional_file_pipeline_triggers task_id=send_final_report run_id=manual__2026-08-22T03:58:50.827199+00:00 try_number=1 map_index=-1
[2026-08-22T03:58:56.382584Z] INFO - DAG bundles loaded: dags-folder, example_dags, apache-airflow-providers-common-sql-example-dags, apache-airflow-providers-standard-example-dags
[2026-08-22T03:58:56.384261Z] INFO - Filling up the DagBag from /home/laptop_user/Code/AWS_Learning/airflow_home/dags/37. The Unskippable Task.py
[2026-08-22T03:58:56.520466Z] INFO - Worker startup parse complete bundle_name=dags-folder  bundle_version=null  dag_file=37. The Unskippable Task.py  bundle_prepare_ms=2  dag_file_parse_ms=136 
[2026-08-22T03:58:56.543611Z] INFO - Task instance is in running state
[2026-08-22T03:58:56.543783Z] INFO - ::endgroup::
[2026-08-22T03:58:56.544035Z] INFO -  Previous state of the Task instance: TaskInstanceState.QUEUED
[2026-08-22T03:58:56.546783Z] INFO - Current task name:send_final_report
[2026-08-22T03:58:56.546117Z] INFO - Done. Returned value was: None
[2026-08-22T03:58:56.546389Z] INFO - ::group::Post Execute
[2026-08-22T03:58:56.547668Z] INFO - Dag name:conditional_file_pipeline_triggers
[2026-08-22T03:58:56.548155Z] INFO - Pipeline finished. Sending final report.
[2026-08-22T03:58:56.570685Z] INFO - Task instance in success state
[2026-08-22T03:58:56.570189Z] INFO - ::endgroup::
[2026-08-22T03:58:56.571036Z] INFO -  Previous state of the Task instance: TaskInstanceState.RUNNING
[2026-08-22T03:58:56.571337Z] INFO - Task operator:<Task(_PythonDecoratedOperator): send_final_report>

```
