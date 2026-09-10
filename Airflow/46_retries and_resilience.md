# Data Engineering Learning Log: Part 44 - Retries and Resilience

In production, data pipelines fail constantly for reasons completely outside of your control:

* An API rate-limits your request.
* A database gets locked by another transaction.
* A network switch temporarily drops packets.

If an Airflow task fails immediately on the first attempt, it breaks the entire pipeline. To fix this, we build resilience directly into our tasks using **Retries**, **Timeouts**, and **Callbacks**.

## Task-Level Resilience Arguments

You can pass specific resilience parameters directly into the `@task` decorator:

* **`retries`**: The number of times to retry a failed task before marking it as `Failed`.
* **`retry_delay`**: A `timedelta` object defining how long to wait between retries. (Waiting 3-5 minutes is standard to let transient network issues resolve).
* **`execution_timeout`**: The maximum amount of time a task is allowed to run before Airflow forcefully kills it. (Crucial for preventing tasks from hanging infinitely).
* **`on_failure_callback`**: A Python function to execute if the task ultimately fails after all retries are exhausted (often used to send a Slack or Email alert).

```python
from airflow.sdk import dag, task
from datetime import datetime, timedelta

def alert_team(context):
    print(f"ALERT: Task {context.get('task_instance').task_id} failed permanently!")

@task(
    retries=2, 
    retry_delay=timedelta(seconds=10),
    execution_timeout=timedelta(minutes=5),
    on_failure_callback=alert_team
)
def flaky_api_call():
    print("Attempting to hit API...")
    # Simulating a random API failure
    raise ConnectionError("API connection reset by peer!")

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def resilient_pipeline():
    flaky_api_call()

resilient_pipeline()
```
