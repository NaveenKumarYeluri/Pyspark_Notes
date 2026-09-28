# Data Engineering Learning Log: Part 50 - Deferrable Operators

In a standard Airflow cluster, you have a limited number of "Worker Slots" (e.g., 16 slots). 
When a standard `@task.sensor` goes to sleep for 5 minutes, **it still occupies that worker slot.** If 16 sensors are waiting for files at the same time, your cluster is effectively paralyzed. No other tasks can run because all 16 workers are busy "sleeping." This is called **Worker Starvation**.

## The Solution: Async Airflow and the Triggerer

Airflow introduced **Deferrable Operators** (Async Operators) to solve this. 
When a deferrable operator needs to wait, it fully suspends itself and **yields its worker slot back to the cluster**. 

The task is handed off to a highly efficient background service called the **Triggerer**. The Triggerer uses Python's `asyncio` to monitor thousands of events simultaneously on a single thread. When the event finally happens, the Triggerer wakes the task back up and puts it back into a worker slot to finish.

## Using Deferrable Operators

Instead of writing complex custom asynchronous classes from scratch, the easiest way to use this feature is to leverage Airflow's pre-built Async operators, or pass `deferrable=True` to supported operators.

```python
from airflow.sdk import dag, task
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from datetime import datetime, timedelta

@dag(schedule="@daily", start_date=datetime(2026, 9, 1), catchup=False)
def async_wait_pipeline():
    
    # This task waits for 5 minutes, but frees up the worker slot entirely!
    wait_for_five = TimeDeltaSensorAsync(
        task_id="wait_async",
        delta=timedelta(minutes=5)
    )

    @task
    def process_data():
        print("Wait is over! Processing data...")

    wait_for_five >> process_data()

async_wait_pipeline()
```
