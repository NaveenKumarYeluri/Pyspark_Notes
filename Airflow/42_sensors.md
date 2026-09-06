# Data Engineering Learning Log: Part 40 - Sensors

Up to this point, our pipelines have executed immediately when triggered. But in the real world, you often have to wait for external events:

* Waiting for a vendor to drop a CSV into an S3 bucket.
* Waiting for an external database to finish its nightly backup.
* Waiting for an API to return a `200 OK` status.

In Airflow, we handle this using **Sensors**. 
A Sensor is a special type of task that evaluates a condition at a specific interval (e.g., every 60 seconds). 

* If the condition is `False`, the Sensor goes to sleep and tries again later (called "poking"). 
* If the condition is `True`, the Sensor succeeds, and the downstream tasks are allowed to run.
* If it waits too long, it times out and fails.

## The `@task.sensor` Decorator

In modern Airflow, you can turn any Python function into a Sensor using the `@task.sensor` decorator. The function must return a specific state: `PokeReturnValue`.

```python
from airflow.sdk import dag, task
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import random

@task.sensor(poke_interval=5, timeout=30, mode="poke")
def wait_for_data() -> PokeReturnValue:
    # Simulate checking an external system
    file_arrived = random.choice([True, False])
    
    if file_arrived:
        print("Data found! Proceeding...")
        return PokeReturnValue(is_done=True)
    else:
        print("No data yet. Waiting...")
        return PokeReturnValue(is_done=False)

@task
def process_data():
    print("Processing the arrived data!")

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def sensor_example_pipeline():
    wait_for_data() >> process_data()

sensor_example_pipeline()
```
