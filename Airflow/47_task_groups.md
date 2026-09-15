# Data Engineering Learning Log: Part 45 - Task Groups

In older versions of Airflow, engineers used "SubDAGs" to group tasks together. SubDAGs caused massive performance issues and deadlocks. They have been entirely replaced by **Task Groups**, which are purely UI-level logical groupings that do not impact Airflow's scheduling engine.

Using the modern TaskFlow API, you can bundle multiple tasks together effortlessly using the `@task_group` decorator.

## The `@task_group` Decorator

When you wrap tasks in a Task Group, you can treat the entire group as a single node when setting dependencies. 

```python
from airflow.sdk import dag, task, task_group
from datetime import datetime

@task
def extract_sql():
    print("Extracting SQL database...")

@task
def extract_api():
    print("Extracting from REST API...")

# 1. Define the Task Group
@task_group(group_id="extraction_layer")
def run_all_extractions():
    # Calling the tasks inside this function assigns them to the group
    extract_sql()
    extract_api()

@task
def transform_data():
    print("Cleaning and joining the extracted data...")

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def task_group_pipeline():
    
    # 2. Set dependencies using the group!
    # Transform will only run after BOTH extractions in the group finish.
    run_all_extractions() >> transform_data()

task_group_pipeline()
```
