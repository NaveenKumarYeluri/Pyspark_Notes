# Data Engineering Learning Log: Part 37 - Branching (Conditional Logic)

In real-world data pipelines, you rarely execute every task every single time.
* If a file arrives empty, you skip the processing task.
* If a data quality check fails, you trigger a quarantine task instead of the load task.

In Airflow, we handle this using **Branching**. 

## The `@task.branch` Decorator

Using the TaskFlow API, branching is incredibly intuitive. You apply the `@task.branch` decorator to a function. The only rule is that **a branch function must return the `task_id` (as a string) of the downstream task that Airflow should execute next.**

Airflow will automatically skip any tasks that were not returned by the branch function.

```python
from airflow.decorators import dag, task
from datetime import datetime

@task
def check_data_quality():
    # Simulate a quality score
    quality_score = 85
    return quality_score

# 1. Use the branch decorator
@task.branch
def decide_next_step(score: int):
    if score >= 90:
        return "load_to_production" # Returns the exact name of the next function/task
    else:
        return "send_warning_email"

@task
def load_to_production():
    print("Data is good! Loading...")

@task
def send_warning_email():
    print("Quality too low. Alerting team.")

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def branching_example_pipeline():
    score = check_data_quality()
    
    # 2. Execute the branch decision
    decision = decide_next_step(score)
    
    # 3. Set the dependencies for the branched tasks
    # The decision task points to BOTH possible outcomes. Airflow chooses the winner.
    decision >> [load_to_production(), send_warning_email()]

branching_example_pipeline()
```
