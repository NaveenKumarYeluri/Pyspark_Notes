# Data Engineering Learning Log: Part 39 - Dynamic Task Mapping

In earlier versions of Airflow, if you had to process 10 files from an S3 bucket or call an API for 5 different customer IDs, you had to write a static Python loop or hardcode 5 separate tasks.

Modern Airflow introduces **Dynamic Task Mapping** (the `.expand()` method). This enables Airflow to take a list returned by an upstream task and dynamically generate $N$ parallel task instances at runtime based on the list length.

---

## The `.expand()` Method

Instead of calling the task like a normal function, you call `.expand()` on the task and pass in a list or an upstream task's return value.

```python
from airflow.sdk import dag, task
from datetime import datetime

@task
def get_file_list() -> list[str]:
    # Upstream task discovers files dynamically
    return ["file_a.csv", "file_b.csv", "file_c.csv"]

@task
def process_single_file(file_name: str):
    # This runs in parallel for EACH item in the list
    print(f"Processing {file_name}")

@dag(
    schedule="@daily",
    start_date=datetime(2026, 8, 22),
    catchup=False,
    tags=["practice"]
)
def dynamic_mapping_pipeline():
    files = get_file_list()
    
    # Airflow dynamically spawns 3 parallel tasks:
    process_single_file.expand(file_name=files)

dynamic_mapping_pipeline()
```
