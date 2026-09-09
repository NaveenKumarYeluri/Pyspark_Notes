# Data Engineering Learning Log: Part 43 - Variables and Macros (Jinja)

If your DAG runs every day to process "yesterday's" data, you cannot hardcode the date `"2026-08-24"` into your Python script. You need the date to inject itself dynamically at runtime. 

Airflow solves this using **Jinja Templating** (Macros) and **Variables**.

## 1. Built-in Macros (The Execution Date)

Airflow provides built-in variables that you can inject into your tasks using Jinja syntax (double curly braces `{{ }}`). The most common is `{{ ds }}`, which stands for "Data Stamp" (the logical execution date of the DAG run, formatted as `YYYY-MM-DD`).

*Note: In the modern TaskFlow API, you can access these context variables directly by passing them as arguments to your function!*

## 2. Airflow Variables

Sometimes you need a global configuration parameter (like an API endpoint or a configuration threshold) that multiple DAGs can share. Instead of hardcoding it, you save it in the Airflow UI (Admin -> Variables) and pull it into your code dynamically using `Variable.get()`.

```python
from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime

@task
def fetch_daily_data(logical_date: str = None):
    # 'logical_date' is a built-in Airflow context variable representing the run date
    
    # We fetch a global config variable (defaulting to 'json' if it doesn't exist)
    file_format = Variable.get("preferred_file_format", default_var="json")
    
    print(f"Fetching {file_format} data for the date: {logical_date}")

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def dynamic_templating_pipeline():
    fetch_daily_data()

dynamic_templating_pipeline()
```
