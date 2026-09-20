# Data Engineering Learning Log: Part 47 - Time Travel (Catchup and Backfilling)

When migrating from legacy SSRS architectures to modern orchestration, dealing with historical data is a massive challenge. If your daily ETL job fails on Friday, and you don't fix it until Monday, what happens to Saturday and Sunday's data? 

Airflow handles this automatically using **Catchup** and **logical_date**.

## Catchup = True

When you create a DAG, you give it a `start_date` and a `schedule`. 
If you set `start_date=datetime(2026, 9, 4)` and `schedule="@daily"`, and you deploy that DAG on **September 7, 2026**, Airflow notices a gap. 

If `catchup=True` (the default behavior), Airflow will immediately spawn three independent, historical DAG runs:
1. One for Sept 4
2. One for Sept 5
3. One for Sept 6

For each of those runs, Airflow injects the specific historical date into the `logical_date` context variable. Your code executes *as if* it were running on that exact day in the past. 

```python
from airflow.sdk import dag, task
from datetime import datetime

# Assuming today's date is September 7, 2026
@dag(
    schedule="@daily", 
    start_date=datetime(2026, 9, 4), 
    catchup=True # Turning the time machine ON
)
def historical_catchup_pipeline():
    
    @task
    def process_daily_batch(logical_date: str):
        # This function will automatically run 3 times in rapid succession,
        # printing Sept 4, Sept 5, and Sept 6 dynamically!
        print(f"Processing historical batch for date: {logical_date}")

    process_daily_batch()

historical_catchup_pipeline()
```
