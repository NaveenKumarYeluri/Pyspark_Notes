# Data Engineering Learning Log: Part 46 - Pools and Concurrency

By default, Airflow will run as many parallel tasks as its worker nodes can handle. While this is great for performance, it is dangerous for your downstream systems. If you hit a third-party API with 50 parallel tasks, you will be rate-limited or IP-banned. If you hit a SQL Server database with 50 parallel queries, you will cause a memory exhaustion outage.

To restrict concurrency for specific resources, you use **Pools**.

## 1. Creating a Pool

Pools are typically created in the Airflow UI (Admin -> Pools). You give the Pool a name (e.g., `sql_server_pool`) and assign it a number of "slots" (e.g., `3`). This means no matter how many tasks are triggered, only 3 tasks assigned to this pool can ever run at the exact same time.

## 2. Assigning Tasks to a Pool

Using the TaskFlow API, you assign a task to a pool by passing the `pool` argument directly into the `@task` decorator. 

If you assign 10 tasks to a 3-slot pool, Airflow will start 3 tasks. The other 7 will wait in the `Queued` state until slots become available.

```python
from airflow.sdk import dag, task
from datetime import datetime

# This task is assigned to a restricted pool
@task(pool="api_rate_limit_pool")
def extract_customer_data(customer_id: int):
    print(f"Extracting data for customer {customer_id}...")

@dag(schedule="@daily", start_date=datetime(2026, 9, 1), catchup=False)
def pool_example_pipeline():
    
    # We trigger 10 parallel tasks using dynamic mapping
    customer_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    extract_customer_data.expand(customer_id=customer_ids)

pool_example_pipeline()
```
