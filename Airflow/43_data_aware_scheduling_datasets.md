# Data Engineering Learning Log: Part 41 - Data-Aware Scheduling (Datasets)

Historically, Airflow DAGs were purely time-based (e.g., "Run every day at midnight"). But modern data engineering is event-driven. 

With **Data-Aware Scheduling**, a downstream DAG (the Consumer) is scheduled to run *only* when a specific data asset is updated by an upstream DAG (the Producer).

## The `Dataset` Object

You define a logical `Dataset` using a Uniform Resource Identifier (URI), like an S3 path or a database table name. 
* The **Producer** task declares that it updates the dataset using the `outlets` parameter.
* The **Consumer** DAG schedules itself based on that dataset instead of a cron string.

```python
from airflow.sdk import dag, task
from airflow.datasets import Dataset
from datetime import datetime

# 1. Define the logical dataset
flight_data = Dataset("s3://flight-analytics-bucket/cleaned_flights.parquet")

# 2. PRODUCER DAG (Time-based schedule)
@dag(dag_id="flight_data_producer", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def producer_pipeline():
    
    # This task registers an UPDATE to the dataset when it finishes successfully
    @task(outlets=[flight_data])
    def process_and_upload_flights():
        print("Uploading cleaned flight data to S3...")
        
    process_and_upload_flights()

producer_pipeline()

# 3. CONSUMER DAG (Data-driven schedule)
# Notice the schedule is a list containing the Dataset, NOT a time string!
@dag(dag_id="flight_data_consumer", schedule=[flight_data], start_date=datetime(2026, 1, 1), catchup=False)
def consumer_pipeline():
    
    @task
    def run_analytical_queries():
        print("New flight data detected in S3! Running queries...")
        
    run_analytical_queries()

consumer_pipeline()
```
