# Data Engineering Learning Log: Part 51 - Containerized Execution

As your Airflow cluster scales, you will inevitably hit the "Dependency Hell" problem. 
* Team A writes a task that requires `pandas==1.0`.
* Team B writes a task that requires `pandas==2.0`.
If both tasks run on the same Airflow worker node, they will conflict and crash.

## The Solution: Docker and Kubernetes

Instead of running tasks natively in the worker's Python environment, modern Airflow uses the **DockerOperator** or **KubernetesPodOperator**. 

With the modern TaskFlow API, you can use the `@task.docker` or `@task.kubernetes` decorators. Airflow will spin up a completely isolated, ephemeral container to run just that specific task. Once the task finishes, the container is destroyed. No dependency conflicts, ever!

```python
from airflow.sdk import dag
from airflow.decorators import task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 9, 1), catchup=False)
def container_example_pipeline():
    
    # This task will pull the Python 3.9 image and run entirely inside Docker!
    @task.docker(image="python:3.9-slim", multiple_outputs=False)
    def isolated_processing():
        import sys
        print(f"Running inside an isolated container! Python version: {sys.version}")
        return "Container executed successfully."

    isolated_processing()

container_example_pipeline()
```
