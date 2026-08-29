# Data Engineering Learning Log: Part 36 - Airflow TaskFlow API

In the previous lesson, we passed data between tasks using `PythonOperator` and `ti.xcom_pull()`. While this works, it requires a lot of boilerplate code, and passing data feels "clunky" because you have to explicitly reference `task_ids`.

Starting in Airflow 2.0 (and continuing into Airflow 3.0), the community introduced the **TaskFlow API**. It allows you to write Airflow DAGs using standard Python decorators (`@dag` and `@task`).

## 1. The `@task` Decorator

Instead of importing `PythonOperator`, you simply add `@task` above your Python function. Airflow automatically converts it into a task behind the scenes!

Even better, **TaskFlow automatically handles XComs for you**. If Task A returns a value, you can pass it directly into Task B as a normal Python argument. Airflow secretly does the `xcom_push` and `xcom_pull` under the hood.

```python
from airflow.decorators import dag, task
from datetime import datetime

# 1. Define the DAG using a decorator
@dag(
    dag_id="modern_taskflow_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False
)
def my_data_pipeline():
    
    # 2. Define tasks using decorators
    @task
    def get_user_name():
        return "Alice" # Automatically pushed to XCom
        
    @task
    def greet_user(name):
        # The argument 'name' is automatically pulled from XCom!
        print(f"Hello, {name}!")
        
    # 3. Define dependencies by passing functions into each other
    # We do NOT use >> here!
    extracted_name = get_user_name()
    greet_user(extracted_name)

# 4. Instantiate the DAG by calling the main function
my_data_pipeline()
```

## 2. Why is TaskFlow Better?

1. **Less Code:** No need to type `PythonOperator(task_id="...", python_callable=...)` over and over.
2. **Implicit Dependencies:** Because `greet_user()` requires the output of `get_user_name()`, Airflow automatically knows that `get_user_name` must run first. You don't even need to use the bitshift `>>` operator!
3. **Type Hinting:** It feels exactly like writing native, modern Python.

*Note: You can mix and match! If you need to run a `BashOperator`, you can easily chain it to a `@task` using the traditional `>>` operator.*
