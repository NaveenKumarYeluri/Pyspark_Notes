# Data Engineering Learning Log: Part 35 - PythonOperator & XComs

In the previous lesson, we used the `BashOperator` to trigger terminal commands. However, it is highly common to execute native Python functions directly inside your DAG using the `PythonOperator`.

## 1. The PythonOperator

To use the `PythonOperator`, you define a standard Python function and then pass it to the operator using the `python_callable` argument.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from the PythonOperator!")

with DAG(dag_id="simple_python_dag", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
    
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello
    )
```

## 2. Passing Arguments (`op_kwargs`)

If your Python function requires arguments, you cannot pass them directly in the `python_callable`. Instead, you use the `op_kwargs` (Operator Keyword Arguments) dictionary.

```python
def greet_user(name, age):
    print(f"Hello {name}, you are {age} years old.")

greet_task = PythonOperator(
    task_id="greet_user",
    python_callable=greet_user,
    op_kwargs={"name": "Alice", "age": 28}
)
```

## 3. Task Communication (XComs)

Airflow tasks run completely isolated from one another. Task A cannot simply pass a normal Python variable to Task B. 

To share small pieces of metadata (like a dynamic file path, a run ID, or a success count), Airflow uses **XComs** (Cross-Communications). 

**Pushing an XCom:**

The easiest way to "push" an XCom is simply to `return` a value from your Python function. Airflow automatically saves this return value into its internal database.

**Pulling an XCom:**

To retrieve that value in a downstream task, you must accept a special Airflow argument called `ti` (Task Instance) into your function. You then use `ti.xcom_pull()` and specify which `task_id` generated the data.

```python
# TASK 1: Generates the data and returns it (Auto-Pushes to XCom)
def generate_file_path():
    path = "/tmp/data_2026_08_19.csv"
    print(f"Generated path: {path}")
    return path 

# TASK 2: Pulls the data using the 'ti' context variable
def process_file(ti):
    # Pull the return value from Task 1
    pulled_path = ti.xcom_pull(task_ids="generate_path_task")
    print(f"Processing the file located at: {pulled_path}")

with DAG(dag_id="xcom_demo", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
    
    task_1 = PythonOperator(
        task_id="generate_path_task",
        python_callable=generate_file_path
    )
    
    task_2 = PythonOperator(
        task_id="process_file_task",
        python_callable=process_file
    )
    
    task_1 >> task_2
```
*Note: XComs are strictly for tiny metadata (like strings or numbers). Never use an XCom to pass a massive PySpark DataFrame between tasks!*
