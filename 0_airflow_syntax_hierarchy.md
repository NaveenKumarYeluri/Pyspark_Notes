# 0_airflow_syntax_hierarchy.md
# Apache Airflow Syntax Hierarchy: The 3 Levels of Orchestration

Airflow's TaskFlow API separates workflow architecture from runtime execution. Understanding what happens during DAG parsing vs. what happens during task execution prevents unexpected crashes.

---

## Level 1: Global Architecture & Events (`@dag`, `Asset`, `Variable`)
These commands define the physical shape and trigger conditions of your pipelines. They are evaluated by the Airflow Scheduler every few seconds to determine if a pipeline should run.

* **The `@dag` Decorator:** `@dag(dag_id="daily_etl", schedule="@daily", start_date=datetime(2026,1,1), catchup=False)`
  * *Why:* Instantiates a workflow pipeline where `catchup=False` prevents historical backfilling when deployed.
* **Assets (Event-Driven Scheduling):** `my_asset = Asset("s3://bucket/trusted_data")`
  * *Why:* Replaces cron schedules so downstream DAGs trigger automatically the millisecond an upstream task refreshes an Asset.
* **Variables (Global Configuration):** `config = Variable.get("api_endpoint")`
  * *Why:* Retrieves global configuration parameters stored securely in the Airflow metadata database.
* **Dynamic DAG Generation:** `globals()["dag_name"] = factory_function()`
  * *Why:* Injects dynamically generated DAG objects into the global Python namespace so the parser registers them as independent workflows.

---

## Level 2: Topological Graph Routing (`>>`, `@task.branch`, `.expand()`)
These commands establish the Directed Acyclic Graph (DAG). They define exactly how tasks relate to each other, how they execute in parallel, and how Airflow handles branching logic.

* **Bitshift Dependencies:** `extract_task >> [transform_1, transform_2] >> load_task`
  * *Why:* Sets the execution order, with lists indicating parallel execution.
* **Branching (Conditional Logic):** `@task.branch`
  * *Why:* Evaluates logic and returns the string `task_id` of the path Airflow should follow, automatically skipping unselected paths.
* **Dynamic Task Mapping:** `process_file.expand(file_name=get_files_task())`
  * *Why:* Takes an array generated at runtime and spawns $N$ independent, parallel task instances dynamically.
* **Task Groups:** `@task_group(group_id="extraction_phase")`
  * *Why:* Visually and logically bundles multiple tasks together in the UI without altering execution logic.

---

## Level 3: Runtime Task Execution (`@task`, Context, Hooks)
These commands dictate what actually happens on the Worker Node when a task executes.

* **The `@task` Decorator:** `@task(retries=2, retry_delay=timedelta(minutes=5))`
  * *Why:* Wraps a standard Python function, isolating it into an executable Airflow task with built-in resilience.
* **Trigger Rules:** `@task(trigger_rule="none_failed")`
  * *Why:* Overrides the default `all_success` behavior, which is critical for forcing final alert tasks to run even if upstream branches were skipped.
* **Cross-Communication (XComs):** Handled automatically via `return` statements and function arguments.
  * *Why:* Allows isolated tasks to pass small metadata (like record counts or file paths) to downstream tasks.
* **Jinja Context Variables:** `def my_task(logical_date: str):`
  * *Why:* Injects built-in Airflow metadata (like the execution date) directly into your task arguments at runtime.
* **Sensors:** `@task.sensor(poke_interval=10, mode="poke")`
  * *Why:* Pokes an external system (like an API or S3 bucket) until a condition is met before unblocking the DAG.
* **Container Isolation:** `@task.docker(image="python:3.10-slim")`
  * *Why:* Detaches execution into an isolated, ephemeral Docker container to prevent dependency conflicts.
