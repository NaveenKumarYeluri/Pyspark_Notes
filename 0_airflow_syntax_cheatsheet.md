# 0_airflow_syntax_cheatsheet.md
# Apache Airflow Orchestration Syntax Cheatsheet

This single-file document serves as a high-density reference sheet for modern Airflow data orchestration, TaskFlow API mechanics, and pipeline resilience.

---

## 1. Global Pipeline Architecture & Orchestration

### Workflow Instantiation (`@dag`)
* **Syntax:** `@dag(dag_id='name', schedule='string', start_date=datetime(), catchup=False)`
* **Explanation:** The decorator that transforms a Python function into a tracked workflow pipeline. `catchup=False` prevents historical backfilling.
* **Example:** `@dag(dag_id="nightly_elt", schedule="@daily", start_date=datetime(2026, 9, 1), catchup=False)`

### Data-Aware Scheduling (`Asset`)
* **Syntax:** `asset_obj = Asset('uri')` -> `@dag(schedule=[asset_obj])`
* **Explanation:** Replaces time-based cron scheduling. Consumer DAGs trigger instantly the moment an upstream Producer DAG updates the registered Asset.
* **Example:** `s3_asset = Asset("s3://bucket/ready.csv"); @dag(schedule=[s3_asset])`

### Dynamic DAG Generation (`globals()`)
* **Syntax:** `globals()['dag_name'] = factory_function()`
* **Explanation:** Injects dynamically generated DAG objects into the global Python namespace so Airflow's parser registers them as individual workflows.
* **Example:** `globals()[f"extract_{client_name}"] = create_dynamic_dag(client_name)`

---

## 2. Task Execution & Environment Isolation

### Task Instantiation (`@task`)
* **Syntax:** `@task(retries=int, pool='name')`
* **Explanation:** Wraps a standard Python function, isolating it into an executable Airflow node with built-in resilience and tracking.
* **Example:** `@task(retries=3, pool="database_connections")`

### Container Isolation (`@task.docker`)
* **Syntax:** `@task.docker(image='container:tag')`
* **Explanation:** Detaches execution into an isolated, ephemeral Docker container to prevent dependency conflicts on the worker node.
* **Example:** `@task.docker(image="python:3.10-slim")`

### Global Configuration Fetching (`Variable.get`)
* **Syntax:** `Variable.get('key', default='fallback')`
* **Explanation:** Retrieves global configuration parameters (API keys, thresholds, environments) stored securely in the Airflow metadata database.
* **Example:** `api_endpoint = Variable.get("weather_api_url", default="https://api.weather.com")`

### Dynamic Context Injection (`logical_date`)
* **Syntax:** `def my_task(logical_date: str):`
* **Explanation:** Airflow automatically injects built-in metadata (like the execution date) directly into task arguments during runtime via Jinja templating.
* **Example:** `print(f"Running backfill for {logical_date}")`

---

## 3. Topological Graph Routing & Logic

### Dependency Binding (`>>`)
* **Syntax:** `task_a >> [task_b, task_c] >> task_d`
* **Explanation:** Establishes the topological execution order (Directed Acyclic Graph). Lists represent tasks that execute concurrently in parallel.
* **Example:** `extract_data() >> [transform_1(), transform_2()] >> load_data()`

### Conditional Branching (`@task.branch`)
* **Syntax:** `@task.branch` -> `return 'downstream_task_id'`
* **Explanation:** Evaluates logic and returns the specific string ID of the task Airflow should execute next, automatically skipping unselected paths.
* **Example:** `return "process_data" if row_count > 0 else "send_alert"`

### Pipeline Trigger Rules (`trigger_rule`)
* **Syntax:** `@task(trigger_rule='rule_name')`
* **Explanation:** Overrides the default `all_success` dependency behavior. `none_failed` ensures final alert tasks run even if upstream branches were skipped.
* **Example:** `@task(trigger_rule="none_failed")`

---

## 4. Structural Organization & Parallelism

### Logical Task Grouping (`@task_group`)
* **Syntax:** `@task_group(group_id='name')`
* **Explanation:** Visually and logically bundles multiple related tasks together in the Airflow UI without altering the underlying execution engine.
* **Example:** `@task_group(group_id="parallel_quality_checks")`

### Dynamic Task Mapping (`.expand()`)
* **Syntax:** `target_task.expand(kwarg_name=list_variable)`
* **Explanation:** Takes a Python list generated at runtime and spawns $N$ independent, parallel task instances dynamically (MapReduce pattern).
* **Example:** `process_file.expand(file_name=list_of_s3_keys)`

---

## 5. Asynchronous Operations & External Systems

### External Probing (`@task.sensor`)
* **Syntax:** `@task.sensor(poke_interval=int, timeout=int)` -> `return PokeReturnValue(is_done=bool)`
* **Explanation:** Pauses a pipeline and repeatedly pokes an external system (like an API or directory) until a specific condition is met.
* **Example:** `return PokeReturnValue(is_done=True)`

### Deferrable Operators (Async Airflow)
* **Syntax:** `TimeDeltaSensorAsync(task_id='id', delta=timedelta)`
* **Explanation:** Fully suspends a waiting task and yields its worker slot back to the cluster. A background Triggerer process monitors the event asynchronously to prevent worker starvation.
* **Example:** `wait_task = TimeDeltaSensorAsync(task_id="wait_5m", delta=timedelta(minutes=5))`
