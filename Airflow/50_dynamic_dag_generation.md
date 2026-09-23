# Data Engineering Learning Log: Part 49 - Dynamic DAG Generation

When migrating a massive legacy architecture, manually writing dozens of identical Python files is a violation of the DRY (Don't Repeat Yourself) principle.

Instead, Airflow allows you to write a single Python script that reads a configuration file and dynamically generates $N$ distinct DAGs into the global namespace.

## The `globals()` Dictionary Pattern

Airflow parses files looking for DAG objects at the top level. By injecting dynamically created DAGs into the native Python `globals()` dictionary, Airflow registers them exactly as if you had typed them all out by hand.

```python
from airflow.sdk import dag, task
from datetime import datetime

# 1. A configuration dictionary (often loaded from a YAML/JSON file)
report_configs = {
    "finance_report": {"schedule": "@daily", "target": "finance_db"},
    "hr_report": {"schedule": "@weekly", "target": "hr_db"}
}

# 2. A factory function that builds a DAG
def create_dynamic_dag(dag_id, schedule_str, target_db):
    
    @dag(dag_id=dag_id, schedule=schedule_str, start_date=datetime(2026, 1, 1), catchup=False)
    def dynamic_generated_dag():
        
        @task
        def run_extraction():
            print(f"Extracting data for {target_db}")
            
        run_extraction()
        
    return dynamic_generated_dag()

# 3. The Generator Loop
for report_name, config in report_configs.items():
    generated_dag_id = f"extract_{report_name}"
    # Inject the generated DAG into the global namespace
    globals()[generated_dag_id] = create_dynamic_dag(generated_dag_id, config["schedule"], config["target"])
```
