# Data Engineering Learning Log: Part 38 - Trigger Rules

You just saw Branching in action. Because the pipeline went down the `log_empty_file` route, the `process_data` task was marked as **Skipped**.

But what if you wanted a final task (like a `cleanup` or `send_status_report` task) to run at the very end of the DAG, *regardless* of which branch was taken?

By default, Airflow tasks use a **Trigger Rule** called `all_success`. This means a task will ONLY run if every single task directly upstream of it succeeded. If even one upstream task is skipped or fails, the downstream task is skipped too!

To fix this, we change the Trigger Rule.

## Common Trigger Rules

* `all_success`: (Default) All parents must succeed.
* `all_failed`: All parents must fail.
* `all_done`: All parents are done (success, failed, or skipped).
* `none_failed`: All parents succeeded or were skipped (no failures).

You can apply a trigger rule in the TaskFlow API by passing it directly into the decorator:

```python
from airflow.sdk import task

@task(trigger_rule="none_failed")
def final_cleanup():
    print("Cleaning up workspace...")
```
