from functools import lru_cache

from airflow.utils.context import Context
from airflow.models import TaskInstance


@lru_cache(1024)
def get_template_context(task) -> Context:
    run = task.dag.get_last_dagrun(include_externally_triggered=True)
    task_instance = TaskInstance(task=task, run_id=run.run_id)
    return task_instance.get_template_context()
