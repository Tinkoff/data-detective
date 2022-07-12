from datetime import datetime, timezone, timedelta
from functools import lru_cache

from airflow.models import DagRun, TaskInstance
from airflow.utils.context import Context
from airflow.utils.types import DagRunType


@lru_cache(1024)
def get_template_context(task) -> Context:
    run = task.dag.get_last_dagrun(include_externally_triggered=True)
    task_instance = TaskInstance(task=task, run_id=run.run_id)
    return task_instance.get_template_context()


def create_or_get_dagrun(dag, task) -> DagRun:
    if task == dag.tasks[0]:
        return dag.create_dagrun(
            execution_date=datetime.now(timezone(offset=timedelta(hours=0))),
            run_type=DagRunType.MANUAL,
            state='queued',
            data_interval=(
                datetime.now(timezone(offset=timedelta(hours=0))),
                datetime.now(timezone(offset=timedelta(hours=0))) + timedelta(hours=1)
            )
        )
    return dag.get_last_dagrun(include_externally_triggered=True)
