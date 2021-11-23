import pandas as pd
import petl as etl
from airflow.models import TaskInstance
from airflow.utils import timezone

from data_detective_airflow.operators.tbaseoperator import TBaseOperator
from data_detective_airflow.test_utilities.assertions import to_bytes
from data_detective_airflow.test_utilities.test_helper import run_and_read

DEFAULT_EXCLUDE_COLUMNS = ['processed_dttm']


def task_assertion_petl(task: TBaseOperator, dataset: dict, exclude_cols: list = None):
    task_instance = TaskInstance(task=task, execution_date=timezone.utcnow())

    context = task_instance.get_template_context()

    work = task.dag.get_work(task.work_type, task.work_conn_id)
    work.create(context)

    actual = run_and_read(task=task, context=context)
    if isinstance(actual, tuple):
        actual = etl.wrap(actual)

    if actual is not None:
        expected = dataset[task.task_id]

        actual_tbl = etl.fromdataframe(actual) if isinstance(actual, pd.DataFrame) else actual
        expected_tbl = etl.fromdataframe(expected) if isinstance(expected, pd.DataFrame) else expected

        e_cols = DEFAULT_EXCLUDE_COLUMNS
        if exclude_cols:
            e_cols.extend(exclude_cols)

        actual_lists = (
            actual_tbl.cut([field for field in etl.header(actual_tbl) if field not in e_cols])
            .replaceall(None, '')
            .listoflists()
        )

        expected_lists = expected_tbl.cut(
            [field for field in etl.header(expected_tbl) if field not in e_cols]
        ).listoflists()

        assert to_bytes(actual_lists) == to_bytes(expected_lists)
