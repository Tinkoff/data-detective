import logging
from datetime import datetime

import allure
import pytest
from airflow.models import TaskInstance
from pandas import DataFrame

from data_detective_airflow.operators.extractors import PythonDump
from data_detective_airflow.dag_generator import ResultType, WorkType
from data_detective_airflow.test_utilities import run_task


@allure.feature('Works')
@allure.story('Get size in Pickle')
@pytest.mark.parametrize(
    'test_dag, expected',
    [
        ((ResultType.RESULT_PICKLE.value, WorkType.WORK_FILE.value, None), '740.00 B'),
        ((ResultType.RESULT_PICKLE.value, WorkType.WORK_S3.value, None), '740.00 B'),
        ((ResultType.RESULT_PICKLE.value, WorkType.WORK_SFTP.value, None), '740.00 B'),
        ((ResultType.RESULT_PG.value, WorkType.WORK_PG.value, None), '8.00 KB')
    ],
    indirect=['test_dag'],
)
def test_get_size(test_dag, context, expected, caplog):
    task = PythonDump(
        task_id="test_python_dump",
        dag=test_dag,
        python_callable=lambda _context: DataFrame([[1, 2], [3, 4]], columns=('foo', 'bar'))
    )

    TaskInstance(task=task, execution_date=datetime.now())
    test_dag.get_work(test_dag.conn_id).create(context)
    with caplog.at_level(logging.INFO):
        run_task(task=task, context=context)
    assert f'Dumped {expected}' in caplog.text

    test_dag.clear_all_works(context)
