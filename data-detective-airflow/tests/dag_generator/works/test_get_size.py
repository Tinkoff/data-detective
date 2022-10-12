import logging
import uuid

import allure
import pytest
from pandas import DataFrame

from data_detective_airflow.operators.extractors import PythonDump
from data_detective_airflow.dag_generator import ResultType, WorkType
from data_detective_airflow.test_utilities import create_or_get_dagrun, get_template_context, run_task


@allure.feature('Works')
@allure.story('Get size in Pickle')
@pytest.mark.parametrize(
    'test_dag, expected',
    [
        ((ResultType.RESULT_PICKLE.value, WorkType.WORK_FILE.value, None), '694.00 B'),
        ((ResultType.RESULT_PICKLE.value, WorkType.WORK_S3.value, None), '694.00 B'),
        ((ResultType.RESULT_PICKLE.value, WorkType.WORK_SFTP.value, None), '694.00 B'),
        ((ResultType.RESULT_PG.value, WorkType.WORK_PG.value, None), '8.00 KB')
    ],
    indirect=['test_dag'],
)
def test_get_size(test_dag, expected, caplog):
    task = PythonDump(
        task_id=f'test_get_size__{uuid.uuid1()}',
        dag=test_dag,
        python_callable=lambda _context: DataFrame([[1, 2], [3, 4]], columns=('foo', 'bar'))
    )
    create_or_get_dagrun(test_dag, task)
    with caplog.at_level(logging.INFO):
        run_task(task=task, context=get_template_context(task))
    assert f'Dumped {expected}' in caplog.text
