from datetime import datetime
from pathlib import Path
import logging

import allure
import pytest
from airflow.models.taskinstance import TaskInstance
from pandas import DataFrame

from mg_airflow.dag_generator import ResultType, WorkType
from mg_airflow.operators import PythonDump
from mg_airflow.test_utilities import run_task
from tests_data.operators.extractors.python_dump_data import data_dict, only_columns_dict
from tests_data.fixtures.PY.python_fixtures import invalid_python_dump_data as _data


@allure.feature('Extractors')
@allure.story('Python dump without args')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_python_dump_without_args(test_dag, context):
    def python2df_callable_without_args(_context):
        return DataFrame.from_dict(data_dict)

    with allure.step('Create and run a task'):
        test_dag.clear()

        task = PythonDump(
            task_id="test_python_dump",
            dag=test_dag,
            python_callable=python2df_callable_without_args
        )

        TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
        test_dag.get_work(test_dag.conn_id).create(context)
        run_task(task=task, context=context)

        assert task.result is not None
        assert task.result.name is not None

    with allure.step('Check result'):
        result = f'{task.result.work.get_path(context)}/{task.result.name}.p'
        assert Path(result).exists()
        assert Path(result).is_file()
        assert task.result.read(context).shape == (3, 2)

        test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('Python dump with args')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_python_dump_with_args(test_dag, context):
    test_dag.clear()

    def python2df_callable_with_args(_context, d):
        return DataFrame.from_dict(d)

    with allure.step('Create and run a task'):
        task = PythonDump(
            task_id="test_python_dump",
            dag=test_dag,
            python_callable=python2df_callable_with_args,
            op_kwargs={'d': data_dict}
        )

        test_dag.get_work(test_dag.conn_id).create(context)
        run_task(task=task, context=context)

        assert task.result is not None
        assert task.result.name is not None

    with allure.step('Check result'):
        result = f'{task.result.work.get_path(context)}/{task.result.name}.p'
        assert Path(result).exists()
        assert Path(result).is_file()
        assert task.result.read(context).shape == (3, 2)

        test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('Python dump with lambda args')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_python_dump_lambda_with_args(test_dag, context):
    with allure.step('Create and run a task'):
        test_dag.clear()

        task = PythonDump(
            task_id="test_python_dump",
            dag=test_dag,
            python_callable=lambda _context, d: DataFrame.from_dict(d),
            op_kwargs={'d': data_dict}
        )

        test_dag.get_work(test_dag.conn_id).create(context)
        run_task(task=task, context=context)

        assert task.result is not None
        assert task.result.name is not None

    with allure.step('Check result'):
        result = f'{task.result.work.get_path(context)}/{task.result.name}.p'

        assert Path(result).exists()
        assert Path(result).is_file()
        assert task.result.read(context).shape == (3, 2)

        test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('Python dump with an empty target')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_python_dump_empty_target(test_dag, context):
    def python2df_callable_without_args(_context):
        return DataFrame.from_dict(only_columns_dict)

    with allure.step('Create and run a task'):
        test_dag.clear()

        task = PythonDump(
            task_id="test_python_dump",
            dag=test_dag,
            python_callable=python2df_callable_without_args
        )

        TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
        test_dag.get_work(test_dag.conn_id).create(context)
        run_task(task=task, context=context)

        assert task.result is not None
        assert task.result.name is not None

    with allure.step('Check result'):
        result = f'{task.result.work.get_path(context)}/{task.result.name}.p'
        assert Path(result).exists()
        assert Path(result).is_file()
        assert task.result.read(context).empty

        test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('Python dump with an invalid code')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_python_dump_invalid_code(test_dag, context, _data):
    with allure.step('Create task'):
        test_dag.clear()

        task = PythonDump(
            task_id="test_python_dump",
            dag=test_dag,
            python_callable=_data.extract_func
        )

        ti = TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
        test_dag.get_work(test_dag.conn_id).create(context)

        with allure.step('Run task with exception'):
            try:
                err = None
                logging.info('Task execute:')
                run_task(task=task, context=context)
                # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
                state = 0
            except _data.exception:
                state = 1
            except Exception as exc:
                err = exc
                state = -1
            if err:
                logging.error(f"Task failed with an error: {err}.")
            assert state == 1

        test_dag.clear_all_works(context)
