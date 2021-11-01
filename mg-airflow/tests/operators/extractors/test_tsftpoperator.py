import datetime
import allure
import pytest

from airflow.models import TaskInstance

from mg_airflow.operators import TSFTPOperator
from mg_airflow.test_utilities import run_and_read
from tests_data.fixtures.File.file_fixtures import (
    prepare_data_files,
    get_keys_of_correct_files,
    get_keys_of_empty_files,
    get_keys_of_invalid_files,
)


@allure.feature('Extractors')
@allure.story('TSFTP operator')
@pytest.mark.parametrize('file_key', get_keys_of_correct_files())
def test_real_file(test_dag, prepare_data_files, file_key):
    with allure.step('Create a task and context'):
        test_dag.clear()
        file = prepare_data_files.get(file_key)
        task = TSFTPOperator(task_id='test_task', conn_id='ssh_service',
                             remote_filepath=file.path, dag=test_dag)
        ti = TaskInstance(task=task, execution_date=datetime.datetime.now())
        context = ti.get_template_context()
        test_dag.get_work(test_dag.conn_id).create(context)

    with allure.step('Run a task'):
        result = run_and_read(task=task, context=context)

    with allure.step('Check result'):
        assert result is not None
        res_text = result.decode('utf-8')
        assert res_text == file.data
        test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('TSFTP operator with an empty file')
@pytest.mark.parametrize('file_key', get_keys_of_empty_files())
def test_empty_file(test_dag, prepare_data_files, file_key):
    with allure.step('Create a task and context'):
        test_dag.clear()
        file = prepare_data_files.get(file_key).path
        task = TSFTPOperator(task_id='test_task', conn_id='ssh_service',
                             remote_filepath=file, dag=test_dag)
        ti = TaskInstance(task=task, execution_date=datetime.datetime.now())
        context = ti.get_template_context()
        test_dag.get_work(test_dag.conn_id).create(context)

    with allure.step('Run a task'):
        result = run_and_read(task=task, context=context)

    with allure.step('Check result'):
        assert not result
        test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('TSFTP operator with an invalid file')
@pytest.mark.parametrize('file_key', get_keys_of_invalid_files())
def test_invalid_file(test_dag, prepare_data_files, file_key):
    with allure.step('Create a task and context'):
        test_dag.clear()
        file = prepare_data_files.get(file_key)
        task = TSFTPOperator(task_id='test_task', conn_id='ssh_service',
                             remote_filepath=file.path, dag=test_dag)
        ti = TaskInstance(task=task, execution_date=datetime.datetime.now())
        context = ti.get_template_context()
        test_dag.get_work(test_dag.conn_id).create(context)

    with allure.step('Run a task with exception'):
        try:
            err = None
            result = run_and_read(task=task, context=context)
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except file.error:
            state = 1
        except Exception as exc:
            state = -1
            err = exc
        assert state == 1, f"Details: {err}"
        test_dag.clear_all_works(context)
