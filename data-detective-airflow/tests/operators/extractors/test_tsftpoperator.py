import allure
import pytest

from data_detective_airflow.operators import TSFTPOperator
from data_detective_airflow.test_utilities import create_or_get_dagrun, get_template_context, run_and_read
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
        file = prepare_data_files.get(file_key)
        task = TSFTPOperator(task_id='test_task', conn_id='ssh_service',
                             remote_filepath=file.path, dag=test_dag)

        create_or_get_dagrun(test_dag, task)

    with allure.step('Run a task'):
        result = run_and_read(task=task, context=get_template_context(task))

    with allure.step('Check result'):
        assert result is not None
        res_text = result.decode('utf-8')
        assert res_text == file.data


@allure.feature('Extractors')
@allure.story('TSFTP operator with an empty file')
@pytest.mark.parametrize('file_key', get_keys_of_empty_files())
def test_empty_file(test_dag, prepare_data_files, file_key):
    with allure.step('Create a task and context'):
        file = prepare_data_files.get(file_key).path
        task = TSFTPOperator(task_id='test_task', conn_id='ssh_service',
                             remote_filepath=file, dag=test_dag)

        create_or_get_dagrun(test_dag, task)

    with allure.step('Run a task'):
        result = run_and_read(task=task, context=get_template_context(task))

    with allure.step('Check result'):
        assert not result


@allure.feature('Extractors')
@allure.story('TSFTP operator with an invalid file')
@pytest.mark.parametrize('file_key', get_keys_of_invalid_files())
def test_invalid_file(test_dag, prepare_data_files, file_key):
    with allure.step('Create a task and context'):
        file = prepare_data_files.get(file_key)
        task = TSFTPOperator(task_id='test_task', conn_id='ssh_service',
                             remote_filepath=file.path, dag=test_dag)

        create_or_get_dagrun(test_dag, task)

    with allure.step('Run a task with exception'):
        try:
            err = None
            result = run_and_read(task=task, context=get_template_context(task))
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except file.error:
            state = 1
        except Exception as exc:
            state = -1
            err = exc
        assert state == 1, f"Details: {err}"
