from pathlib import Path

import allure
import pytest

from data_detective_airflow.dag_generator import ResultType, WorkType


@allure.feature('Works')
@allure.story('Clean work in Pickle')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_no_error_on_double_clean(test_dag, context):
    test_dag.get_work(work_type=test_dag.work_type).create(context)
    test_dag.clear_all_works(context)
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Create work in Pickle')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_create(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type)
    work.create(context)
    path = Path(work.get_path(context))
    assert work.exists(path.as_posix())
    assert work.is_dir(path.as_posix())
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Clean work in Pickle')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_clean(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type)
    work.create(context)
    test_dag.clear_all_works(context)
    path = Path(work.get_path(context))
    assert not work.exists(path.as_posix())
