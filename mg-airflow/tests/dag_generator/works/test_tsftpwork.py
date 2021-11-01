import allure
import pytest

from mg_airflow.constants import SFTP_CONN_ID
from mg_airflow.dag_generator import ResultType, WorkType


@allure.feature('Works')
@allure.story('Clean sftp work')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_SFTP.value,
                           SFTP_CONN_ID)],
                         indirect=True)
def test_no_error_on_double_clean(test_dag, context):
    test_dag.get_work(work_type=test_dag.work_type,
                      work_conn_id=test_dag.conn_id).create(context)
    test_dag.clear_all_works(context)
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Create sftp work')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_SFTP.value,
                           SFTP_CONN_ID)],
                         indirect=True)
def test_create(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type,
                             work_conn_id=test_dag.conn_id)
    work.create(context)
    path = work.get_path(context)
    assert work.exists(path.as_posix())
    assert work.is_dir(path.as_posix())
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Clean sftp work')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_SFTP.value,
                           SFTP_CONN_ID)],
                         indirect=True)
def test_clean(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type,
                             work_conn_id=test_dag.conn_id)
    work.create(context)
    test_dag.clear_all_works(context)
    path = work.get_path(context)
    assert not work.exists(path.as_posix())
