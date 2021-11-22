import allure
import pytest

from data_detective_airflow.constants import S3_CONN_ID
from data_detective_airflow.dag_generator import ResultType, WorkType


@allure.feature('Works')
@allure.story('Clean work in S3')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_S3.value,
                           S3_CONN_ID)],
                         indirect=True)
def test_no_error_on_double_clean(test_dag, context):
    test_dag.get_work(work_type=test_dag.work_type,
                      work_conn_id=test_dag.conn_id).create(context)
    test_dag.clear_all_works(context)
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Clean work in S3')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_S3.value,
                           S3_CONN_ID)],
                         indirect=True)
def test_clean(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type,
                             work_conn_id=test_dag.conn_id)
    work.create(context)
    filepath = work.get_path(context) / 'testfile.txt'
    work.write_bytes(filepath.as_posix(), b'test')
    assert work.exists(filepath.as_posix())
    test_dag.clear_all_works(context)
    assert not work.exists(filepath.as_posix())
