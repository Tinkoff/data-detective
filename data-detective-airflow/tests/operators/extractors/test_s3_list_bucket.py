import pytest
import allure

from mg_airflow.constants import S3_CONN_ID
from mg_airflow.operators import S3ListBucket
from mg_airflow.dag_generator import TDag, ResultType, WorkType
from mg_airflow.test_utilities import run_task, assert_frame_equal
from tests_data.operators.extractors.s3_dataset import dataset, setup_storage, MG_AIRFLOW_BUCKET


@allure.feature('Extractors')
@allure.story('S3 list bucket')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_list_bucket(test_dag: TDag, context, setup_storage):
    task = S3ListBucket(
        conn_id=S3_CONN_ID,
        bucket=MG_AIRFLOW_BUCKET,
        task_id="test_s3_list_bucket",
        prefix=dataset['source']['path'].values[0],
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    df = task.result.read(context)
    assert_frame_equal(df[[*dataset['list_bucket'].columns]], dataset['list_bucket'])
    test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('S3 list with empty result')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_empty_list(test_dag: TDag, context, setup_storage):
    task = S3ListBucket(
        conn_id=S3_CONN_ID,
        bucket=MG_AIRFLOW_BUCKET,
        task_id="test_s3_empty_list",
        prefix='empty',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    df = task.result.read(context)
    assert_frame_equal(df[[*dataset['empty_list'].columns]], dataset['empty_list'])
    test_dag.clear_all_works(context)
