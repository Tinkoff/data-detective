import pytest
import allure
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pandas import DataFrame

from mg_airflow.constants import S3_CONN_ID, WORK_S3_BUCKET
from mg_airflow.dag_generator import TDag, ResultType, WorkType
from mg_airflow.operators.extractors import PythonDump
from mg_airflow.operators.sinks import S3Load
from mg_airflow.test_utilities import run_task
from tests_data.operators.sinks.s3_load_dataset import dataset


@allure.feature('Sinks')
@allure.story('S3 load')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_load_single(test_dag: TDag, mocker, context):
    test_dag.clear()

    upstream_task = PythonDump(
        python_callable=lambda x: DataFrame(),
        task_id='upstream_task',
        dag=test_dag)

    run_task(upstream_task, context)
    upstream_task.result.read = mocker.MagicMock(return_value=dataset)

    task = S3Load(
        conn_id=S3_CONN_ID,
        bucket=WORK_S3_BUCKET,
        task_id="test_s3_load",
        source=['upstream_task'],
        filename_column='path',
        bytes_column='data',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    # s3_dump.py:67
    hook = S3Hook(S3_CONN_ID)
    client = hook.get_conn()
    dataset['comparison'] = dataset.apply(
        lambda row: client.get_object(
            Bucket=WORK_S3_BUCKET,
            Key=row['path']
        )['Body'].read() == row['data'],
        axis=1
    )

    # check all True
    assert dataset['comparison'].all()
    test_dag.clear_all_works(context)


@allure.feature('Sinks')
@allure.story('S3 load')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_load_with_metadata(test_dag: TDag, mocker, context):
    test_dag.clear()

    upstream_task = PythonDump(
        python_callable=lambda x: DataFrame(),
        task_id='upstream_task',
        dag=test_dag)

    run_task(upstream_task, context)
    upstream_task.result.read = mocker.MagicMock(return_value=dataset)

    task = S3Load(
        conn_id=S3_CONN_ID,
        bucket=WORK_S3_BUCKET,
        task_id="test_s3_load",
        source=['upstream_task'],
        filename_column='path',
        bytes_column='data',
        metadata_column='metadata',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    hook = S3Hook(S3_CONN_ID)
    client = hook.get_conn()
    dataset['upload'] = dataset.apply(
        lambda row: client.get_object(
            Bucket=WORK_S3_BUCKET,
            Key=row['path']
        ),
        axis=1
    )
    assert dataset['upload'][1]['Metadata'] == {'content-type': 'image/svg+xml'}
    assert dataset['upload'][1]['ContentType'] == 'image/svg+xml'
    test_dag.clear_all_works(context)


@allure.feature('Sinks')
@allure.story('S3 load')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_load_update_metadata(test_dag: TDag, mocker, context):
    test_dag.clear()

    upstream_task = PythonDump(
        python_callable=lambda x: DataFrame(),
        task_id='upstream_task',
        dag=test_dag)

    run_task(upstream_task, context)
    upstream_task.result.read = mocker.MagicMock(return_value=dataset)

    task = S3Load(
        conn_id=S3_CONN_ID,
        bucket=WORK_S3_BUCKET,
        task_id="test_s3_load",
        source=['upstream_task'],
        filename_column='path',
        bytes_column='data',
        metadata_column='metadata',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    # заружаем первый раз
    run_task(task=task, context=context)

    # обновляем метаданные
    dataset['metadata'] = dataset['metadata'].apply(lambda row: {'ContentType': 'binary/octet-stream'})
    upstream_task.result.read = mocker.MagicMock(return_value=dataset)

    task = S3Load(
        conn_id=S3_CONN_ID,
        bucket=WORK_S3_BUCKET,
        task_id="test_s3_load2",
        source=['upstream_task'],
        filename_column='path',
        bytes_column='data',
        metadata_column='metadata',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    # заружаем второй раз, метаданные должны обновиться
    run_task(task=task, context=context)

    hook = S3Hook(S3_CONN_ID)
    client = hook.get_conn()
    dataset['upload'] = dataset.apply(
        lambda row: client.get_object(
            Bucket=WORK_S3_BUCKET,
            Key=row['path']
        ),
        axis=1
    )

    assert dataset['upload'][1]['ContentType'] == 'binary/octet-stream'
    test_dag.clear_all_works(context)
