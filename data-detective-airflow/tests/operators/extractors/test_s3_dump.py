import pytest
import allure
from pandas import DataFrame

from mg_airflow.constants import S3_CONN_ID
from mg_airflow.operators.extractors import PythonDump, S3Dump
from mg_airflow.dag_generator import TDag, ResultType, WorkType
from mg_airflow.test_utilities import run_task
from mg_airflow.test_utilities.assertions import assert_frame_equal
from tests_data.operators.extractors.s3_dataset import dataset, setup_storage, MG_AIRFLOW_BUCKET


@allure.feature('Extractors')
@allure.story('S3 dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_dump_single(test_dag: TDag, context, setup_storage):
    test_dag.clear()

    task = S3Dump(
        conn_id=S3_CONN_ID,
        bucket=MG_AIRFLOW_BUCKET,
        task_id="test_s3_dump_single",
        object_path=dataset['source']['path'].values[0],
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    df = task.result.read(context)
    assert_frame_equal(df, dataset['source'][['response']].iloc[0])
    test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('S3 dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_dump_source(test_dag: TDag, mocker, context, setup_storage):
    test_dag.clear()

    upstream_task = PythonDump(
        task_id="upstream_task",
        dag=test_dag,
        python_callable=lambda x: DataFrame()
    )
    run_task(upstream_task, context)
    upstream_task.result.read = mocker.MagicMock(return_value=dataset['source'][['path']])

    task = S3Dump(
        conn_id=S3_CONN_ID,
        bucket=MG_AIRFLOW_BUCKET,
        task_id="test_s3_dump_source",
        source=['upstream_task'],
        object_column='path',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)

    run_task(task=task, context=context)

    df = task.result.read(context)
    assert_frame_equal(df, dataset['source'])
    test_dag.clear_all_works(context)
