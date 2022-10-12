import pytest
import allure
from pandas import DataFrame

from data_detective_airflow.constants import S3_CONN_ID
from data_detective_airflow.operators.extractors import PythonDump, S3Dump
from data_detective_airflow.dag_generator import TDag, ResultType, WorkType
from data_detective_airflow.test_utilities import create_or_get_dagrun, get_template_context, run_task
from data_detective_airflow.test_utilities.assertions import assert_frame_equal
from tests_data.operators.extractors.s3_dataset import dataset, setup_storage, DD_AIRFLOW_BUCKET


@allure.feature('Extractors')
@allure.story('S3 dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_dump_single(test_dag: TDag, setup_storage):
    test_dag.clear()

    task = S3Dump(
        conn_id=S3_CONN_ID,
        bucket=DD_AIRFLOW_BUCKET,
        task_id="test_s3_dump_single",
        object_path=dataset['source']['path'].values[0],
        dag=test_dag)

    create_or_get_dagrun(test_dag, task)
    context = get_template_context(task)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    df = task.result.read(context)
    assert_frame_equal(df, dataset['source'][['response']].iloc[0])


@allure.feature('Extractors')
@allure.story('S3 dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_dump_source(test_dag: TDag, mocker, setup_storage):
    test_dag.clear()

    upstream_task = PythonDump(
        task_id="upstream_task",
        dag=test_dag,
        python_callable=lambda x: DataFrame()
    )

    task = S3Dump(
        conn_id=S3_CONN_ID,
        bucket=DD_AIRFLOW_BUCKET,
        task_id="test_s3_dump_source",
        source=['upstream_task'],
        object_column='path',
        dag=test_dag)

    create_or_get_dagrun(test_dag, upstream_task)
    context = get_template_context(task)

    run_task(upstream_task, context)
    upstream_task.result.read = mocker.MagicMock(return_value=dataset['source'][['path']])

    test_dag.get_work(test_dag.conn_id).create(context)

    run_task(task=task, context=context)

    df = task.result.read(context)
    assert_frame_equal(df, dataset['source'])
