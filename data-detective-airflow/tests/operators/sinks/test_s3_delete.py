from io import BytesIO

import pytest
import allure
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pandas import DataFrame

from data_detective_airflow.constants import S3_CONN_ID, WORK_S3_BUCKET
from data_detective_airflow.dag_generator import TDag, ResultType, WorkType
from data_detective_airflow.operators.extractors import PythonDump
from data_detective_airflow.operators.sinks import S3Delete
from data_detective_airflow.test_utilities import get_template_context, run_task
from tests_data.operators.sinks.s3_delete_dataset import dataset


@allure.feature('Sinks')
@allure.story('S3 delete')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_s3_delete(test_dag: TDag, mocker, context):
    test_dag.clear()

    # s3_dump.py:67
    hook = S3Hook(S3_CONN_ID)
    client = hook.get_conn()
    client.upload_fileobj(
        Fileobj=BytesIO(b'deleteme'),
        Bucket=WORK_S3_BUCKET,
        Key=dataset['path'][0]
    )

    upstream_task = PythonDump(
        python_callable=lambda x: DataFrame(),
        task_id='upstream_task',
        dag=test_dag)

    run_task(upstream_task, get_template_context(upstream_task))
    upstream_task.result.read = mocker.MagicMock(return_value=dataset)

    task = S3Delete(
        conn_id=S3_CONN_ID,
        bucket=WORK_S3_BUCKET,
        task_id="test_s3_delete",
        source=['upstream_task'],
        filename_column='path',
        dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    existed_objects = client.list_objects(
        Bucket=WORK_S3_BUCKET,
        Prefix=dataset['path'][0]
    )

    assert 'Contents' not in existed_objects
