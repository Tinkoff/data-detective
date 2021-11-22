import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pandas import DataFrame

from data_detective_airflow.constants import S3_CONN_ID

DD_AIRFLOW_BUCKET = 'dd-airflow'

dataset = {
    'source': DataFrame(
        [
            ['dd-airflow', b'dd-airflow\n'],
            ['dd-airflow.txt', b'dd-airflow.txt\n'],
        ],
        columns=['path', 'response']
    ),
    'list_bucket': DataFrame(
        [
            ['dd-airflow', 11, 'STANDARD'],
            ['dd-airflow.txt', 15, 'STANDARD'],
        ],
        columns=['key', 'size', 'storageclass']
    ),
    'empty_list': DataFrame(
        [

        ],
        columns=['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner']
    ),
}


@pytest.fixture(scope='module')
def setup_storage():
    hook = S3Hook(S3_CONN_ID)
    bucket_name = DD_AIRFLOW_BUCKET
    bucket = hook.get_bucket(bucket_name=bucket_name)

    bucket.objects.all().delete()

    source = dataset['source']
    source.apply(
        lambda row: hook.load_bytes(row['response'], row['path'], bucket_name),
        axis=1
    )

    yield

    bucket.objects.all().delete()
