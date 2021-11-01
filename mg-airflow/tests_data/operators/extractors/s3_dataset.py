import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pandas import DataFrame

from mg_airflow.constants import S3_CONN_ID

MG_AIRFLOW_BUCKET = 'mg-airflow'

dataset = {
    'source': DataFrame(
        [
            ['mg-airflow', b'mg-airflow\n'],
            ['mg-airflow.txt', b'mg-airflow.txt\n'],
        ],
        columns=['path', 'response']
    ),
    'list_bucket': DataFrame(
        [
            ['mg-airflow', 11, 'STANDARD'],
            ['mg-airflow.txt', 15, 'STANDARD'],
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
    bucket_name = MG_AIRFLOW_BUCKET
    bucket = hook.get_bucket(bucket_name=bucket_name)

    bucket.objects.all().delete()

    source = dataset['source']
    source.apply(
        lambda row: hook.load_bytes(row['response'], row['path'], bucket_name),
        axis=1
    )

    yield

    bucket.objects.all().delete()
