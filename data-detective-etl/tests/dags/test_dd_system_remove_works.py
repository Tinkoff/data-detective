import tempfile
from pathlib import Path

import pytest
from airflow import settings
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.constants import (
    WORK_S3_BUCKET,
    PG_CONN_ID,
    WORK_S3_PREFIX,
    WORK_PG_SCHEMA_PREFIX,
    WORK_FILE_PREFIX,
)

from common.constants import S3_WORK_ID

dag_name = 'dd_system_remove_works'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@pytest.fixture(scope='module')
def setup_works():
    s3_hook = S3Hook(S3_WORK_ID)
    pg_hook = PostgresHook(PG_CONN_ID)
    local_path = Path(tempfile.gettempdir()) / f'{WORK_FILE_PREFIX}_dir_01'
    bucket_name = WORK_S3_BUCKET
    key = f'{WORK_S3_BUCKET}/{WORK_S3_PREFIX}_wrk_01/dummy_result'
    if not s3_hook.check_for_key(key=key, bucket_name=bucket_name):
        s3_hook.load_bytes(b'dummy_bytes', key=key, bucket_name=bucket_name)
    pg_hook.run(f'CREATE SCHEMA IF NOT EXISTS {WORK_PG_SCHEMA_PREFIX}_schema_01;')
    if not local_path.exists():
        local_path.mkdir()

    yield

    if s3_hook.check_for_key(key=key, bucket_name=bucket_name):
        raise AssertionError('s3')
    if pg_hook.get_first(
        sql=f"select 1 from pg_catalog.pg_namespace where nspname = '{WORK_PG_SCHEMA_PREFIX}_schema_01';"
    ):
        raise AssertionError('pg')
    if local_path.exists():
        raise AssertionError('local')


@pytest.mark.parametrize('task', dag.tasks)
@pytest.mark.usefixtures('setup_works')
def test_task(task):
    task.execute(context={})


@pytest.mark.parametrize('task', dag.tasks)
def test_task_empty(task):
    task.execute(context={})
