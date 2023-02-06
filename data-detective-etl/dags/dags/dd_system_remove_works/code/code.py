import shutil
import tempfile
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator

from data_detective_airflow.constants import (
    WORK_S3_PREFIX,
    WORK_PG_SCHEMA_PREFIX,
    WORK_FILE_PREFIX,
    WORK_S3_BUCKET,
    PG_CONN_ID,
)
from data_detective_airflow.dag_generator import TDag

from common.constants import S3_WORK_ID


def get_active_run_ids():
    return [str(run.id) for run in DagRun.find(state='running')]


def clean_s3_works(conn_id):
    hook = S3Hook(conn_id)

    active_run_ids = get_active_run_ids()
    print('Active run ids:')
    print(*active_run_ids, sep='\n')

    bucket_name = WORK_S3_BUCKET
    prefix = f'{WORK_S3_BUCKET}/{WORK_S3_PREFIX}'

    bucket = hook.get_bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        dirname = obj.key.split('/')[0]
        run_id = dirname.split('_')[-1]

        print(f'Trying to delete {obj.key}')
        print(f'Dirname: {dirname}')
        print(f'Run id from obj.key: {run_id}')

        if run_id not in active_run_ids:
            obj.delete()
            print(f'{obj.key} deleted successfully')
        else:
            print(f'{obj.key} is in use now. Cant delete.')


def clean_pg_works(conn_id):
    hook = PostgresHook(conn_id)

    active_run_ids = get_active_run_ids()

    sql_schemas = "select nspname " "from pg_catalog.pg_namespace " f"where nspname like '{WORK_PG_SCHEMA_PREFIX}_%';"

    schemas = hook.get_pandas_df(sql_schemas)

    schemas['active'] = schemas['nspname'].apply(lambda nspname: nspname.split('_')[-1] in active_run_ids)
    schemas = schemas[~schemas['active']]

    if not schemas.empty:
        schemas['drop_sql'] = schemas['nspname'].apply(lambda nspname: f'DROP SCHEMA IF EXISTS {nspname} CASCADE;')
        hook.run(sql=schemas['drop_sql'].to_list(), autocommit=True)


def clean_local_works():
    tmp_path = Path(tempfile.gettempdir())

    active_run_ids = get_active_run_ids()

    for path_ in tmp_path.iterdir():
        if path_.is_dir() and path_.name.startswith(WORK_FILE_PREFIX):
            run_id = path_.as_posix().split('_')[-1]
            if run_id not in active_run_ids:
                shutil.rmtree(path_)


def fill_dag(tdag: TDag):

    PythonOperator(task_id='clean_s3_works', python_callable=clean_s3_works, op_args=[S3_WORK_ID], dag=tdag)

    PythonOperator(task_id='clean_pg_works', python_callable=clean_pg_works, op_args=[PG_CONN_ID], dag=tdag)

    PythonOperator(task_id='clean_local_works', python_callable=clean_local_works, dag=tdag)
