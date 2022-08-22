import json

import pandas
import pytest
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_detective_airflow.constants import JSON_FIELDS, PG_CONN_ID
from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import (
    JSONPandasDataset,
    create_or_get_dagrun,
    is_gen_dataset_mode,
    run_and_assert_task,
    run_and_gen_ds,
)

dag_name = 'dd_load_elastic_search'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')
dataset = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')

dds_pg_entity = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/dd_load_dds_pg')['upload_dds_entity']
dds_root_entity = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/dd_load_dds_root')['upload_dds_entity']
dds_dag_entity = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/dd_load_dds_dag')['upload_dds_entity']

gen_dataset = is_gen_dataset_mode()


@pytest.mark.skipif(condition=gen_dataset, reason='Gen dataset')
@pytest.mark.parametrize('task', dag.tasks)
def test_task(task, mocker):
    if task.task_id != 'upload_dd_search':
        run_and_assert_task(task=task, dataset=dataset, dag_run=create_or_get_dagrun(dag, task), mocker=mocker)


@pytest.mark.skipif(condition=(not gen_dataset), reason='Gen dataset')
@pytest.mark.parametrize('task', dag.tasks)
def test_gen_tests_data(task):
    if task.task_id != 'upload_dd_search':
        run_and_gen_ds(
            task=task,
            folder=f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}',
            dag_run=create_or_get_dagrun(dag, task),
        )


@pytest.fixture(scope='module', autouse=True)
def setup_tables():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    queries = [
        'truncate dds.entity;',
        'truncate dds.relation;',
    ]
    hook.run(queries)

    dds_entity = pandas.concat([dds_pg_entity, dds_root_entity, dds_dag_entity], ignore_index=True)
    json_columns = list(JSON_FIELDS & set(dds_entity.columns))
    for column in json_columns:
        dds_entity[column] = dds_entity[column].apply(lambda row: json.dumps(row) if isinstance(row, dict) else None)
    dds_entity.to_sql(con=hook.get_uri(), schema='dds', name='entity', if_exists='append', index=False)
    yield
    hook.run(queries)
