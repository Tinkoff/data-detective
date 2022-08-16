import json

import pytest
import pandas
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID, JSON_FIELDS
from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import (
    create_or_get_dagrun,
    is_gen_dataset_mode,
    JSONPandasDataset,
    run_and_assert_task,
    run_and_gen_ds,
)

dag_name = 'dd_load_tuning_breadcrumb'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')
dataset = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')
root_dataset = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/dd_load_dds_root')

gen_dataset = is_gen_dataset_mode()


@pytest.mark.skipif(condition=gen_dataset, reason='Gen dataset')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker, setup_tables):
    run_and_assert_task(task=task, dataset=dataset, dag_run=create_or_get_dagrun(dag, task), mocker=mocker)


@pytest.mark.skipif(condition=(not gen_dataset), reason='Gen dataset')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_gen_tests_data(task, mocker, setup_tables):
    run_and_gen_ds(task, f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')


setup_dataset = {
    'dds_entity': pandas.concat([
        root_dataset['upload_dds_entity'],
    ],
        ignore_index=True),
    'dds_relation': pandas.concat([
        root_dataset['upload_dds_relation'],
    ],
        ignore_index=True)
}


@pytest.fixture(scope='module')
def setup_tables():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    queries = [
        'truncate dds.entity;',
        'truncate dds.relation;',
        'truncate tuning.breadcrumb;',
    ]
    hook.run(queries)
    for df_name in setup_dataset:
        df = setup_dataset[df_name]
        name = 'relation'
        if df_name == 'dds_entity':
            json_columns = list(JSON_FIELDS & set(df.columns))
            for column in json_columns:
                df[column] = df[column].apply(
                    lambda row: json.dumps(row) if isinstance(row, dict) else None
                )
            name = 'entity'
        df.to_sql(con=hook.get_uri(), schema='dds', name=name,
                  if_exists='append', index=False)
    yield
    hook.run(queries)
