import pytest
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import (
    create_or_get_dagrun,
    is_gen_dataset_mode,
    JSONPandasDataset,
    run_and_assert_task,
    run_and_gen_ds,
)

dag_name = 'dd_load_tuning_search_system_x_type'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')
dataset = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')

gen_dataset = is_gen_dataset_mode()


@pytest.mark.skipif(condition=gen_dataset, reason='Gen dataset')
@pytest.mark.parametrize('task', dag.tasks)
def test_task(task, mocker):
    run_and_assert_task(task=task, dataset=dataset, dag_run=create_or_get_dagrun(dag, task), mocker=mocker)


@pytest.mark.skipif(condition=(not gen_dataset), reason='Gen dataset')
@pytest.mark.parametrize('task', dag.tasks)
def test_gen_tests_data(task):
    run_and_gen_ds(
        task=task, folder=f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}', dag_run=create_or_get_dagrun(dag, task)
    )


@pytest.fixture(scope='module', autouse=True)
def setup_tables():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    queries = [
        'truncate tuning.search_system_x_type;',
    ]
    hook.run(queries)
    yield
    hook.run(queries)
