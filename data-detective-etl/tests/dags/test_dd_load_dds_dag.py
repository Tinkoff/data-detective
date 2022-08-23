import petl
import pytest
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import (
    JSONPandasDataset,
    create_or_get_dagrun,
    is_gen_dataset_mode,
    run_and_assert_task,
    run_and_gen_ds,
)

dag_name = 'dd_load_dds_dag'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')
dataset = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')

gen_dataset = is_gen_dataset_mode()

# if False don't get real data from dags code.py
get_real_data_from_dag = False

# This task_id get data from pre generated json
fake_task_id = ['get_list_of_dags', 'add_code_files_to_dags']


def fake_data(self, context) -> None:
    data = dataset[context["task"].task_id]
    self.result.write(petl.fromdataframe(data.where(data.notnull(), None)).tupleoftuples(), context)


@pytest.mark.skipif(condition=gen_dataset, reason='Gen dataset')
@pytest.mark.parametrize('task', dag.tasks)
def test_task(task, mocker):
    if not get_real_data_from_dag and task.task_id in fake_task_id:
        mocker.patch(f'data_detective_airflow.operators.PyTransform.execute', new=fake_data)
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
        'truncate dds.entity;',
        'truncate dds.relation;',
    ]
    hook.run(queries)
    yield
    hook.run(queries)
