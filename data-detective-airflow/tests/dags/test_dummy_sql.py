import pytest
import allure
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.test_utilities import run_and_assert_task
from data_detective_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_sql_dataset import dataset

dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/dummy_sql')


@allure.feature('Dags')
@allure.story('Dummy with sql')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker, setup_tables):
    with allure.step(task.task_id):
        run_and_assert_task(task=task, dataset=dataset, mocker=mocker)


@pytest.fixture(scope='module')
def setup_tables():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(['truncate test2;'])
    yield
    hook.run(['truncate test2;'])
