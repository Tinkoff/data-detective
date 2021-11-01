import pytest
import allure
from airflow import settings


from mg_airflow.test_utilities import run_and_assert_task
from mg_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_etl_elt_dataset import dataset

dag_name = 'dummy_elt_etl'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@allure.feature('Dags')
@allure.story(dag_name)
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker):
    with allure.step(task.task_id):
        run_and_assert_task(task=task, dataset=dataset, mocker=mocker)
