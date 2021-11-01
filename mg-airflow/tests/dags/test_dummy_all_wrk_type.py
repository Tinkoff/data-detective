import pytest
import allure
from airflow import settings


from mg_airflow.test_utilities import run_and_assert_task
from mg_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_all_wrk_type_dataset import dataset

dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/dummy_all_wrk_type')


@allure.feature('Dags')
@allure.story('Dummy with all work types')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker):
    with allure.step(task.task_id):
        run_and_assert_task(task=task, dataset=dataset, mocker=mocker)
