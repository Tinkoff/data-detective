import pytest
import allure
from airflow import settings


from mg_airflow.test_utilities import run_and_assert_task
from mg_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_s3_dataset import dataset
from tests_data.operators.extractors.s3_dataset import setup_storage

dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/dummy_s3')


@allure.feature('Dags')
@allure.story('Dummy with s3 dump/load')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker, setup_storage):
    with allure.step(task.task_id):
        run_and_assert_task(task=task, dataset=dataset, mocker=mocker, debug=True)
