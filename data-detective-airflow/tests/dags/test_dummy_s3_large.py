import pytest
import allure
from airflow import settings
from airflow.models import TaskInstance
from airflow.utils import timezone

from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import run_task

dag_name = 'dummy_s3_large'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@allure.feature('Dags')
@allure.story(dag_name)
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker):
    with allure.step(task.task_id):
        task_instance = TaskInstance(task=task, execution_date=timezone.utcnow())
        context = task_instance.get_template_context()
        run_task(task=task, context=context)
