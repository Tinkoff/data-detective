import uuid

import pytest
import allure
from airflow import settings

from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import get_template_context, run_task

dag_name = 'dummy_s3_large'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@allure.feature('Dags')
@allure.story(dag_name)
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker):
    dag.create_dagrun(
        run_id=f'{__name__}__{uuid.uuid1()}',
        external_trigger=True,
        state='queued'
    )
    with allure.step(task.task_id):
        run_task(task=task, context=get_template_context(task))
