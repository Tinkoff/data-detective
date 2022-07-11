import allure
from airflow import settings

from data_detective_airflow.test_utilities import run_dag_and_assert_tasks
from data_detective_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_s3_dataset import dataset
from tests_data.operators.extractors.s3_dataset import setup_storage

dag_name = 'dummy_s3'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@allure.feature('Dags')
@allure.story(dag_name)
def test_task(mocker, setup_storage):
    run_dag_and_assert_tasks(dag=dag, dataset=dataset, mocker=mocker, debug=True)
