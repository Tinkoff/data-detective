import allure
from airflow import settings


from data_detective_airflow.test_utilities import run_dag_and_assert_tasks
from data_detective_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_etl_elt_dataset import dataset

dag_name = 'dummy_elt_etl'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@allure.feature('Dags')
@allure.story(dag_name)
def test_task(mocker):
    run_dag_and_assert_tasks(dag=dag, dataset=dataset, mocker=mocker)
