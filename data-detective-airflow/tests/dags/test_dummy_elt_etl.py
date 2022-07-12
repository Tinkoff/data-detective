import pytest
import allure
from airflow import settings


from data_detective_airflow.test_utilities import create_or_get_dagrun, run_and_assert
from data_detective_airflow.dag_generator import generate_dag
from tests_data.dags.dummy_etl_elt_dataset import dataset

dag_name = 'dummy_elt_etl'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')


@allure.feature('Dags')
@allure.story(dag_name)
@pytest.mark.parametrize('task_id', dag.task_ids)
def test_task(task_id, mocker):
    with allure.step(task_id):
        run_and_assert(
            dag=dag,
            task_id=task_id,
            test_datasets=dataset,
            mocker=mocker,
            dag_run=create_or_get_dagrun(dag, dag.task_dict[task_id]),
        )
