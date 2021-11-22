from datetime import datetime

import allure
from airflow import settings
from airflow.models.taskinstance import TaskInstance

from mg_airflow.dag_generator import generate_dag
from mg_airflow.test_utilities import run_and_read, run_task


@allure.feature('Dags')
@allure.story('Dummy with manual creation')
def test_dummy_dag():
    with allure.step('Init'):
        dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/dummy')
        ti = TaskInstance(task=dag.task_dict['df_now1'], execution_date=datetime.now())
        context = ti.get_template_context()
        dag.get_work(work_type=dag.work_type, work_conn_id=dag.work_conn_id).create(context)

    with allure.step('Check df_now1 step'):
        df = run_and_read(task=dag.task_dict['df_now1'], context=context)
        assert df.shape == (1, 2)

    with allure.step('Check df_now2 step'):
        df = run_and_read(task=dag.task_dict['df_now2'], context=context)
        assert df.shape == (1, 2)

    with allure.step('Check transform step'):
        df = run_and_read(task=dag.task_dict['transform'], context=context)
        assert df.shape == (1, 2)

    with allure.step('Check append_all step'):
        df = run_and_read(dag.task_dict['append_all'], context=context)
        assert df.shape == (2, 2)

    run_task(dag.task_dict['sink'], context=context)

    dag.clear_all_works(context)
