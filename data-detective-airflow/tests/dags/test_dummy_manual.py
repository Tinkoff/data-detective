import uuid

import allure
from airflow import settings

from data_detective_airflow.dag_generator import generate_dag
from data_detective_airflow.test_utilities import get_template_context, run_and_read, run_task


@allure.feature('Dags')
@allure.story('Dummy with manual creation')
def test_dummy_dag():
    with allure.step('Init'):
        dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/dummy')
        run_id = f'{__name__}__{uuid.uuid1()}'
        dag.create_dagrun(
            run_id=run_id,
            external_trigger=True,
            state='queued'
        )
        context = get_template_context(dag.tasks[0])
        dag.clear_all_works(context)

    with allure.step('Check df_now1 step'):
        task = dag.task_dict['df_now1']
        df = run_and_read(task=task, context=get_template_context(task))
        assert df.shape == (1, 2)

    with allure.step('Check df_now2 step'):
        task = dag.task_dict['df_now2']
        df = run_and_read(task=task, context=get_template_context(task))
        assert df.shape == (1, 2)

    with allure.step('Check transform step'):
        task = dag.task_dict['transform']
        df = run_and_read(task=task, context=get_template_context(task))
        assert df.shape == (1, 2)

    with allure.step('Check append_all step'):
        task = dag.task_dict['append_all']
        df = run_and_read(task=task, context=get_template_context(task))
        assert df.shape == (2, 2)

    with allure.step('Run sink'):
        task = dag.task_dict['sink']
        run_task(task=task, context=get_template_context(task))

    dag.clear_all_works(context)
