from datetime import datetime

import allure

from airflow import settings
from airflow.models.taskinstance import TaskInstance

from mg_airflow.dag_generator import generate_dag
from mg_airflow.test_utilities import run_and_read


@allure.feature('Dag Generator')
@allure.story('Generate dag by python')
def test_generate_dag_python():
    dag = generate_dag(dag_dir=f'{settings.AIRFLOW_HOME}/tests_data/dag_generator/Python/dags/dummy',
                       dag_id='test_python_dummy')
    assert dag.task_ids == ['df_left', 'df_right', 'append_everything']
    assert dag.result_type == 'pickle'
    assert dag.tags == ['dummy', 'python']


@allure.feature('Dag Generator')
@allure.story('Generate dag by yaml')
def test_generate_dag_yaml():
    dag = generate_dag(dag_dir=f'{settings.AIRFLOW_HOME}/tests_data/dag_generator/YAML/dags/dummy',
                       dag_id='test_yaml_dummy')
    assert dag.task_ids == ['df_first', 'df_second', 'df_third', 'append_all']
    assert dag.result_type == 'pickle'
    assert dag.tags == ['dummy', 'yaml']


@allure.feature('Dag Generator')
@allure.story('Generate dag by yaml')
def test_generate_dag_yaml_callback():
    dag = generate_dag(dag_dir=f'{settings.AIRFLOW_HOME}/tests_data/dag_generator/YAML/dags/test1')
    assert dag.task_ids == ['get_df', 'test']


@allure.feature('Dag Generator')
@allure.story('Generate dag by yaml')
def test_generate_dag_yaml_sql_file():
    with allure.step('Generate dag'):
        dag = generate_dag(dag_dir=f'{settings.AIRFLOW_HOME}/tests_data/dag_generator/YAML/dags/test_sql_file')
        task = dag.tasks[0]
        assert task.sql == '/code/sql.sql'

    with allure.step('Check result'):
        task_instance = TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_dt
        context = task_instance.get_template_context()
        dag.get_work(dag.work_conn_id).create(context)
        task.render_template_fields(context=context)

        res1 = run_and_read(task, context)
        assert res1 is not None
        assert task.sql == "SELECT 1 as value, TO_CHAR(10, 'l99999D99') as currency"
        dag.clear_all_works(context)


@allure.feature('Dag Generator')
@allure.story('Generate dag by lambda')
def test_generate_dag_lambda():
    dag = generate_dag(
        dag_dir=f'{settings.AIRFLOW_HOME}/tests_data/dag_generator/YAML/dags/test_pandas_transform_lambda')
    assert dag.task_ids == ['get_df', 'test']


@allure.feature('Dag Generator')
@allure.story('Generate dag with camelcase dag_id')
def test_generate_dag_camelcase():
    dag = generate_dag(dag_dir=f'{settings.AIRFLOW_HOME}/tests_data/dag_generator/YAML/dags/dummy_CamelCase')
    assert dag is None
