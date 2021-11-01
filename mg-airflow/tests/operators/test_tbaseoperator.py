import time
import allure
import pytest
from airflow.utils.module_loading import import_string
from airflow.exceptions import AirflowTaskTimeout

from mg_airflow.dag_generator import ResultType, WorkType
from mg_airflow.constants import PG_CONN_ID


@allure.feature('Execution_timeout execution')
@allure.story('Exception')
@pytest.mark.parametrize(
    'clname,kwargs',
    [
        ('mg_airflow.operators.transformers.pg_sql.PgSQL',
         {
             'conn_id': PG_CONN_ID,
             'source': None,
             'work_type': 'pg',
             'work_conn_id': PG_CONN_ID,
             'result_type': 'pg',
             'sql': 'select 1 from pg_sleep(3)',
             'execution_timeout': 1
         }),
    ]
)
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True, ids=['base_operator'])
def test_timeout_exc_pass_through(test_dag, clname, kwargs):
    test_task = import_string(clname)(task_id='test', dag=test_dag, **kwargs)
    with pytest.raises(AirflowTaskTimeout):
        test_task.run()


@allure.feature('Execution_timeout execution')
@allure.story('Exception')
@pytest.mark.parametrize(
    'clname,kwargs',
    [
        ('mg_airflow.operators.extractors.python_dump.PythonDump',
         {
             'source': None,
             'execution_timeout': 1,
             'python_callable': lambda _ctx: time.sleep(3)
         }),
    ]
)
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True, ids=['base_operator'])
def test_timeout_exc_dataframe(test_dag, clname, kwargs):
    test_task = import_string(clname)(task_id='test', dag=test_dag, **kwargs)
    with pytest.raises(AirflowTaskTimeout):
        test_task.run()


@allure.feature('Terminate query of failed task PG')
@allure.story('Termination')
@pytest.mark.parametrize(
    'clname,kwargs',
    [
        ('mg_airflow.operators.transformers.pg_sql.PgSQL',
         {
             'conn_id': PG_CONN_ID,
             'source': None,
             'work_type': 'pg',
             'work_conn_id': PG_CONN_ID,
             'result_type': 'pg',
             'sql': 'select 1 from pg_sleep(3)',
             'execution_timeout': 1
         }),
    ]
)
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           None)],
                         indirect=True, ids=['base_operator'])
def test_terminate_failed_query_pg(test_dag, clname, kwargs):
    test_task = import_string(clname)(task_id='test', dag=test_dag, **kwargs)
    work = test_task.dag.get_work(test_task.work_type, test_task.work_conn_id)
    try:
        test_task.run()
    except AirflowTaskTimeout:
        cnt = work.execute("""
        SELECT count(*)
        FROM pg_stat_activity
        WHERE state = 'active'
        AND wait_event_type = 'Timeout';
        """, fetch='one')
        assert not cnt
