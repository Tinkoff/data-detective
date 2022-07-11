import logging
import pytest
import allure
from airflow import settings

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.operators import DBDump
from data_detective_airflow.dag_generator import ResultType, WorkType
from data_detective_airflow.test_utilities import run_task, get_template_context
from tests_data.fixtures.PG.pg_fixtures import setup_sources, invalid_pg_data


@allure.feature('Extractors')
@allure.story('DB dump with pg query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True, ids=['dbdump-pg-query'])
def test_pg_query(test_dag):
    task = DBDump(sql="select * from dummy_test_pg_dump",
                  conn_id=PG_CONN_ID, task_id="test_pg_query", dag=test_dag)
    context = get_template_context(task)
    run_task(task=task, context=context)

    assert task.result is not None
    assert task.result.read(context).shape == (5, 2)


@allure.feature('Extractors')
@allure.story('DB dump with pg file')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True,
                         ids=['dbdump-pg-file'])
def test_pg_file(test_dag):
    sql_file = f'{settings.AIRFLOW_HOME}/tests_data/operators/extractors/test_sql_query.sql'

    task = DBDump(conn_id=PG_CONN_ID, sql=sql_file,
                  work_conn_id=test_dag.work_conn_id, result_type=test_dag.result_type,
                  task_id="test_pg_file", dag=test_dag)

    context = get_template_context(task)
    test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)
    run_task(task=task, context=context)

    assert task.result is not None
    assert task.result.read(context).shape == (5, 2)


@allure.feature('Extractors')
@allure.story('DB dump with an invalid query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['dbdump-pg-invalid-query'])
def test_gp_sql_with_invalid_query(test_dag, setup_sources, invalid_pg_data):
    with allure.step('Create task and context'):

        task = DBDump(conn_id=PG_CONN_ID,
                      sql=invalid_pg_data.sql,
                      work_conn_id=test_dag.work_conn_id,
                      result_type=test_dag.result_type,
                      task_id="test_pg_query", dag=test_dag)
        context = get_template_context(task)
        test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

    with allure.step('Run tasks with exception'):
        try:
            err = None
            logging.info('TEST: Task execute:')
            run_task(task=task, context=context)
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except Exception as exc:
            err = str(exc)
            state = 1 if invalid_pg_data.res_mes in err else -1
        if err:
            logging.error(f"Task failed with an error: {err}.")
        assert state == 1


@allure.feature('Extractors')
@allure.story('DB dump with pg file')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True,
                         ids=['dbdump-pg-file'])
def test_pg_with_non_existent_query_file(test_dag):
    with allure.step('Create task'):
        query_path = f"{settings.AIRFLOW_HOME}/tests_data/operators/extractors/sql_query.sql"

        task = DBDump(conn_id=PG_CONN_ID, sql=query_path,
                      work_conn_id=test_dag.work_conn_id, result_type=test_dag.result_type,
                      task_id="pg_with_non_existent_query_file", dag=test_dag)

    with allure.step('Run tasks with exception'):
        try:
            err = None
            logging.info('TEST: Task execute:')

            context = get_template_context(task)
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

            run_task(task=task, context=context)
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except Exception as exc:
            err = exc
            state = 1 if err and query_path in f'{err}' else -1
        if err:
            logging.error(f"Task failed with an error: {err}.")
        assert state == 1
