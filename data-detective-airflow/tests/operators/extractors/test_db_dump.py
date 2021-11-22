from datetime import datetime

import logging
import pytest
import allure
from airflow import settings
from airflow.models.taskinstance import TaskInstance

from mg_airflow.constants import PG_CONN_ID
from mg_airflow.operators import DBDump
from mg_airflow.dag_generator import ResultType, WorkType
from mg_airflow.test_utilities import run_task
from tests_data.fixtures.PG.pg_fixtures import setup_sources, invalid_pg_data


@allure.feature('Extractors')
@allure.story('DB dump with pg query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True, ids=['dbdump-pg-query'])
def test_pg_query(test_dag, context):
    test_dag.clear()

    task = DBDump(sql="select * from dummy_test_pg_dump",
                  conn_id=PG_CONN_ID, task_id="test_pg_query", dag=test_dag)
    task.render_template_fields(context=context)

    ti = TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
    context = ti.get_template_context()
    test_dag.get_work(test_dag.work_conn_id).create(context)
    run_task(task=task, context=context)

    assert task.result is not None
    assert task.result.read(context).shape == (5, 2)

    test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('DB dump with pg file')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True,
                         ids=['dbdump-pg-file'])
def test_pg_file(test_dag, context):
    test_dag.clear()
    sql_file = f'{settings.AIRFLOW_HOME}/tests_data/operators/extractors/test_sql_query.sql'

    task = DBDump(conn_id=PG_CONN_ID, sql=sql_file,
                  work_conn_id=test_dag.work_conn_id, result_type=test_dag.result_type,
                  task_id="test_pg_query", dag=test_dag)
    task.render_template_fields(context=context)

    ti = TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
    test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

    run_task(task=task, context=context)

    assert task.result is not None
    assert task.result.read(context).shape == (5, 2)

    test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('DB dump with an invalid query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['dbdump-pg-invalid-query'])
def test_gp_sql_with_invalid_query(test_dag, context, setup_sources, invalid_pg_data):
    with allure.step('Create task and context'):
        test_dag.clear()

        task = DBDump(conn_id=PG_CONN_ID,
                      sql=invalid_pg_data.sql,
                      work_conn_id=test_dag.work_conn_id,
                      result_type=test_dag.result_type,
                      task_id="test_pg_query", dag=test_dag)
        task.render_template_fields(context=context)

        ti = TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
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

    test_dag.clear_all_works(context)


@allure.feature('Extractors')
@allure.story('DB dump with pg file')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True,
                         ids=['dbdump-pg-file'])
def test_pg_with_non_existent_query_file(test_dag, context):
    with allure.step('Create task'):
        test_dag.clear()
        query_path = f"{settings.AIRFLOW_HOME}/tests_data/operators/extractors/sql_query.sql"

        task = DBDump(conn_id=PG_CONN_ID, sql=query_path,
                      work_conn_id=test_dag.work_conn_id, result_type=test_dag.result_type,
                      task_id="test_pg_query", dag=test_dag)

    with allure.step('Run tasks with exception'):
        try:
            err = None
            logging.info('TEST: Task execute:')

            task.render_template_fields(context=context)
            ti = TaskInstance(task=task, execution_date=datetime.now())  # test_dag.start_date
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

    test_dag.clear_all_works(context)
