import logging
from contextlib import closing

import allure
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import ResultType, WorkType


def is_work_schema_exists(work):
    res = work.hook.get_pandas_df(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{work.schema}';"
    )
    return res['schema_name'].to_list() == ['work_airflow_test_dag']


def get_work_schema(work, context):
    hook = PostgresHook(work.conn_id)
    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            sql = f"select nspname from pg_namespace where nspname =" \
                  f" '{work.get_path(context)}';"
            logging.info(sql)
            cur.execute(sql)
            query_result = cur.fetchone()
            logging.info(f'get_work_schema = {query_result}')
            if query_result is not None:
                return query_result[0]
    return None


@allure.feature('Works')
@allure.story('Clean work in PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True)
def test_no_error_on_double_clean(test_dag, context):
    test_dag.get_work(work_type=test_dag.work_type,
                      work_conn_id=test_dag.conn_id).create(context)
    test_dag.clear_all_works(context)
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Create work in PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True)
def test_create(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type,
                             work_conn_id=test_dag.conn_id)
    work.create(context)
    assert get_work_schema(work, context) == work.get_path(context)
    test_dag.clear_all_works(context)


@allure.feature('Works')
@allure.story('Clean work in PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True)
def test_clean(test_dag, context):
    work = test_dag.get_work(work_type=test_dag.work_type,
                             work_conn_id=test_dag.conn_id)
    work.create(context)
    test_dag.clear_all_works(context)
    assert get_work_schema(work, context) is None
