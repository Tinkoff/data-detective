import logging
from contextlib import closing
from datetime import datetime

import allure
import pytest

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.taskinstance import TaskInstance

from mg_airflow.constants import WORK_PG_SCHEMA_PREFIX, PG_CONN_ID
from mg_airflow.dag_generator import ResultType, WorkType
from mg_airflow.operators.transformers.pg_sql import PgSQL
from mg_airflow.test_utilities.generate_df import generate_single_dataframe, fill_table_from_dataframe
from tests_data.operators.transformers import pg_sql_queries as pg_test_data

TEST_SCHEMA = f'{WORK_PG_SCHEMA_PREFIX}_test'
TEST_TABLE = 'test_table'


@pytest.fixture
def setup_sources(test_dag):
    """Фикстура с подготовкой данных для тестов в данном модуле"""
    logging.info('TEST: Init source for tasks:')
    hook = PostgresHook(test_dag.conn_id)
    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            logging.info('TEST: Init schema')
            sql = f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA};"
            logging.info(sql)
            cur.execute(sql)

            logging.info('TEST: Create a source and fill it with data')
            sql = pg_test_data.sql_test_table.format(TEST_SCHEMA, TEST_TABLE)
            logging.info(sql)
            cur.execute(sql)
    yield
    sql = f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE;"
    logging.info(sql)
    hook.run([sql])


@allure.feature('Transformers')
@allure.story('PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql(test_dag, setup_sources):
    with allure.step('Init'):
        test_dag.clear()
        hook = PostgresHook(test_dag.conn_id)

    with closing(hook.get_conn()) as conn:
        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE}',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          dag=test_dag)

            task2 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE} where id < 4',
                          source=None, target='test_view_pg', obj_type='view',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_view',
                          dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

            logging.info('TEST: Task1 execute:')
            task1.post_execute(context, result=task1.execute(context))
            logging.info('TEST: Task2 execute:')
            task2.post_execute(context, result=task2.execute(context))

        with closing(conn.cursor()) as cur:
            with allure.step('Check the result of task1'):
                sql = f"SELECT COUNT(*)" \
                      f"FROM {task1.result.work.get_path(context)}.{task1.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result1 = cur.fetchone()
                assert query_result1[0] == 4

            with allure.step('Check the result of task2'):
                sql = f"SELECT COUNT(*)" \
                      f"FROM {task2.result.work.get_path(context)}.{task2.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result2 = cur.fetchone()
                assert query_result2[0] == 3

    test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('PG with the view source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql_with_view_src(test_dag, setup_sources):
    with allure.step('Init'):
        test_dag.clear()
        view_even = "test_view_even"
        view_odd = "test_view_odd"
        hook = PostgresHook(test_dag.conn_id)

    with closing(hook.get_conn()) as conn:
        with allure.step('Recreate views'):
            sql = f"CREATE OR REPLACE VIEW {TEST_SCHEMA}.{view_even} AS " \
                  f"SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE} " \
                  f"WHERE (id % 2) = 0;"
            logging.info(sql)
            hook.run(sql)

            sql = f"CREATE OR REPLACE VIEW {TEST_SCHEMA}.{view_odd} AS " \
                  f"SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE} " \
                  f"WHERE (id % 2) <> 0;"
            logging.info(sql)
            hook.run(sql)

        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE}',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          dag=test_dag)

            task2 = PgSQL(sql=f"SELECT id, data "
                              f"FROM {TEST_SCHEMA}.{view_even} "
                              f"UNION ALL "
                              f"SELECT id, data "
                              f"FROM {TEST_SCHEMA}.{view_odd} ",
                          source=None, target='test_table_res', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_res',
                          dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

            logging.info('TEST: Task1 execute:')
            task1.post_execute(context, result=task1.execute(context))
            logging.info('TEST: Task2 execute:')
            task2.post_execute(context, result=task2.execute(context))

        with closing(conn.cursor()) as cur:
            with allure.step('Compare the results of task1 and task2'):
                sql = f"SELECT COUNT(*)" \
                      f"FROM {task1.result.work.get_path(context)}.{task1.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result1 = cur.fetchone()

                sql = f"SELECT COUNT(*)" \
                      f"FROM {task2.result.work.get_path(context)}.{task2.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result2 = cur.fetchone()
                assert len(query_result1) > 0 and \
                       len(query_result2) > 0 and \
                       query_result1[0] == query_result2[0]

    test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('PG with duplicates in the data')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql_with_duplicates_data(test_dag, setup_sources):
    with allure.step('Init'):
        test_dag.clear()
        hook = PostgresHook(test_dag.conn_id)

    with allure.step('Add data'):
        query = f"INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (id, data) " \
                f"VALUES (2, 'two'), (3, 'three'); " \
                f"COMMIT;"
        hook.run(query)

    with closing(hook.get_conn()) as conn:
        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE}',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

            logging.info('TEST: Task1 execute:')
            task1.post_execute(context, result=task1.execute(context))

        with closing(conn.cursor()) as cur:
            with allure.step('Check result contains all rows'):
                sql = f"SELECT COUNT(*) " \
                      f"FROM {task1.result.work.get_path(context)}.{task1.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result1 = cur.fetchone()
                assert len(query_result1) > 0 and \
                       query_result1[0] == 6

            with allure.step('Check the number of unique rows'):
                sql = f"SELECT COUNT (DISTINCT id) " \
                      f"FROM {task1.result.work.get_path(context)}.{task1.result.name} ;"
                logging.info(sql)
                cur.execute(sql)
                query_result1 = cur.fetchone()
                assert len(query_result1) > 0 and \
                       query_result1[0] == 4

    test_dag.clear_all_works(context)


@pytest.mark.skip('FIXME')
@allure.feature('Transformers')
@allure.story('PG with a heavy query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql_with_heavy_query(test_dag):
    with allure.step('Init'):
        test_dag.clear()
        hook = PostgresHook(test_dag.conn_id)
        tab1 = "first_test_tab"
        tab2 = "second_test_tab"

    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            with allure.step('Create schema'):
                sql = f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA};"
                logging.info(sql)
                cur.execute(sql)

            with allure.step('Recreate tables'):
                sql = f"DROP TABLE IF EXISTS {TEST_SCHEMA}.{tab1} CASCADE;"
                logging.info(sql)
                cur.execute(sql)

                sql = f"CREATE TABLE {TEST_SCHEMA}.{tab1} (id int, data text);"
                logging.info(sql)
                cur.execute(sql)

                sql = f"DROP TABLE IF EXISTS {TEST_SCHEMA}.{tab2} CASCADE;"
                logging.info(sql)
                cur.execute(sql)

                sql = f"CREATE TABLE {TEST_SCHEMA}.{tab2} (id int, value text);"
                logging.info(sql)
                cur.execute(sql)

            with allure.step('Generate data and insert into tables'):
                rec_count = 100

                columns1 = {'id': 'int', 'data': 'str'}
                df1 = generate_single_dataframe(columns=columns1,
                                                records_count=rec_count)
                columns2 = {'id': 'int', 'value': 'str'}
                df2 = generate_single_dataframe(columns=columns2,
                                                records_count=rec_count)

                logging.info("Fill in the tables")
                status1 = fill_table_from_dataframe(conn=conn, dframe=df1, schema=TEST_SCHEMA,
                                                    table=tab1)
                status2 = fill_table_from_dataframe(conn=conn, dframe=df2, schema=TEST_SCHEMA,
                                                    table=tab2)
                assert status1 and status2

        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT t1.id, t1.data, t2.id as id2, t2.value '
                              f'FROM {TEST_SCHEMA}.{tab1} as t1 '
                              f'CROSS JOIN {TEST_SCHEMA}.{tab2} as t2 ',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          analyze='id', dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

            logging.info('TEST: Task1 execute:')
            task1.post_execute(context, result=task1.execute(context))

        with closing(conn.cursor()) as cur:
            with allure.step('Check the result'):
                sql = f"SELECT COUNT(*) " \
                      f"FROM {task1.result.work.get_path(context)}.{task1.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result = cur.fetchone()
                assert len(query_result) > 0 and \
                       query_result[0] == rec_count * rec_count

            sql = f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE;"
            logging.info(sql)
            cur.execute(sql)

    test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('PG with an empty result')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql_with_an_empty_result(test_dag, setup_sources):
    with allure.step('Init'):
        test_dag.clear()
        hook = PostgresHook(test_dag.conn_id)

    with closing(hook.get_conn()) as conn:
        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id > 5',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)

            logging.info('TEST: Task1 execute:')
            task1.post_execute(context, result=task1.execute(context))

        with closing(conn.cursor()) as cur:
            with allure.step('Check result'):
                sql = f"SELECT COUNT(*) " \
                      f"FROM {task1.result.work.get_path(context)}.{task1.result.name};"
                logging.info(sql)
                cur.execute(sql)
                query_result1 = cur.fetchone()
                assert len(query_result1) > 0 and \
                       query_result1[0] == 0

    test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('PG with an invalid query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql_with_non_existent_field(test_dag, setup_sources):
    with allure.step('Init'):
        test_dag.clear()
        hook = PostgresHook(test_dag.conn_id)

    with closing(hook.get_conn()) as conn:
        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.{TEST_TABLE} '
                              f'WHERE group_id < 5',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks with exception'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)
            try:
                err = None
                logging.info('TEST: Task1 execute:')
                task1.post_execute(context, result=task1.execute(context))
                # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
                state = 0
            except Exception as exc:
                err = exc
                state = 1 if f'{err}'.startswith('column "group_id" does not exist') else -1
            if err:
                logging.info(f"Task1 failed with an error: {err}.")
            assert state == 1

    test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('PG with an invalid query')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_pg_sql_with_non_existent_source(test_dag, setup_sources):
    with allure.step('Init'):
        test_dag.clear()
        hook = PostgresHook(test_dag.conn_id)

    with closing(hook.get_conn()) as conn:
        with allure.step('Create tasks and context'):
            task1 = PgSQL(sql=f'SELECT * FROM {TEST_SCHEMA}.test_tab',
                          source=None, target='test_table_pg', obj_type='table',
                          conn_id=test_dag.conn_id, task_id='test_pg_sql_table',
                          dag=test_dag)

            ti = TaskInstance(task=task1, execution_date=datetime.now())
            context = ti.get_template_context()

        with allure.step('Run tasks with exception'):
            logging.info('TEST: Create work:')
            test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)
            try:
                err = None
                logging.info('TEST: Task1 execute:')
                task1.post_execute(context, result=task1.execute(context))
                # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
                state = 0
            except Exception as exc:
                err = exc
                state = 1 if f'{err}'.startswith(
                    'relation "wrk_test.test_tab" does not exist') else -1
            if err:
                logging.info(f"Task1 failed with an error: {err}.")
            assert state == 1

    test_dag.clear_all_works(context)
