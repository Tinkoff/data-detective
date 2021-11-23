import logging
from collections import namedtuple
from contextlib import closing

import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import WORK_PG_SCHEMA_PREFIX
from tests_data.fixtures.PG import postgres_sql_queries as pg_test_data


TEST_SCHEMA = f'{WORK_PG_SCHEMA_PREFIX}_test'
TEST_TABLE = 'test_table'


@pytest.fixture
def setup_sources(test_dag):
    """Фикстура с подготовкой данных в GP для тестов"""
    logging.info('TEST: Init source for tasks:')
    hook = PostgresHook(test_dag.conn_id)
    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            logging.info('TEST: Create a source and fill it with data')
            sql = pg_test_data.sql_test_table.format(TEST_SCHEMA, TEST_TABLE)
            logging.info(sql)
            cur.execute(sql)
    yield
    sql = pg_test_data.sql_drop_schema.format(TEST_SCHEMA)
    logging.info(sql)
    hook.run(sql)


def __get_sql_and_mess_for_negative_pg_tests() -> set:
    """Метод для получения запросов и сообщений с ошибками для негативных GP-тестов
    :return множество с запросами и соответствующими сообщениями об ошибках
    """
    pg_test_param = namedtuple("pg_test_param", ["sql", "res_mes"])
    pg_params = set()

    pg_params.add(pg_test_param(pg_test_data.sql_with_non_existent_schema.format(TEST_TABLE),
                                'relation "test_test.test_table" does not exist'))

    pg_params.add(pg_test_param(pg_test_data.sql_with_non_existent_field.format(TEST_SCHEMA,
                                                                                TEST_TABLE),
                                'column "group_id" does not exist'))

    pg_params.add(pg_test_param(pg_test_data.sql_with_non_existent_source.format(TEST_SCHEMA),
                                'relation "{0}.test_tab" does not exist'.format(TEST_SCHEMA)))
    return pg_params


@pytest.fixture(params=__get_sql_and_mess_for_negative_pg_tests())
def invalid_pg_data(request):
    """Фикстура для негативных тестов в GP"""
    yield request.param

