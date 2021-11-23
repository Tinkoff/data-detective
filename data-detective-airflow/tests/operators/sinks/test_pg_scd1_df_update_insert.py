import allure
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.operators import PythonDump, PgSCD1DFUpdateInsert
from data_detective_airflow.test_utilities import run_task, assert_frame_equal

from tests_data.operators.sinks.pg_scd1_df_update_insert_dataset import dataset

cases = [
    'empty',
    'not_empty'
]

chunk_row_numbers = [2, 100, None]


@allure.feature('Sinks')
@allure.story('SCD1 PG')
@allure.step('1 key, upload mode')
@pytest.mark.parametrize('case', cases)
@pytest.mark.parametrize('chunk_row_number', chunk_row_numbers)
def test_scd1_df(test_dag, context, case, chunk_row_number):
    test_dag.clear()
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    hook.run("drop table if exists test_scd1")
    hook.run("create table test_scd1 (id integer, c1 varchar(20), "
             "c2 varchar(20), processed_dttm timestamp default now())")
    dataset['target'].to_sql('test_scd1', con=hook.get_uri(), if_exists='append', index=False)

    source_task = PythonDump(task_id='source_task',
                             python_callable=lambda _context: dataset[f"{case}_source"],
                             dag=test_dag)

    task_params = {
        'task_id': 'test_task',
        'source': ['source_task'],
        'conn_id': PG_CONN_ID,
        'table_name': 'test_scd1',
        'key': 'id',
        'diff_change_oper': 'diff',
        'chunk_row_number': chunk_row_number,
        'dag': test_dag}

    task = PgSCD1DFUpdateInsert(**task_params)

    run_task(task=source_task, context=context)
    run_task(task=task, context=context)

    expected = dataset[f"{case}_expected"]
    actual = task.read_result(context)

    assert actual['processed_dttm'].isnull().sum() == 0
    del actual['processed_dttm']

    assert_frame_equal(expected, actual)

    test_dag.clear_all_works(context)
