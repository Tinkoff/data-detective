import uuid

import allure
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID, TASK_ID_KEY
from data_detective_airflow.operators import DBDump, PythonDump, PgSCD1, LoadingMethod
from data_detective_airflow.test_utilities import create_or_get_dagrun, run_task, assert_frame_equal, get_template_context
from tests_data.operators.sinks.pg_scd1_dataset import dataset

cases = [
    'empty',
    'insert_only',
    'delete_insert',
    'deleted_flg_column',
    'process_deletions',
    'process_existing_records'
]

chunk_row_numbers = [2, 100, None]


@allure.feature('Sinks')
@allure.story('SCD1 PG')
@allure.step('1 key, upload mode')
@pytest.mark.parametrize('case', cases)
@pytest.mark.parametrize('loading_method', ['D/I', 'U/I'])
@pytest.mark.parametrize('chunk_row_number', chunk_row_numbers)
def test_scd1_1key(test_dag, case, loading_method, chunk_row_number):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    hook.run("drop table if exists test_scd1")
    hook.run("create table test_scd1 (id integer, c1 varchar(20), "
             "c2 varchar(20), processed_dttm timestamp default now())")
    dataset['target'].to_sql('test_scd1', con=hook.get_uri(), if_exists='append', index=False)

    task_uuid = uuid.uuid1()
    source_task = PythonDump(task_id=f'source_pg_scd1_1key__{task_uuid}',
                             python_callable=lambda _context: dataset[f"{case}_source"],
                             dag=test_dag)

    task_params = {
        TASK_ID_KEY: f'test_pg_scd1_1key__{task_uuid}',
        'source': [source_task.task_id],
        'conn_id': PG_CONN_ID,
        'table_name': 'test_scd1',
        'key': 'id',
        'loading_method': loading_method,
        'chunk_row_number': chunk_row_number,
        'dag': test_dag}
    if case == 'deleted_flg_column':
        task_params.update({'deleted_flg_column': 'del_flg'})
    if case == 'process_deletions':
        task_params.update({'process_deletions': True})
    if case == 'process_existing_records':
        task_params.update({'process_existing_records': True})

    task = PgSCD1(**task_params)

    create_or_get_dagrun(test_dag, source_task)

    run_task(task=source_task, context=get_template_context(source_task))

    context = get_template_context(task)
    if case == 'process_deletions' \
            and chunk_row_number \
            and chunk_row_number < len(dataset[f"{case}_source"].index) \
            and loading_method == LoadingMethod.Update_Insert:
        with pytest.raises(RuntimeError):
            run_task(task=task, context=context)
        return
    run_task(task=task, context=context)

    expected = dataset[f"{case}_expected"]
    actual = task.read_result(context)

    if case == 'delete_insert':
        assert actual['processed_dttm'].isnull().sum() == 0
    del actual['processed_dttm']

    assert_frame_equal(expected, actual)


@allure.feature('Sinks')
@allure.story('SCD1 PG')
@allure.step('2 keys')
@pytest.mark.parametrize('case', cases)
@pytest.mark.parametrize('loading_method', ['D/I', 'U/I'])
def test_scd1_2keys(test_dag, case, loading_method):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    dataset['target'].to_sql('test_scd1', con=hook.get_uri(), if_exists='replace', index=False)
    dataset[f"{case}_source"].to_sql('test_scd1_source',
                                     con=hook.get_uri(), if_exists='replace', index=False)

    task_uuid = uuid.uuid1()
    source_task = DBDump(task_id=f'source_pg_scd1_2keys__{task_uuid}', conn_id=PG_CONN_ID,
                         sql="SELECT * FROM test_scd1_source", dag=test_dag)

    task_params = {TASK_ID_KEY: f'test_pg_scd1_2keys__{task_uuid}',
                   'source': [source_task.task_id],
                   'conn_id': PG_CONN_ID,
                   'table_name': 'test_scd1',
                   'key': ['id', 'c1'],
                   'loading_method': loading_method,
                   'dag': test_dag}
    if case == 'deleted_flg_column':
        task_params.update({'deleted_flg_column': 'del_flg'})
    if case == 'process_deletions':
        task_params.update({'process_deletions': True})
    if case == 'process_existing_records':
        task_params.update({'process_existing_records': True})

    task = PgSCD1(**task_params)

    create_or_get_dagrun(test_dag, source_task)

    run_task(task=source_task, context=get_template_context(source_task))
    context = get_template_context(task)
    run_task(task=task, context=context)

    expected = dataset[f"{case}_expected"]
    actual = task.read_result(context)

    assert_frame_equal(expected, actual)
