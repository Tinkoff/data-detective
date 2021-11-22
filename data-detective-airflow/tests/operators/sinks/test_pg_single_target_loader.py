import allure
import pytest
from pandas import DataFrame

from data_detective_airflow.operators import PythonDump, PgSingleTargetLoader,\
    filter_for_entity, filter_for_relation, filter_for_breadcrumb
from data_detective_airflow.test_utilities.test_helper import run_task, assert_frame_equal
from data_detective_airflow.constants import PG_CONN_ID
from tests_data.operators.sinks.pg_single_table_loader_dataset import dataset, setup_tables, setup_tables_empty

targets = [
    {'table_name': 'dds.entity', 'key': ['urn'], 'filter_callable': filter_for_entity},
    {'table_name': 'dds.relation', 'key': ['source', 'destination', 'attribute'],
     'filter_callable': filter_for_relation},
    {'table_name': 'tuning.breadcrumb', 'key': ['urn'], 'filter_callable': filter_for_breadcrumb}
]

tracking_deletions = [True, False]

setups = ['setup_tables_empty', 'setup_tables']


@allure.feature('Operators')
@allure.story('PG Single Table Loader')
@pytest.mark.parametrize('deleted_flg', tracking_deletions)
@pytest.mark.parametrize('target', targets)
@pytest.mark.parametrize('setup', setups)
def test_pg_single_table_loader_main(test_dag, context, target, deleted_flg, setup, request):
    request.getfixturevalue(setup)
    test_dag.clear()
    source = dataset[f"source_{target['table_name']}"]
    source_task = PythonDump(task_id='source_task',
                             python_callable=lambda x: source,
                             dag=test_dag)
    task_params = {
        'conn_id': PG_CONN_ID,
        'task_id': 'test_task',
        'source': ['source_task'],
        'table_name': target['table_name'],
        'key': target['key'],
        'dag': test_dag,
        'deleted_flg': deleted_flg,
        'filter_callable': target['filter_callable'],
    }

    task = PgSingleTargetLoader(**task_params)

    run_task(task=source_task, context=context)
    run_task(task=task, context=context)

    trg = task.read_result(context)
    del trg['processed_dttm']

    expected_nm = f"expected_{target['table_name']}_{deleted_flg}_{'empty' if 'empty' in setup else 'not_empty'}"
    assert_frame_equal(dataset[expected_nm], trg, debug=True)

    test_dag.clear_all_works(context)


@allure.feature('Operators')
@allure.story('PG Single Table Loader')
@pytest.mark.parametrize('deleted_flg', tracking_deletions)
@pytest.mark.parametrize('target', targets)
@pytest.mark.parametrize('setup', setups)
def test_pg_single_table_loader_empty_source(test_dag, context, target, deleted_flg, setup, request):
    request.getfixturevalue(setup)
    test_dag.clear()
    source = DataFrame(columns=dataset[f"source_{target['table_name']}"].columns)
    source_task = PythonDump(task_id='source_task',
                             python_callable=lambda _context: source,
                             dag=test_dag)
    task_params = {
        'conn_id': PG_CONN_ID,
        'task_id': 'test_task',
        'source': ['source_task'],
        'table_name': target['table_name'],
        'key': target['key'],
        'dag': test_dag,
        'deleted_flg': deleted_flg,
        'filter_callable': target['filter_callable'],
    }

    task = PgSingleTargetLoader(**task_params)

    run_task(task=source_task, context=context)
    run_task(task=task, context=context)

    test_dag.clear_all_works(context)
