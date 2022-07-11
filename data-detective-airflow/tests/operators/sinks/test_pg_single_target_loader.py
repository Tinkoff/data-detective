import uuid

import allure
import pytest
from pandas import DataFrame

from data_detective_airflow.operators import PythonDump, PgSingleTargetLoader,\
    filter_for_entity, filter_for_relation, filter_for_breadcrumb
from data_detective_airflow.test_utilities import run_task, assert_frame_equal, get_template_context
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
def test_pg_single_table_loader_main(test_dag, target, deleted_flg, setup, request):
    request.getfixturevalue(setup)

    source = dataset[f"source_{target['table_name']}"]
    task_uuid = uuid.uuid1()
    source_task = PythonDump(task_id=f'source_pg_stl_main__{task_uuid}',
                             python_callable=lambda x: source,
                             dag=test_dag)
    task_params = {
        'conn_id': PG_CONN_ID,
        'task_id': f'test_pg_stl__{task_uuid}',
        'source': [source_task.task_id],
        'table_name': target['table_name'],
        'key': target['key'],
        'dag': test_dag,
        'deleted_flg': deleted_flg,
        'filter_callable': target['filter_callable'],
    }

    task = PgSingleTargetLoader(**task_params)

    source_context = get_template_context(source_task)
    run_task(task=source_task, context=source_context)
    context = get_template_context(task)
    run_task(task=task, context=context)

    actual = task.read_result(context)

    expected_nm = f"expected_{target['table_name']}_{deleted_flg}_{'empty' if 'empty' in setup else 'not_empty'}"
    expected = dataset[expected_nm]
    actual = actual.drop(labels=set(actual.columns) - set(expected.columns), axis=1)
    assert_frame_equal(expected, actual, debug=True)


@allure.feature('Operators')
@allure.story('PG Single Table Loader')
@pytest.mark.parametrize('deleted_flg', tracking_deletions)
@pytest.mark.parametrize('target', targets)
@pytest.mark.parametrize('setup', setups)
def test_pg_single_table_loader_empty_source(test_dag, target, deleted_flg, setup, request):
    request.getfixturevalue(setup)
    source = DataFrame(columns=dataset[f"source_{target['table_name']}"].columns)
    task_uuid = uuid.uuid1()
    source_task = PythonDump(task_id=f'test_pg_stl_empty__{task_uuid}',
                             python_callable=lambda _context: source,
                             dag=test_dag)
    task_params = {
        'conn_id': PG_CONN_ID,
        'task_id': f'test_task__{task_uuid}',
        'source': [source_task.task_id],
        'table_name': target['table_name'],
        'key': target['key'],
        'dag': test_dag,
        'deleted_flg': deleted_flg,
        'filter_callable': target['filter_callable'],
    }

    task = PgSingleTargetLoader(**task_params)

    source_context = get_template_context(source_task)
    run_task(task=source_task, context=source_context)
    context = get_template_context(task)
    run_task(task=task, context=context)
