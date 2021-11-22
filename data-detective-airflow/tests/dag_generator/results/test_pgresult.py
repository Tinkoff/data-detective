import allure
import pytest
from airflow.hooks.base import BaseHook
from pandas import DataFrame

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import ResultType, WorkType


@allure.feature('Results')
@allure.story('PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-write'])
def test_write(test_dag, dummy_task, context):
    work = test_dag.get_work(test_dag.work_type, test_dag.conn_id)
    work.create(context)
    res = test_dag.get_result(operator=dummy_task,
                              result_name='test',
                              result_type=test_dag.result_type,
                              work_type=test_dag.work_type,
                              work_conn_id=test_dag.work_conn_id)
    res.write(DataFrame([[123]], columns=['id']), context)
    res = BaseHook.get_connection(PG_CONN_ID).get_hook().get_pandas_df(
        f"SELECT table_name FROM information_schema.tables"
        f" WHERE table_schema = '{work.get_path(context)}'"
        f" AND table_name = '{res.name}';"
    )
    assert res['table_name'].to_list() == ['test']
    test_dag.clear_all_works(context)


@allure.feature('Results')
@allure.story('PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True, ids=['pg-result-read'])
def test_read(test_dag, dummy_task, context):
    test_dag.get_work(test_dag.work_type, test_dag.conn_id).create(context)
    res = test_dag.get_result(operator=dummy_task, result_name='test',
                              result_type=test_dag.result_type,
                              work_type=test_dag.work_type,
                              work_conn_id=test_dag.work_conn_id)
    res.write(DataFrame([[123]], columns=['id']), context)
    df = res.read(context)
    assert df.shape == (1, 1)
    assert df['id'].to_list() == [123]
    test_dag.clear_all_works(context)
