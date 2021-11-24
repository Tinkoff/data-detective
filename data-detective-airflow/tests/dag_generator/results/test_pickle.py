import allure
import pytest
from pandas import DataFrame

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.operators.extractors import DBDump
from data_detective_airflow.dag_generator import ResultType, WorkType


@allure.feature('Results')
@allure.story('Pickle')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True, ids=['pickle-result-write'])
def test_write(test_dag, context):
    task = DBDump(sql=f"select 1 as id",
                  task_id="test_pg_query", conn_id=PG_CONN_ID, dag=test_dag)

    work = test_dag.get_work(work_type=test_dag.work_type)
    work.create(context)
    res = test_dag.get_result(operator=task,
                              result_name=task.task_id,
                              work_type=test_dag.work_type,
                              result_type=test_dag.result_type)
    res.write(DataFrame([[123]]), context)
    result = work.get_path(context) / f'{res.name}.p'
    assert work.exists(result.as_posix())
    assert work.is_file(result.as_posix())
    test_dag.clear_all_works(context)


@allure.feature('Results')
@allure.story('Pickle')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True, ids=['pickle-result-read'])
def test_read(test_dag, dummy_task, context):
    work = test_dag.get_work(work_type=test_dag.work_type)
    work.create(context)
    res = test_dag.get_result(operator=dummy_task,
                              result_name=dummy_task.task_id,
                              work_type=test_dag.work_type,
                              result_type=test_dag.result_type)
    res.write(DataFrame([[123]], columns=['id']), context)
    df = res.read(context)
    assert df.shape == (1, 1)
    assert df['id'].to_list() == [123]
    test_dag.clear_all_works(context)
