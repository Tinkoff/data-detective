import pytest

import allure
from data_detective_airflow.constants import PG_CONN_ID, S3_CONN_ID
from data_detective_airflow.dag_generator.results import PgResult, PickleResult
from data_detective_airflow.dag_generator import ResultType, WorkType
from data_detective_airflow.test_utilities import create_or_get_dagrun, get_template_context

@allure.feature('Dag results')
@allure.story('Create Pickle')
def test_dag_file_create_result(test_dag, dummy_task):
    create_or_get_dagrun(test_dag, dummy_task)
    context = get_template_context(dummy_task)
    dag_result = test_dag.get_result(operator=None,
                                     result_name='test',
                                     context=context,
                                     result_type=test_dag.result_type,
                                     work_type=test_dag.work_type)
    assert isinstance(dag_result, PickleResult)
    test_dag.clear_all_works(context)


@allure.feature('Dag results')
@allure.story('Create S3')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_S3.value,
                           S3_CONN_ID)],
                         indirect=True)
def test_dag_s3_create_result(test_dag, dummy_task):
    create_or_get_dagrun(test_dag, dummy_task)
    context = get_template_context(dummy_task)
    dag_result = test_dag.get_result(operator=None, result_name='test', context=context,
                                     result_type=test_dag.result_type,
                                     work_type=test_dag.work_type)
    assert isinstance(dag_result, PickleResult)
    test_dag.clear_all_works(context)


@allure.feature('Dag results')
@allure.story('Create PG')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PG.value,
                           WorkType.WORK_PG.value,
                           PG_CONN_ID)],
                         indirect=True)
def test_dag_pg_create_result(test_dag, dummy_task):
    create_or_get_dagrun(test_dag, dummy_task)
    context = get_template_context(dummy_task)
    dag_result = test_dag.get_result(operator=None, result_name='test', context=context,
                                     result_type=test_dag.result_type,
                                     work_type=test_dag.work_type)
    assert isinstance(dag_result, PgResult)
    test_dag.clear_all_works(context)
