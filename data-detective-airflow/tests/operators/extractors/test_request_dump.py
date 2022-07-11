from datetime import datetime
import allure
from pathlib import Path

import pytest
from airflow.exceptions import AirflowBadRequest
from airflow.models.taskinstance import TaskInstance
from numpy.testing import assert_array_equal
from pandas import DataFrame
from requests import Session

from data_detective_airflow.operators.extractors import RequestDump
from data_detective_airflow.dag_generator import TDag, ResultType, WorkType
from data_detective_airflow.test_utilities import run_task, get_template_context
from data_detective_airflow.constants import PG_CONN_ID
from tests_data.operators.extractors.request_dump_dataset import dataset, empty_dataset


@allure.feature('Extractors')
@allure.story('Request dump with an invalid request')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_bad_request(test_dag, mocker):
    mocker.patch.object(Session,
                        'get',
                        return_value=mocker.MagicMock(
                            content='',
                            ok=False)
                        )

    task = RequestDump(conn_id=PG_CONN_ID,
                       url='bad_url',
                       task_id="test_request_dump",
                       dag=test_dag)
    context = get_template_context(task)
    test_dag.get_work(test_dag.conn_id).create(context)

    with pytest.raises(AirflowBadRequest):
        assert run_task(task=task, context=context)


@allure.feature('Extractors')
@allure.story('Request dump')
@pytest.mark.parametrize('response_dataset',
                         [dataset, empty_dataset],
                         ids=['dataset', 'empty_dataset'])
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_normal_request(test_dag, mocker, context, response_dataset):
    with allure.step('Create a task and context'):
        mocker.patch.object(Session,
                            'get',
                            return_value=mocker.MagicMock(
                                content=response_dataset['request_dump']['response'],
                                ok=True)
                            )
        test_dag.clear()

        task = RequestDump(conn_id=PG_CONN_ID,
                           url='good_url',
                           task_id="test_request_dump",
                           dag=test_dag)

        test_dag.get_work(test_dag.conn_id).create(context)

    with allure.step('Run task and check result'):
        run_task(task=task, context=context)
        assert task.result is not None
        assert task.result.name is not None

        actual = task.result.read(context)
        expected = DataFrame({'response': response_dataset['request_dump']['response']},
                             index=[0])

        if actual.isna().any().any() or expected.isna().any().any():
            # Т.к. nan != nan, маскируем такие значения
            actual = actual.mask(actual.isna(), 0)
            expected = expected.mask(expected.isna(), 0)

        assert_array_equal(actual.values, expected.values)


@allure.feature('Extractors')
@allure.story('Request dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_params(test_dag, mocker, context):
    with allure.step('Create and run a task'):
        mocker.patch.object(Session,
                            'get',
                            return_value=mocker.MagicMock(
                                content=dataset['request_dump']['response'],
                                ok=True)
                            )

        params = DataFrame({
            'param': ('param1', 'param2')})

        task = RequestDump(conn_id=PG_CONN_ID,
                           url='good_url/{param}',
                           task_id="test_request_dump",
                           url_params=params,
                           dag=test_dag)

        test_dag.get_work(test_dag.conn_id).create(context)
        run_task(task=task, context=context)

        assert task.result is not None
        assert task.result.name is not None

    with allure.step('Check result'):
        result = f'{task.result.work.get_path(context)}/{task.result.name}.p'
        assert Path(result).exists()
        assert Path(result).is_file()

    with allure.step('Compare actual and expected values'):
        actual = task.result.read(context)
        expected = params
        expected['response'] = dataset['request_dump'].iloc[0]['response']

        assert_array_equal(actual.values, expected.values)


@allure.feature('Extractors')
@allure.story('Request dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_source(test_dag: TDag, mocker, context):
    with allure.step('Create and run a task'):
        mocker.patch.object(Session,
                            'get',
                            return_value=mocker.MagicMock(
                                content=dataset['request_dump']['response'],
                                ok=True)
                            )

        params = DataFrame({
            'param': ('param1', 'param2')
        })

        upstream_task = RequestDump(conn_id=PG_CONN_ID,
                                    url='good_url',
                                    task_id="upstream_task",
                                    dag=test_dag)
        run_task(upstream_task, context)
        upstream_task.result.read = mocker.MagicMock(return_value=params)

        main_task = RequestDump(conn_id=PG_CONN_ID,
                                url='good_url/{param}',
                                task_id="main_task",
                                source=['upstream_task'],
                                dag=test_dag)

        test_dag.get_work(test_dag.conn_id).create(context)
        run_task(task=main_task, context=context)

        assert main_task.result is not None
        assert main_task.result.name is not None

    with allure.step('Check result'):
        result = f'{main_task.result.work.get_path(context)}/{main_task.result.name}.p'
        assert Path(result).exists()
        assert Path(result).is_file()

    with allure.step('Compare actual and expected values'):
        actual = main_task.result.read(context)
        expected = params
        expected['response'] = dataset['request_dump'].iloc[0]['response']

        assert_array_equal(actual.values, expected.values)


@allure.feature('Extractors')
@allure.story('Request dump')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_request_with_sleep(test_dag: TDag, mocker, context):
    sleep_mock = mocker.patch('data_detective_airflow.operators.extractors.request_dump.sleep')
    mocker.patch.object(Session,
                        'get',
                        return_value=mocker.MagicMock(
                            content=dataset['request_dump']['response'],
                            ok=True)
                        )
    task = RequestDump(conn_id=PG_CONN_ID,
                       url='good_url',
                       task_id="test_request_dump",
                       wait_seconds=100,
                       dag=test_dag)

    test_dag.get_work(test_dag.conn_id).create(context)
    run_task(task=task, context=context)
    sleep_mock.assert_called_once()


@allure.feature('Extractors')
@allure.story('Request dump with an invalid request')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_source_with_an_unformed_result(test_dag: TDag, mocker, context):
    with allure.step('Init'):
        mocker.patch.object(Session,
                            'get',
                            return_value=mocker.MagicMock(
                                content=dataset['request_dump']['response'],
                                ok=True)
                            )

        params = DataFrame({
            'param': ('param1', 'param2')
        })

    with allure.step('Create a task "upstream_task1"'):
        upstream_task1 = RequestDump(conn_id=PG_CONN_ID,
                                     url='good_url1',
                                     task_id="upstream_task1",
                                     dag=test_dag)

    with allure.step('Create and run a task "upstream_task2"'):
        upstream_task2 = RequestDump(conn_id=PG_CONN_ID,
                                     url='good_url2',
                                     task_id="upstream_task2",
                                     dag=test_dag)
        run_task(upstream_task2, context)
        upstream_task2.result.read = mocker.MagicMock(return_value=params)

    with allure.step('Create a task "main_task"'):
        main_task = RequestDump(conn_id=PG_CONN_ID,
                                url='good_url/{param}',
                                task_id="main_task",
                                source=['upstream_task1', 'upstream_task2'],
                                dag=test_dag)
        test_dag.get_work(test_dag.conn_id).create(context)

    with allure.step('Run a task with exception'):
        try:
            err = None
            run_task(task=main_task, context=context)
            task_res = main_task.result.read(context)
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except FileNotFoundError:
            state = 1
        except Exception as exc:
            state = -1
        assert state == 1


@allure.feature('Extractors')
@allure.story('Request dump with an invalid request')
@pytest.mark.parametrize('time_params',
                         [(10, 5), (5, 10)],
                         ids=['waiting time < time to receive a response',
                              'waiting time >= time to receive a response'])
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_long_request(test_dag: TDag, mocker, context, time_params):
    with allure.step('Init'):
        test_dag.clear()
        timeout, wait_time = time_params
        if timeout < wait_time:
            content = dataset['request_dump']['response']
            scs = True
        else:
            content = ''
            scs = False
        sleep_mock = mocker.patch('data_detective_airflow.operators.extractors.request_dump.sleep')
        mocker.patch.object(Session,
                            'get',
                            return_value=mocker.MagicMock(
                                content=content,
                                ok=scs)
                            )

    with allure.step('Create a task with a context'):
        task = RequestDump(conn_id=PG_CONN_ID,
                           url='long_req_url',
                           task_id="test_request_dump",
                           wait_seconds=wait_time,
                           dag=test_dag)

        test_dag.get_work(test_dag.conn_id).create(context)

    with allure.step('Run a task'):
        try:
            err = None
            run_task(task=task, context=context)
            task_res = task.result.read(context)
            sleep_mock.assert_called_with(wait_time)
            assert_array_equal(task_res.values,
                               dataset['request_dump'].values)
            state = 0
        except AirflowBadRequest:
            state = 1
        except Exception as exc:
            state = -1
        assert (scs and not state) or (not scs and state)
