import datetime

import pandas
import allure
import pytest
from airflow.models.taskinstance import TaskInstance

from data_detective_airflow.operators import PythonDump, Append
from data_detective_airflow.dag_generator import ResultType, WorkType
from data_detective_airflow.test_utilities import run_task
from data_detective_airflow.test_utilities.generate_df import generate_dfs_with_random_data

df1 = pandas.DataFrame(
    [
        [123, 'Sergey', 25],
        [133, 'Anton', 30],
        [421, 'Dmirty', 26],
        [978, 'Olga', 22]
    ],
    columns=['Account_rk', 'Name', 'Age'])

df2 = pandas.DataFrame(
    [
        [713, 'Vladimir', 25],
        [934, 'Ivan', 40]
    ],
    columns=['Account_rk', 'Name', 'Age'])

double_df2 = df2.copy(deep=True)

df3 = pandas.DataFrame(
    [
        [901, 'Grigory', 25],
        [111, 'Petr', 40]
    ],
    columns=['Account_rk', 'Name', 'Age'])

df_empty = pandas.DataFrame(columns=['Account_rk', 'Name', 'Age'])

df_empty2 = df_empty.copy(deep=True)


@allure.feature('Transformers')
@allure.story('Append')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append(test_dag):
    test_dag.clear()
    task_d1 = PythonDump(python_callable=lambda context: df1, task_id="test_df1_dump",
                         dag=test_dag)
    task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                         dag=test_dag)
    task_d3 = PythonDump(python_callable=lambda context: df3, task_id="test_df3_dump",
                         dag=test_dag)
    task = Append(task_id="test_df_append",
                  source=['test_df1_dump', 'test_df2_dump', 'test_df3_dump'], dag=test_dag)

    ti = TaskInstance(task=task_d1, execution_date=datetime.datetime.now())  # test_dag.start_date
    context = ti.get_template_context()

    test_dag.get_work(work_type=test_dag.work_type).create(context)

    run_task(task=task_d1, context=context)
    run_task(task=task_d2, context=context)
    run_task(task=task_d3, context=context)
    run_task(task=task, context=context)

    assert task.result.read(context).shape == (8, 3)
    test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('Append duplicate sources')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_duplicate_sources(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()
        task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                             dag=test_dag)
        task_double_d2 = PythonDump(python_callable=lambda context: double_df2,
                                    task_id="test_double_df2_dump",
                                    dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df2_dump', 'test_double_df2_dump'], dag=test_dag)

        ti = TaskInstance(task=task_d2,
                          execution_date=datetime.datetime.now())  # test_dag.start_date
        context = ti.get_template_context()

        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks'):
        run_task(task=task_d2, context=context)
        run_task(task=task_double_d2, context=context)
        run_task(task=task, context=context)

    with allure.step('Check result contains all rows'):
        assert task.result.read(context).shape == (4, 3)
        test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('Append with one empty source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_one_empty_source(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()
        task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                             dag=test_dag)
        task_d3 = PythonDump(python_callable=lambda context: df3, task_id="test_df3_dump",
                             dag=test_dag)
        task_d_empty = PythonDump(python_callable=lambda context: df_empty,
                                  task_id="test_df_empty_dump",
                                  dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df2_dump', 'test_df3_dump', 'test_df_empty_dump'], dag=test_dag)

        ti = TaskInstance(task=task_d2,
                          execution_date=datetime.datetime.now())  # test_dag.start_date
        context = ti.get_template_context()

        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks'):
        run_task(task=task_d2, context=context)
        run_task(task=task_d3, context=context)
        run_task(task=task_d_empty, context=context)
        run_task(task=task, context=context)

    with allure.step('Check result'):
        assert task.result.read(context).shape == (4, 3)
        test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('Append empty sources')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_empty_sources(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()
        task_d_empty = PythonDump(python_callable=lambda context: df_empty,
                                  task_id="test_df_empty_dump",
                                  dag=test_dag)
        task_d2_empty = PythonDump(python_callable=lambda context: df_empty2,
                                   task_id="test_df2_empty_dump",
                                   dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df_empty_dump', 'test_df2_empty_dump'], dag=test_dag)

        ti = TaskInstance(task=task_d_empty,
                          execution_date=datetime.datetime.now())  # test_dag.start_date
        context = ti.get_template_context()

        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks'):
        run_task(task=task_d_empty, context=context)
        run_task(task=task_d2_empty, context=context)
        run_task(task=task, context=context)

    with allure.step('Check empty result'):
        assert task.result.read(context).empty
        test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('Append many sources')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_many_sources(test_dag):
    with allure.step('Generate data and tasks'):
        test_dag.clear()
        tasks = []
        sources = []
        sources_count = 20
        records_count = 2
        columns = {'Account_rk': 'int', 'Name': 'str', 'Age': 'int'}

        dataframes = generate_dfs_with_random_data(columns=columns,
                                                   dataframes_count=sources_count,
                                                   records_count=records_count)
        columns_count = len(columns)

        for id, df in enumerate(dataframes):
            task_id = f"test_df_{id}"
            sources.append(task_id)
            tasks.append(
                PythonDump(python_callable=lambda context: df, task_id=task_id, dag=test_dag))

        task = Append(task_id="test_df_append", source=sources, dag=test_dag)

    with allure.step('Create context'):
        ti = TaskInstance(task=tasks[0] if tasks else None,
                          execution_date=datetime.datetime.now())  # test_dag.start_date
        context = ti.get_template_context()

        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks'):
        for task_name in tasks:
            run_task(task=task_name, context=context)
        run_task(task=task, context=context)

    with allure.step('Check result'):
        assert task.result.read(context).shape == (sources_count * records_count, columns_count)
        test_dag.clear_all_works(context)


@allure.feature('Transformers')
@allure.story('Append with a non-existent source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_not_exist_source(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()
        task_d1 = PythonDump(python_callable=lambda context: df1, task_id="test_df1_dump",
                             dag=test_dag)
        task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                             dag=test_dag)
        task_d3 = PythonDump(python_callable=lambda context: df3, task_id="test_df3_dump",
                             dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df1_dump', 'test_df2_dump', 'test_df3_dump'], dag=test_dag)

        ti = TaskInstance(task=task_d1,
                          execution_date=datetime.datetime.now())  # test_dag.start_date
        context = ti.get_template_context()

        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks with exception'):
        run_task(task=task_d1, context=context)
        run_task(task=task_d2, context=context)
        # not run task_d3
        try:
            err = None
            run_task(task=task, context=context)
            task_res = task.result.read(context)
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except FileNotFoundError:
            state = 1
        except Exception as exc:
            state = -1
        assert state == 1
        test_dag.clear_all_works(context)
