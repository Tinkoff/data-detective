import allure
import pandas
import pytest

from data_detective_airflow.operators import PythonDump, Append
from data_detective_airflow.dag_generator import ResultType, WorkType
from data_detective_airflow.test_utilities import create_or_get_dagrun, get_template_context, run_task
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
def test_append(test_dag, dummy_task):
    task_d1 = PythonDump(python_callable=lambda context: df1, task_id="test_df1_dump",
                         dag=test_dag)
    task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                         dag=test_dag)
    task_d3 = PythonDump(python_callable=lambda context: df3, task_id="test_df3_dump",
                         dag=test_dag)
    task = Append(task_id="test_df_append",
                  source=['test_df1_dump', 'test_df2_dump', 'test_df3_dump'], dag=test_dag)
    create_or_get_dagrun(test_dag, dummy_task)

    run_task(task=task_d1, context=get_template_context(task_d1))
    run_task(task=task_d2, context=get_template_context(task_d2))
    run_task(task=task_d3, context=get_template_context(task_d3))
    context = get_template_context(task)
    run_task(task=task, context=context)

    assert task.result.read(context).shape == (8, 3)


@allure.feature('Transformers')
@allure.story('Append duplicate sources')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_duplicate_sources(test_dag, dummy_task):
    with allure.step('Create tasks and context'):
        task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                             dag=test_dag)
        task_double_d2 = PythonDump(python_callable=lambda context: double_df2,
                                    task_id="test_double_df2_dump",
                                    dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df2_dump', 'test_double_df2_dump'], dag=test_dag)
        create_or_get_dagrun(test_dag, dummy_task)

    with allure.step('Run tasks'):
        run_task(task=task_d2, context=get_template_context(task_d2))
        run_task(task=task_double_d2, context=get_template_context(task_double_d2))
        context = get_template_context(task)
        run_task(task=task, context=context)

    with allure.step('Check result contains all rows'):
        assert task.result.read(context).shape == (4, 3)


@allure.feature('Transformers')
@allure.story('Append with one empty source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_one_empty_source(test_dag, dummy_task):

    with allure.step('Create tasks and context'):
        task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                             dag=test_dag)
        task_d3 = PythonDump(python_callable=lambda context: df3, task_id="test_df3_dump",
                             dag=test_dag)
        task_d_empty = PythonDump(python_callable=lambda context: df_empty,
                                  task_id="test_df_empty_dump",
                                  dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df2_dump', 'test_df3_dump', 'test_df_empty_dump'], dag=test_dag)
        create_or_get_dagrun(test_dag, dummy_task)

    with allure.step('Run tasks'):
        run_task(task=task_d2, context=get_template_context(task_d2))
        run_task(task=task_d3, context=get_template_context(task_d3))
        run_task(task=task_d_empty, context=get_template_context(task_d_empty))
        context = get_template_context(task)
        run_task(task=task, context=context)

    with allure.step('Check result'):
        assert task.result.read(context).shape == (4, 3)


@allure.feature('Transformers')
@allure.story('Append empty sources')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_empty_sources(test_dag, dummy_task):

    with allure.step('Create tasks and context'):
        task_d_empty = PythonDump(python_callable=lambda context: df_empty,
                                  task_id="test_df_empty_dump",
                                  dag=test_dag)
        task_d2_empty = PythonDump(python_callable=lambda context: df_empty2,
                                   task_id="test_df2_empty_dump",
                                   dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=['test_df_empty_dump', 'test_df2_empty_dump'], dag=test_dag)
        create_or_get_dagrun(test_dag, dummy_task)

    with allure.step('Run tasks'):
        run_task(task=task_d_empty, context=get_template_context(task_d_empty))
        run_task(task=task_d2_empty, context=get_template_context(task_d2_empty))
        context = get_template_context(task)
        run_task(task=task, context=context)

    with allure.step('Check empty result'):
        assert task.result.read(context).empty


@allure.feature('Transformers')
@allure.story('Append many sources')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_many_sources(test_dag, dummy_task):

    with allure.step('Generate data and tasks'):
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
        create_or_get_dagrun(test_dag, dummy_task)

        context = get_template_context(task)
    with allure.step('Run tasks'):
        for tsk in tasks:
            run_task(task=tsk, context=get_template_context(tsk))
        run_task(task=task, context=context)

    with allure.step('Check result'):
        assert task.result.read(context).shape == (sources_count * records_count, columns_count)


@allure.feature('Transformers')
@allure.story('Append with a non-existent source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_append_not_exist_source(test_dag, dummy_task):


    with allure.step('Create tasks and context'):
        task_d1 = PythonDump(python_callable=lambda context: df1, task_id="test_df1_dump",
                             dag=test_dag)
        task_d2 = PythonDump(python_callable=lambda context: df2, task_id="test_df2_dump",
                             dag=test_dag)
        task_d3 = PythonDump(python_callable=lambda context: df3, task_id="test_df3_dump",
                             dag=test_dag)
        task = Append(task_id="test_df_append",
                      source=[task_d1.task_id, task_d2.task_id, task_d3.task_id], dag=test_dag)
        create_or_get_dagrun(test_dag, dummy_task)

    with allure.step('Run tasks with exception'):
        run_task(task=task_d1, context=get_template_context(task_d1))
        run_task(task=task_d2, context=get_template_context(task_d2))
        # not run task_d3
        try:
            context = get_template_context(task)
            run_task(task=task, context=context)
            task_res = task.result.read(context)
            # state: '0'-отработал, '1'-упал с ожидаемой ошибкой, '-1'-упал с другой ошибкой
            state = 0
        except FileNotFoundError:
            state = 1
        except Exception as exc:
            state = -1
        assert state == 1
