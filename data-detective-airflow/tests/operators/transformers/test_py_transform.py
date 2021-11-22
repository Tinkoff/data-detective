import petl
import allure
import pytest
from petl import Table

from airflow.models.taskinstance import TaskInstance
from airflow.utils.timezone import utcnow

from mg_airflow.dag_generator import ResultType, WorkType
from mg_airflow.operators.transformers import PyTransform
from mg_airflow.test_utilities import run_task


# petl tests

table1 = petl.wrap([
    ['account_rk', 'name', 'age'],
    [123, 'Sergey', 25],
    [133, 'Anton', 30],
    [421, 'Dmirty', 26],
    [978, 'Olga', 22]
])

table_empty = petl.wrap([
    ['account_rk', 'name', 'age'],
])


def transform(_context, source: Table):
    return petl.cutout(source, 'name')


def transform_with_result_kwargs(_context, source: Table, p1, p2):
    return petl.addfields(source, [('p1', p1), ('p2', p2)])


def truncate_func(_context, source: Table):
    return petl.head(source, 0)


@allure.feature('Transformers')
@allure.story('PyTransform with petl')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_pytransform_petl(test_dag):
    test_dag.clear()

    source_task = PyTransform(transformer_callable=lambda _context: table1, task_id='source_task',
                              dag=test_dag)
    task = PyTransform(task_id='test_pytransform_petl', source=['source_task'],
                       transformer_callable=transform, dag=test_dag)

    ti = TaskInstance(task=source_task, execution_date=utcnow())  # test_dag.start_date
    context = ti.get_template_context()
    test_dag.get_work(work_type=test_dag.work_type).create(context)

    run_task(task=source_task, context=context)
    run_task(task=task, context=context)

    result = task.result.read(context)

    assert 'name' not in petl.header(result)
    assert petl.nrows(result) == 4


@allure.feature('Transformers')
@allure.story('PyTransform with petl')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_petl_op_kwargs(test_dag):
    test_dag.clear()

    source_task = PyTransform(transformer_callable=lambda _context: table1, task_id='source_task',
                              dag=test_dag)
    task = PyTransform(task_id='test_petl', source=['source_task'],
                       op_kwargs={'p1': 'test1', 'p2': 'test2'},
                       transformer_callable=transform_with_result_kwargs, dag=test_dag)

    ti = TaskInstance(task=source_task, execution_date=utcnow())  # test_dag.start_date
    context = ti.get_template_context()
    test_dag.get_work(work_type=test_dag.work_type).create(context)

    run_task(task=source_task, context=context)
    run_task(task=task, context=context)

    result = task.result.read(context)

    assert 'p1' in petl.header(result)
    assert 'p2' in petl.header(result)
    assert petl.nrows(result) == 4

    cols = petl.cut(result, 'p1', 'p2').columns()

    assert cols['p1'] == ['test1', 'test1', 'test1', 'test1']
    assert cols['p2'] == ['test2', 'test2', 'test2', 'test2']


@allure.feature('Transformers')
@allure.story('PyTransform with petl')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_multiple_sources(test_dag):
    test_dag.clear()

    source_task = PyTransform(
        task_id='source_task',
        transformer_callable=lambda _context: table1,
        dag=test_dag
    )
    task = PyTransform(
        task_id='test_petl',
        source=['source_task', 'source_task'],
        transformer_callable=lambda _context, *src: petl.cat(*src),
        dag=test_dag
    )

    ti = TaskInstance(task=source_task, execution_date=utcnow())  # test_dag.start_date
    context = ti.get_template_context()
    test_dag.get_work(work_type=test_dag.work_type).create(context)

    run_task(task=source_task, context=context)
    run_task(task=task, context=context)

    result = task.result.read(context)

    assert petl.header(result) == ('account_rk', 'name', 'age')
    assert petl.nrows(result) == 8


@allure.feature('Transformers')
@allure.story('PyTransform with petl, empty source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_petl_empty_source(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()

        source_task = PyTransform(
            task_id='source_task',
            transformer_callable=lambda _context: table_empty,
            dag=test_dag
        )
        task = PyTransform(
            task_id='test_petl',
            source=['source_task'],
            transformer_callable=transform,
            dag=test_dag
        )

        ti = TaskInstance(task=source_task, execution_date=utcnow())  # test_dag.start_date
        context = ti.get_template_context()
        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks'):
        run_task(task=source_task, context=context)
        run_task(task=task, context=context)

    with allure.step('Check empty result'):
        res = task.result.read(context)

        assert not res.nrows()


@allure.feature('Transformers')
@allure.story('PyTransform with petl, empty target')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_petl_empty_target(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()

        source_task = PyTransform(
            task_id='source_task',
            transformer_callable=lambda _context: table1,
            dag=test_dag
        )
        task = PyTransform(
            task_id='test_petl',
            source=['source_task'],
            transformer_callable=truncate_func,
            dag=test_dag
        )

        ti = TaskInstance(task=source_task, execution_date=utcnow())  # test_dag.start_date
        context = ti.get_template_context()
        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks'):
        run_task(task=source_task, context=context)
        run_task(task=task, context=context)

    with allure.step('Check empty result'):
        res = task.result.read(context)

        assert not res.nrows()


@allure.feature('Transformers')
@allure.story('PyTransform with petl, non-existent source')
@pytest.mark.parametrize('test_dag',
                         [(ResultType.RESULT_PICKLE.value,
                           WorkType.WORK_FILE.value,
                           None)],
                         indirect=True)
def test_petl_not_exist_source(test_dag):
    with allure.step('Create tasks and context'):
        test_dag.clear()
        source_task = PyTransform(
            task_id='source_task',
            transformer_callable=lambda _context: table1,
            dag=test_dag
        )
        task = PyTransform(
            task_id='test_petl',
            source=['source_task'],
            transformer_callable=transform,
            dag=test_dag
        )

        ti = TaskInstance(task=source_task, execution_date=utcnow())  # test_dag.start_date
        context = ti.get_template_context()
        test_dag.get_work(work_type=test_dag.work_type).create(context)

    with allure.step('Run tasks with exception'):
        state = -1
        try:
            run_task(task=task, context=context)
        except FileNotFoundError:
            state = 1
        finally:
            pass
        assert state == 1
