import datetime

import pytest
from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from mg_airflow.dag_generator import TDag, ResultType, WorkType


@pytest.fixture
def test_dag(request) -> TDag:
    """Airflow DAG for testing."""
    if hasattr(request, 'param'):
        result_type, work_type, work_conn_id = request.param
    else:
        result_type = ResultType.RESULT_PICKLE.value
        work_type = WorkType.WORK_FILE.value
        work_conn_id = None

    tdag = TDag(
        dag_dir="test",
        dag_id=request.node.originalname or request.node.name,
        default_args={
            'owner': 'airflow',
            'result_type': result_type,
            'work_conn_id': work_conn_id,
            'work_type': work_type
        },
        schedule_interval=datetime.timedelta(days=1),
        start_date=datetime.datetime(2020, 2, 2),
        template_searchpath='/'
    )

    yield tdag

    if tdag.tasks:
        task = tdag.tasks[0]
    else:
        task = DummyOperator(task_id='dummy', dag=tdag)
    ti = TaskInstance(task=task, execution_date=tdag.start_date)
    context = ti.get_template_context()

    tdag.clear_all_works(context)


@pytest.fixture
def dummy_task(test_dag):
    """dummy task for testing"""
    return DummyOperator(task_id='dummy', dag=test_dag)


@pytest.fixture
def context(dummy_task):
    """context for testing"""
    task_instance = TaskInstance(task=dummy_task, execution_date=dummy_task.dag.start_date)
    res = task_instance.get_template_context()
    dag_run = DagRun(external_trigger=True)
    dag_run.id = 1
    res.update({'dag_run': dag_run})
    return res
