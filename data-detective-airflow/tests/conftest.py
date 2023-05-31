from datetime import datetime, timedelta

import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator

from data_detective_airflow.dag_generator import TDag, ResultType, WorkType


@pytest.fixture(scope='function')
def test_dag(request) -> TDag:
    """Airflow DAG for testing."""

    result_type = ResultType.RESULT_PICKLE.value
    work_type = WorkType.WORK_FILE.value
    work_conn_id = None

    if hasattr(request, 'param') and len(request.param) == 3:
        result_type, work_type, work_conn_id = request.param

    t_dag = TDag(
        dag_dir="test",
        dag_id=request.node.originalname or request.node.name,
        default_args={
            'owner': 'airflow',
            'result_type': result_type,
            'work_conn_id': work_conn_id,
            'work_type': work_type
        },
        schedule=timedelta(days=1),
        start_date=datetime(2020, 2, 2),
        template_searchpath='/'
    )
    t_dag.clear()

    yield t_dag

    if t_dag.tasks:
        task = t_dag.tasks[0]
    else:
        task = EmptyOperator(task_id='dummy', dag=t_dag)

    ti = TaskInstance(task=task, run_id=t_dag.get_last_dagrun().run_id)
    context = ti.get_template_context()

    t_dag.clear_all_works(context)


@pytest.fixture
def dummy_task(test_dag):
    """dummy task for testing"""
    return EmptyOperator(task_id='dummy', dag=test_dag)

