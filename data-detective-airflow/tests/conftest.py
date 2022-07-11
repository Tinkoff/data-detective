from datetime import datetime, timedelta, timezone

import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils.types import DagRunType

from data_detective_airflow.dag_generator import TDag, ResultType, WorkType
from data_detective_airflow.test_utilities import get_template_context


@pytest.fixture(scope='function')
#  airflow.exceptions.DuplicateTaskIdFound: Task id 'test_task_pgscd1df' has already been added to the DAG
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
        schedule_interval=timedelta(days=1),
        start_date=datetime(2020, 2, 2),
        template_searchpath='/'
    )
    tdag.clear()
    run = tdag.create_dagrun(
        execution_date=datetime.utcnow(),
        run_type=DagRunType.MANUAL,
        state='queued',
        data_interval=(
            datetime.now(timezone(offset=timedelta(hours=0))),
            datetime.now(timezone(offset=timedelta(hours=0))) + timedelta(hours=1)
        )
    )

    yield tdag

    if tdag.tasks:
        task = tdag.tasks[0]
    else:
        task = DummyOperator(task_id='dummy', dag=tdag)
    ti = TaskInstance(task=task, run_id=run.run_id)
    context = ti.get_template_context()

    tdag.clear_all_works(context)


@pytest.fixture
def dummy_task(test_dag):
    """dummy task for testing"""
    return DummyOperator(task_id='dummy', dag=test_dag)


@pytest.fixture
def context(dummy_task):
    """context for testing"""
    # run = dummy_task.dag.get_last_dagrun(include_externally_triggered=True)
    # task_instance = TaskInstance(task=dummy_task, run_id=run.run_id)
    return get_template_context(dummy_task)
