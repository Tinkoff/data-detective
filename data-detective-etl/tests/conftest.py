import pytest
import datetime

from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_detective_airflow.dag_generator import TDag, ResultType, WorkType


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

    op = DummyOperator(task_id='dummy', dag=tdag)
    ti = TaskInstance(task=op, execution_date=op.dag.start_date)
    context = ti.get_template_context()

    tdag.clear_all_works(context)


@pytest.fixture
def dummy_task(test_dag):
    """dummy task for testing"""
    return DummyOperator(task_id='dummy_etl', dag=test_dag)


@pytest.fixture
def context(dummy_task):
    """context for testing"""
    task_instance = TaskInstance(task=dummy_task, execution_date=dummy_task.dag.start_date)
    res = task_instance.get_template_context()
    dag_run = DagRun(external_trigger=True)
    dag_run.id = 1
    res.update({'dag_run': dag_run})
    return res


@pytest.fixture(scope='session', autouse=True)
def ensure_unique():
    sql_create_dds_entity_unique_index = "CREATE UNIQUE INDEX entity_urn_index ON dds.entity (urn);"
    sql_drop_dds_entity_unique_index = "DROP INDEX IF EXISTS dds.entity_urn_index;"
    sql_create_dds_entity_index = "CREATE INDEX entity_urn_index ON dds.entity (urn);"
    sql_add_dds_relation_unique = "ALTER TABLE dds.relation ADD CONSTRAINT rel_unique UNIQUE (source, destination, type);"
    sql_drop_dds_relation_unique = "ALTER TABLE dds.relation DROP CONSTRAINT IF EXISTS rel_unique;"

    pg_hook = PostgresHook(postgres_conn_id='pg')

    # drop unique constraint just in case
    pg_hook.run(sql_drop_dds_relation_unique)
    # drop unique index just in case
    pg_hook.run(sql_drop_dds_entity_unique_index)
    # add unique constraint on setup
    pg_hook.run(sql_add_dds_relation_unique)
    # create unique index on setup
    pg_hook.run(sql_create_dds_entity_unique_index)

    yield

    # drop unique constraint on teardown
    pg_hook.run(sql_drop_dds_relation_unique)
    # drop unique index on teardown
    pg_hook.run(sql_drop_dds_entity_unique_index)
    # re-create non unique index on teardown
    pg_hook.run(sql_create_dds_entity_index)
