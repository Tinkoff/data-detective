from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator.dags import TDag
from data_detective_airflow.operators.extractors import DBDump
from data_detective_airflow.operators.sinks import PgSCD1
from data_detective_airflow.operators.transformers import Append, PyTransform


def fill_dag(tdag: TDag):
    DBDump(
        task_id="test1",
        conn_id=PG_CONN_ID,
        sql="/code/test1.sql",
        dag=tdag,
    )

    DBDump(
        task_id="test2",
        conn_id=PG_CONN_ID,
        sql="/code/test1.sql",
        dag=tdag,
    )

    PyTransform(
        task_id="transform",
        source=["test2"],
        transformer_callable=lambda _context, df: df,
        dag=tdag,
    )

    Append(
        task_id="append_all",
        source=["transform", "test1"],
        dag=tdag,
    )

    PgSCD1(
        task_id="sink",
        source=["append_all"],
        conn_id=PG_CONN_ID,
        table_name="test2",
        key=["test"],
        dag=tdag,
    )
