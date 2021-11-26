import petl

from data_detective_airflow.constants import PG_CONN_ID

from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSCD1, PyTransform
from data_detective_airflow.utils.petl_utils import dump_sql_petl_tupleoftuples, appender_petl2pandas


def fill_dag(tdag: TDag):
    PyTransform(
        task_id='dump1',
        description='Get query results from test1.sql',
        transformer_callable=lambda _context, **kwargs: dump_sql_petl_tupleoftuples(**kwargs),
        template_kwargs=dict(
            conn_id=PG_CONN_ID,
            sql='/code/test1.sql'),
        dag=tdag
    )

    PyTransform(
        task_id='dump2',
        description='Get query results from test1.sql again',
        transformer_callable=lambda _context, **kwargs: dump_sql_petl_tupleoftuples(**kwargs),
        template_kwargs=dict(
            conn_id=PG_CONN_ID,
            sql='/code/test1.sql'),
        dag=tdag
    )

    PyTransform(
        task_id='transform',
        description='Add a number to the test1 column',
        source=['dump2'],
        transformer_callable=lambda _context, rows: petl
            .wrap(rows).convert('test1', lambda row: row + 5).tot(),
        dag=tdag
    )

    PyTransform(
        task_id='append_all',
        description='Append the results of two flows',
        source=['transform', 'dump1'],
        transformer_callable=appender_petl2pandas,
        dag=tdag
    )

    PgSCD1(
        task_id='sink',
        description='Load to the database',
        source=['append_all'],
        conn_id=PG_CONN_ID,
        table_name='test2',
        key=['test'],
        dag=tdag
    )
