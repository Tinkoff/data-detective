from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator.dags import TDag
from data_detective_airflow.operators.extractors import DBDump
from data_detective_airflow.operators.transformers import PyTransform


def fill_dag(tdag: TDag):
    DBDump(
        task_id='df_left',
        conn_id=PG_CONN_ID,
        sql='select now() as value;',
        dag=tdag
    )

    DBDump(
        task_id='df_right',
        conn_id=PG_CONN_ID,
        sql='select now() as value;',
        dag=tdag
    )

    PyTransform(
        task_id='append_everything',
        source=['df_left', 'df_right'],
        transformer_callable=lambda _context, df_now1, transform:
        df_now1.append(transform, sort=False),
        dag=tdag
    )
