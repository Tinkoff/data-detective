from pandas import DataFrame
import petl

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import DBDump, PgSCD1, PyTransform

from common.utilities.search_enums import SystemForSearch, TypeForSearch


def fill_dag(t_dag: TDag):

    DBDump(
        task_id='dump_pg_schemas',
        description='Dump schemas from pg database',
        conn_id=PG_CONN_ID,
        sql='/code/dump_schemas.sql',
        dag=t_dag,
    )

    DBDump(
        task_id='dump_pg_tables',
        description='Dump tables from pg database',
        conn_id=PG_CONN_ID,
        sql='/code/dump_tables.sql',
        dag=t_dag,
    )

    DBDump(
        task_id='dump_pg_columns',
        description='Dump columns from pg database',
        conn_id=PG_CONN_ID,
        sql='/code/dump_columns.sql',
        dag=t_dag,
    )
