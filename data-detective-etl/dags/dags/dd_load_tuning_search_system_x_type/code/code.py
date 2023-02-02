from typing import Any

import petl

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSCD1, PyTransform
from data_detective_airflow.utils.petl_utils import exploder

from common.utilities.search_enums import system_for_search_x_type_for_search


def link_system_x_type_from_search_enums(context: dict) -> tuple[tuple[Any]]:
    """Link search systems with their types
    :param context: Execution context
    :returns: ('system_name', 'type_name', 'loaded_by')
    """
    return (
        petl.fromdicts(system_for_search_x_type_for_search, header=['system_name', 'type_name'])
        .rowmapmany(lambda row: exploder(row, 'type_name'), header=['system_name', 'type_name'])
        .addfield('loaded_by', context['dag'].dag_id)
        .todataframe()
    )


def fill_dag(t_dag: TDag):

    PyTransform(
        task_id='link_system_x_type_from_search_enums',
        description='Link search systems with their types',
        transformer_callable=link_system_x_type_from_search_enums,
        dag=t_dag,
    )

    PgSCD1(
        task_id='upload_data_to_tuning.search_system_x_type',
        description='Upload data into tuning.search_system_x_type table',
        conn_id=PG_CONN_ID,
        process_deletions=True,
        table_name='tuning.search_system_x_type',
        source=['link_system_x_type_from_search_enums'],
        key=['system_name', 'type_name'],
        dag=t_dag,
    )
