from pandas import DataFrame

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSCD1, PyTransform

from common.utilities.search_enums import SystemForSearch, TypeForSearch


def get_data_from_search_enums(context: dict) -> DataFrame:
    """Get data from SystemForSearch and TypeForSearch
    :param context: Execution context
    :returns: Dataframe ['type', 'key', 'name', 'description']
    """
    search_help_fields = ['type', 'name', 'description', 'loaded_by']
    systems = [['SYSTEM',
                search_system.name,
                search_system.description,
                context['dag'].dag_id]
               for _, search_system in SystemForSearch.__members__.items()]
    types = [['TYPE',
              search_type.name,
              search_type.description,
              context['dag'].dag_id]
             for _, search_type in TypeForSearch.__members__.items()]

    return DataFrame(systems + types, columns=search_help_fields)


def fill_dag(t_dag: TDag):

    PyTransform(
        task_id='get_data_from_search_enums',
        description='Get data from SystemForSearch and TypeForSearch classes',
        transformer_callable=get_data_from_search_enums,
        dag=t_dag,
    )

    PgSCD1(
        task_id='upload_data_to_tuning_search_help',
        description='Upload data into tuning.search_help table',
        conn_id=PG_CONN_ID,
        process_deletions=True,
        table_name='tuning.search_help',
        source=['get_data_from_search_enums'],
        key=['type', 'name'],
        dag=t_dag,
    )
