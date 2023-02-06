from pandas import DataFrame

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSCD1, PyTransform

from common.utilities.entity_enums import EntityTypes


def get_data_from_entity_types(context: dict) -> DataFrame:
    """Get message code from EntityType

    :param context: Execution context
    :return: Dataframe ['type', 'code', 'message_code', 'loaded_by']
    """

    types = [
        ['entity_type', entity_type.key, entity_type.message_code, context['dag'].dag_id]
        for _, entity_type in EntityTypes.__members__.items()
    ]

    return DataFrame(types, columns=['type', 'code', 'message_code', 'loaded_by'])


def fill_dag(t_dag: TDag):

    PyTransform(
        task_id='get_data_from_entity_types',
        description='Get message code from EntityType',
        transformer_callable=get_data_from_entity_types,
        dag=t_dag,
    )

    PgSCD1(
        task_id='upload_data_to_tuning.search_system_x_type',
        description='Upload data into tuning.dictionary table',
        conn_id=PG_CONN_ID,
        process_deletions=True,
        table_name='tuning.dictionary',
        source=['get_data_from_entity_types'],
        key=['message_code'],
        dag=t_dag,
    )
