from pandas import DataFrame
import petl

from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSingleTargetLoader, PyTransform
from data_detective_airflow.utils.petl_utils import appender_petl2pandas

from common.builders import JsonSystemBuilder
from common.utilities.entity_enums import (
    ENTITY_CORE_FIELDS,
    EntityTypes, EntityFields
)
from common.utilities.search_enums import SystemForSearch, TypeForSearch


def transform_dag_to_entity(_context: dict, source: DataFrame) -> tuple[tuple]:
    """Transform .... to dds entity table format
    :param _context: airflow DAG task run context
    :param source:
    :return: petl.tables()
    """
    json_system_builder = JsonSystemBuilder(
        system_for_search=SystemForSearch.DATA_DETECTIVE.name,
        type_for_search=TypeForSearch.JOB.name,
    )

    result = (petl.fromdataframe(source)
              .addfield(EntityFields.URN)
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.JOB)
              .addfield(EntityFields.JSON_SYSTEM, json_system_builder())
              .cut(list(ENTITY_CORE_FIELDS) + [EntityFields.JSON_SYSTEM])
              .distinct(key=EntityFields.URN)
              )
    return result.tupleoftuples()


def fill_dag(t_dag: TDag):

    PyTransform(
        task_id='transform_dag_to_entity',
        description='Transform dags metadata to dds.entity',
        source=['load_info_about_dags'],
        transformer_callable=transform_dag_to_entity,
        dag=t_dag,
    )

    PyTransform(
        task_id='append_entities',
        description='Append entities to load in entity table',
        source=['transform_dag_to_entity'],
        transformer_callable=appender_petl2pandas,
        dag=t_dag,
    )
    PgSingleTargetLoader.upload_dds_entity(dag=t_dag, sources=['append_entities'])
