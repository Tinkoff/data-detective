from pandas import DataFrame
import petl

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import DBDump, PgSingleTargetLoader, PyTransform
from data_detective_airflow.utils.petl_utils import appender_petl2pandas

from common.builders import JsonSystemBuilder, TableInfoBuilder, TableInfoDescriptionType
from common.urn import get_schema
from common.utilities.entity_enums import (
    ENTITY_CORE_FIELDS,
    EntityTypes, EntityFields,
    RelationTypes
)
from common.utilities.search_enums import CardType, SystemForSearch, TypeForSearch


def transform_schema_to_entity(_context: dict, schemas: DataFrame) -> tuple[tuple]:
    """Transform schema metadata to dds entity table format
    :param _context: airflow DAG run context
    :param schemas: Dataframe['schema_name', 'schema_owner', 'schema_acl', 'schema_description']
    :return: petl.tables(ENTITY_CORE_FIELDS + EntityFields.TABLES, EntityFields.JSON_SYSTEM)
    """
    json_system_builder = JsonSystemBuilder(
        system_for_search=SystemForSearch.POSTGRES.name,
        type_for_search=TypeForSearch.SCHEMA.name,
        card_type=CardType.SCHEMA.name,
    )

    schema_table_info_description = TableInfoDescriptionType(
        keys={
            'schema_owner': 'Owner',
            'schema_acl': 'Access privileges',
        },
        header='General',
        display_headers='0',
        orientation='vertical'
    )
    schema_table_info_builder = TableInfoBuilder(schema_table_info_description)

    result = (petl.fromdataframe(schemas)
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.SCHEMA)
              .addfield(EntityFields.ENTITY_NAME, lambda row: row['schema_name'].lower())
              .addfield(EntityFields.ENTITY_NAME_SHORT, None)
              .addfield(EntityFields.URN,
                        lambda row: get_schema('postgres', 'pg', 'airflow', row[EntityFields.ENTITY_NAME]))
              .rename('schema_description', EntityFields.INFO)
              .addfield(EntityFields.JSON_DATA,
                        lambda row: dict(schema_owner=row['schema_owner'], schema_acl=row['schema_acl']))
              .addfield(EntityFields.TABLES, lambda row: [schema_table_info_builder(row)])
              .addfield(EntityFields.JSON_SYSTEM, json_system_builder())
              .addfield(EntityFields.SEARCH_DATA,
                        lambda row: f"{row[EntityFields.URN]} {row[EntityFields.ENTITY_NAME]}")
              .cut(list(ENTITY_CORE_FIELDS) + [EntityFields.TABLES, EntityFields.JSON_SYSTEM])
              .distinct(key=EntityFields.URN)
              )
    return result.tupleoftuples()


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

    PyTransform(
        task_id='transform_schema_to_entity',
        description='Transform schemas metadata to dd.entity',
        source=['dump_pg_schemas'],
        transformer_callable=transform_schema_to_entity,
        dag=t_dag,
    )

    PyTransform(
        task_id='append_entities',
        description='Append entities to load in entity tables',
        source=['transform_schema_to_entity'],
        transformer_callable=appender_petl2pandas,
        dag=t_dag,
    )

    PgSingleTargetLoader.upload_dds_entity(dag=t_dag, sources=['append_entities'])
