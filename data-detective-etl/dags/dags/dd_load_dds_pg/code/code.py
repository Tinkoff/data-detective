import json
from pandas import DataFrame
import petl

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import DBDump, PgSingleTargetLoader, PyTransform
from data_detective_airflow.utils.petl_utils import appender_petl2pandas

from common.builders import JsonSystemBuilder, TableInfoBuilder, TableInfoDescriptionType
from common.urn import get_tree_node, get_schema, get_table, get_column
from common.utils import get_readable_size_bytes
from common.utilities.entity_enums import (
    ENTITY_CORE_FIELDS, RELATION_CORE_FIELDS,
    EntityTypes, EntityFields,
    RelationTypes, RelationFields
)
from common.utilities.search_enums import CardType, SystemForSearch, TypeForSearch


def transform_schema_to_entity(_context: dict, schemas: DataFrame) -> tuple[tuple]:
    """Transform schema metadata to dds entity table format
    :param _context: airflow DAG task run context
    :param schemas: Dataframe['schema_name', 'schema_owner', 'schema_acl', 'schema_description']
    :return: petl.tables(ENTITY_CORE_FIELDS + EntityFields.TABLES, EntityFields.JSON_SYSTEM, EntityFields.INFO)
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
              .addfield(EntityFields.ENTITY_NAME, lambda row: row['schema_name'])
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
              .cut(list(ENTITY_CORE_FIELDS)
                   + [EntityFields.TABLES, EntityFields.JSON_SYSTEM, EntityFields.INFO])
              .distinct(key=EntityFields.URN)
              )
    return result.tupleoftuples()


def transform_table_to_entity(_context: dict, tables: DataFrame) -> tuple[tuple]:
    """Transform tables metadata to dds entity table format
    :param _context: airflow DAG task run context
    :param tables: Dataframe['schema_name', 'table_name', 'table_owner', 'estimated_rows',
                             'table_size', 'full_table_size', 'index_json']
    :return: petl.tables(ENTITY_CORE_FIELDS + EntityFields.TABLES, EntityFields.JSON_SYSTEM)
    """
    json_system_builder = JsonSystemBuilder(
        system_for_search=SystemForSearch.POSTGRES.name,
        type_for_search=TypeForSearch.TABLE.name,
        card_type=CardType.TABLE.name,
    )

    table_size_description = TableInfoDescriptionType(
        keys={
            'estimated_rows': 'Rows',
            'table_size': 'Data size',
            'full_table_size': 'Total relation size',
        },
        header='General',
        display_headers='0',
        orientation='vertical',
        serializers={
            'table_size': get_readable_size_bytes,
            'full_table_size': get_readable_size_bytes,
        },
    )
    table_size_builder = TableInfoBuilder(table_size_description)

    table_index_description = TableInfoDescriptionType(
        keys={
            'name': 'Name',
            'ddl': 'Definitions',
        },
        header='Table indexes',
        display_headers='1',
        orientation='horizontal'
    )
    table_index_builder = TableInfoBuilder(table_index_description)

    result = (petl.fromdataframe(tables)
              .addfield(EntityFields.URN,
                        lambda row: get_table('postgres', 'pg', 'airflow', row['schema_name'], row['table_name']))
              .addfield(EntityFields.ENTITY_NAME, lambda row: f"{row['schema_name']}.{row['table_name']}")
              .rename('table_name', EntityFields.ENTITY_NAME_SHORT)
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.TABLE)
              .addfield(EntityFields.SEARCH_DATA,
                        lambda row: f"{row[EntityFields.URN]} {row[EntityFields.ENTITY_NAME]}")
              .addfield(EntityFields.JSON_SYSTEM, json_system_builder())
              .addfield(EntityFields.JSON_DATA,
                        lambda row: dict(estimated_rows=row['estimated_rows'],
                                         table_size=row['table_size'],
                                         full_table_size=row['full_table_size'],
                                         index_json=row['index_json']))
              .addfield(EntityFields.TABLES, lambda row: [table_size_builder(row),
                                                          table_index_builder(row['index_json'] or {})])
              .cut(list(ENTITY_CORE_FIELDS) + [EntityFields.TABLES, EntityFields.JSON_SYSTEM])
              .distinct(key=EntityFields.URN)
              )
    return result.tupleoftuples()


def transform_column_to_entity(_context: dict, columns: DataFrame) -> tuple[tuple]:
    """Transform tables metadata to dds entity table format
    :param _context: airflow DAG task run context
    :param columns: Dataframe['schema_name', 'table_name', 'column_name', 'column_type', 'ordinal_position']
    :return: petl.tables(ENTITY_CORE_FIELDS + EntityFields.TABLES, EntityFields.JSON_SYSTEM)
    """
    json_system_builder = JsonSystemBuilder(
        system_for_search=SystemForSearch.POSTGRES.name,
        type_for_search=TypeForSearch.COLUMN.name,
    )

    column_table_info_description = TableInfoDescriptionType(
        keys={
            'column_type': 'Type',
            'ordinal_position': 'Position',
        },
        header='General',
        display_headers='0',
        orientation='vertical'
    )
    column_table_info_builder = TableInfoBuilder(column_table_info_description)

    result = (petl.fromdataframe(columns)
              .addfield(EntityFields.URN,
                        lambda row: get_column('postgres', 'pg', 'airflow',
                                               row['schema_name'], row['table_name'], row['column_name']))
              .addfield(EntityFields.ENTITY_NAME,
                        lambda row: f"{row['schema_name']}.{row['table_name']}.{row['column_name']}")
              .addfield(EntityFields.ENTITY_NAME_SHORT, lambda row: row['column_name'])
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.COLUMN)
              .addfield(EntityFields.SEARCH_DATA,
                        lambda row: f"{row[EntityFields.URN]} {row[EntityFields.ENTITY_NAME]}")
              .addfield(EntityFields.JSON_DATA,
                        lambda row: dict(ordinal_position=row['ordinal_position'], column_type=row['column_type']))
              .addfield(EntityFields.JSON_SYSTEM, json_system_builder())
              .addfield(EntityFields.TABLES, lambda row: [column_table_info_builder(row)])
              .cut(list(ENTITY_CORE_FIELDS) + [EntityFields.TABLES, EntityFields.JSON_SYSTEM])
              .distinct(key=EntityFields.URN)
              )
    return result.tupleoftuples()


def link_schema_to_table(_context: dict, tables: DataFrame) -> tuple[tuple]:
    """Link schemas entity to tables
    :param _context: airflow DAG task run context
    :param tables: Dataframe['schema_name', 'table_name', 'table_owner', 'estimated_rows',
                             'table_size', 'full_table_size', 'index_json']
    :return: RELATION_CORE_FIELDS
    """
    result = (petl.fromdataframe(tables)
              .addfield(RelationFields.SOURCE,
                        lambda row: get_schema('postgres', 'pg', 'airflow', row['schema_name']))
              .addfield(RelationFields.DESTINATION,
                        lambda row: get_table('postgres', 'pg', 'airflow', row['schema_name'], row['table_name']))
              .addfield(RelationFields.TYPE, RelationTypes.Contains)
              .addfield(RelationFields.ATTRIBUTE, None)
              .cut(list(RELATION_CORE_FIELDS))
              .distinct()
              )
    return result.tupleoftuples()


def link_table_to_column(_context: dict, columns: DataFrame) -> tuple[tuple]:
    """Link tables entity to columns
    :param _context: airflow DAG task run context
    :param columns: Dataframe['schema_name', 'table_name', 'column_name', 'column_type', 'ordinal_position']
    :return: RELATION_CORE_FIELDS
    """
    result = (petl.fromdataframe(columns)
              .addfield(RelationFields.SOURCE,
                        lambda row: get_table('postgres', 'pg', 'airflow', row['schema_name'], row['table_name']))
              .addfield(RelationFields.DESTINATION,
                        lambda row: get_column('postgres', 'pg', 'airflow',
                                               row['schema_name'], row['table_name'], row['column_name']))
              .addfield(RelationFields.TYPE, RelationTypes.Contains)
              .addfield(RelationFields.ATTRIBUTE, None)
              .cut(list(RELATION_CORE_FIELDS))
              .distinct()
              )
    return result.tupleoftuples()


def link_root_node_to_schema(_context: dict, schemas: DataFrame) -> tuple[tuple]:
    """Link schemas to root tree node urn:tree_node:root:database
    :param _context: airflow DAG task run context
    :param schemas: Dataframe['schema_name', 'schema_owner', 'schema_acl', 'schema_description']
    :return: RELATION_CORE_FIELDS
    """
    result = (petl.fromdataframe(schemas)
              .addfield(RelationFields.SOURCE, lambda row: get_tree_node(['Database']))
              .addfield(RelationFields.DESTINATION,
                        lambda row: get_schema('postgres', 'pg', 'airflow', row['schema_name']))
              .addfield(RelationFields.TYPE, RelationTypes.Contains)
              .addfield(RelationFields.ATTRIBUTE, None)
              .cut(list(RELATION_CORE_FIELDS))
              .distinct()
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
        description='Transform schemas metadata to dds.entity',
        source=['dump_pg_schemas'],
        transformer_callable=transform_schema_to_entity,
        dag=t_dag,
    )

    PyTransform(
        task_id='transform_table_to_entity',
        description='Transform tables metadata to dds.entity',
        source=['dump_pg_tables'],
        transformer_callable=transform_table_to_entity,
        dag=t_dag,
    )

    PyTransform(
        task_id='transform_column_to_entity',
        description='Transform columns metadata to dds.entity',
        source=['dump_pg_columns'],
        transformer_callable=transform_column_to_entity,
        dag=t_dag,
    )

    PyTransform(
        task_id='link_schema_to_table',
        description='Link schemas to tables for dds.relation',
        source=['dump_pg_tables'],
        transformer_callable=link_schema_to_table,
        dag=t_dag,
    )

    PyTransform(
        task_id='link_table_to_column',
        description='Link tables to columns for dds.relation',
        source=['dump_pg_columns'],
        transformer_callable=link_table_to_column,
        dag=t_dag,
    )

    PyTransform(
        task_id='link_root_node_to_schema',
        description='Link schemas to root tree node',
        transformer_callable=link_root_node_to_schema,
        source=['dump_pg_schemas'],
        dag=t_dag
    )

    PyTransform(
        task_id='append_entities',
        description='Append entities to load in entity table',
        source=['transform_schema_to_entity',
                'transform_table_to_entity',
                'transform_column_to_entity'],
        transformer_callable=appender_petl2pandas,
        dag=t_dag,
    )

    PyTransform(
        task_id='append_relations',
        description='Append relations to load in relation table',
        source=['link_root_node_to_schema',
                'link_schema_to_table',
                'link_table_to_column'],
        transformer_callable=appender_petl2pandas,
        dag=t_dag,
    )

    PgSingleTargetLoader.upload_dds_entity(dag=t_dag, sources=['append_entities'])
    PgSingleTargetLoader.upload_dds_relation(dag=t_dag, sources=['append_relations'])
