from pandas import DataFrame
import petl

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import DBDump, PgSCD1, PyTransform

from common.urn import get_schema
from common.utilities.entity_enums import EntityTypes, EntityFields, RelationTypes
from common.utilities.search_enums import SystemForSearch, TypeForSearch


def transform_schema_to_entity(_context: dict, schemas: DataFrame) -> tuple[tuple]:
    """

    """
    result = (petl.fromdataframe(schemas)
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.SCHEMA)
              .addfield(EntityFields.ENTITY_NAME, lambda row: row['schema_name'].lower())
              .addfield(EntityFields.ENTITY_NAME_SHORT, None)
              .addfield(EntityFields.URN, lambda row: get_schema('postgres',
                                                                 'pg',
                                                                 'airflow',
                                                                 row[EntityFields.ENTITY_NAME])
                        )
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
