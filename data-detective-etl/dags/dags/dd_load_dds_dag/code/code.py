import os
from pathlib import Path
import logging

from airflow.models import DagBag
from airflow.settings import DAGS_FOLDER
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
from common.urn import get_etl_job
from common.utilities.search_enums import SystemForSearch, TypeForSearch

# change to AIRFLOW_HOME
DAGS_BASE_PATH = '/usr/local/airflow/dags/dags'


def get_list_of_dags(_context: dict) -> tuple[tuple]:
    """
            dag_dir=dag_dir,
            dag_id=Path(dag_dir).name,
            factory=self.config['factory'],
            start_date=DEFAULT_START_DATE,
            schedule_interval=self.config['schedule_interval'],
            description=self.config.get('description', ''),
            default_args=self.config['default_args'],
            template_searchpath=dag_dir,
            tags=self.config.get('tags'),
    """
    dag_info = ['dag_id', 'dag_dir', 'factory', 'schedule_interval', 'description', 'default_args', 'tags']

    dag_list = [dag_info] + [[getattr(dag, info, None) for info in dag_info] for dag in DagBag().dags.values()]
    logging.info(f"\n{[dag for dag in DagBag().dags.values()]}\n")
    result = petl.wrap(dag_list)

    return result.tupleoftuples()


def transform_dag_to_entity(_context: dict, dags: tuple[tuple]) -> tuple[tuple]:
    """Transform .... to dds entity table format
    :param _context: airflow DAG task run context
    :param dags:
    :return: petl.tables()
    """
    json_system_builder = JsonSystemBuilder(
        system_for_search=SystemForSearch.DATA_DETECTIVE.name,
        type_for_search=TypeForSearch.JOB.name,
    )

    result = (petl.wrap(dags)
              .addfield(EntityFields.URN, lambda row: get_etl_job('dd', 'airflow', row['dag_id']))
              .addfield(EntityFields.ENTITY_NAME, lambda row: row['dag_id'])
              .addfield(EntityFields.ENTITY_NAME_SHORT, None)
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.JOB)
              .addfield(EntityFields.JSON_SYSTEM, json_system_builder())
              .addfield(EntityFields.SEARCH_DATA,
                        lambda row: f"{row[EntityFields.URN]} {row[EntityFields.ENTITY_NAME]}")
              .addfield(EntityFields.JSON_DATA, None)
              .rename('description', EntityFields.INFO)
              # .rename('tags', EntityFields.TAGS)
              .cut(list(ENTITY_CORE_FIELDS) + [EntityFields.JSON_SYSTEM, EntityFields.INFO, EntityFields.TAGS])
              .distinct(key=EntityFields.URN)
              )
    return result.tupleoftuples()


def fill_dag(t_dag: TDag):
    PyTransform(
        task_id='get_list_of_dags',
        description='Get list of DAGs from airflow DagBag',
        transformer_callable=get_list_of_dags,
        dag=t_dag,
    )

    PyTransform(
        task_id='transform_dag_to_entity',
        description='Transform dags metadata to dds.entity',
        source=['get_list_of_dags'],
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
