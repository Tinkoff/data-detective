from pathlib import Path

from airflow.models import DagBag
import petl

from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSingleTargetLoader, PyTransform
from data_detective_airflow.utils.petl_utils import appender_petl2pandas

from common.builders import JsonSystemBuilder, CodeBuilder
from common.utilities.entity_enums import (
    ENTITY_CORE_FIELDS,
    EntityTypes, EntityFields
)
from common.urn import get_etl_job
from common.utilities.search_enums import SystemForSearch, TypeForSearch


def _get_file_content(filename: Path) -> str:
    """Read file contents from Path

    :param filename: Path style file_name
    :return: contents of file in text mode
    """
    with filename.open(mode='r+t', encoding='utf-8') as file:
        return file.read()


def _get_code_files(path: str, code_type: str, exclude_files: list = None) -> dict:
    """Get contents of DAG code files

    :param path: str name of absolute path to get code files
    :param code_type: str suffix for code files (like *.py, *.sql, *.yaml)
    :param exclude_files: list of relative path to exclude files
    :return: list of dict [{'name': 'relative path to file name
                            'type': 'suffix from code_type'
                            'data': 'contents of file in text mode':}]
    """
    pathname = Path(path)
    if not exclude_files:
        exclude_files = []

    result = [{'name': str(code_file.relative_to(pathname)),
               'type': code_type,
               'data': _get_file_content(code_file)}
              for code_file in pathname.glob(f'**/*.{code_type}')
              if code_file not in [pathname / ex_file for ex_file in exclude_files]]

    return result


def get_list_of_dags(_context: dict) -> tuple[tuple]:
    """Get list of dags from airflow.models DagBag

    :param _context: airflow DAG task run context
    :return: tuple_of_tuples dags metadata ('dag_id', 'dag_dir', 'factory', 'schedule_interval', 'description',
                                            'default_args', 'tags', 'tasks')
    """
    dag_info = ['dag_id', 'dag_dir', 'factory', 'schedule_interval', 'description', 'default_args', 'tags']

    dag_list = [dag_info + ['tasks']] + [
        ([getattr(dag, info, None) for info in dag_info] + [[task.task_id for task in dag.tasks]])
        for dag in DagBag().dags.values()
    ]

    return petl.wrap(dag_list).tupleoftuples()


def add_code_files_to_dags(_context: dict, dags: tuple[tuple]) -> tuple[tuple]:
    """Add code file contents to dags info

    :param _context: airflow DAG task run context
    :param dags: tuple_of_tuples dags metadata ('dag_id', 'dag_dir', 'factory', 'schedule_interval', 'description',
                                                'default_args', 'tags', 'tasks')
    :return: dags + ('meta_yaml', 'yaml_files', 'py_files', 'sql_files')
    """
    result = (petl.wrap(dags)
              .addfield('meta_yaml', lambda row: _get_file_content(Path(row['dag_dir']) / 'meta.yaml'))
              .addfield('yaml_files', lambda row: _get_code_files(row['dag_dir'], 'yaml', exclude_files=['meta.yaml']))
              .addfield('py_files', lambda row: _get_code_files(row['dag_dir'], 'py'))
              .addfield('sql_files', lambda row: _get_code_files(row['dag_dir'], 'sql'))
              )

    return result.tupleoftuples()


def transform_dag_to_entity(_context: dict, dags: tuple[tuple]) -> tuple[tuple]:
    """Transform DAGs metadata to dds entity table format

    :param _context: airflow DAG task run context
    :param dags: dags metadata ('dag_id', 'dag_dir', 'factory', 'schedule_interval', 'description',
                                'default_args', 'tags', 'tasks', 'meta_yaml', 'yaml_files', 'py_files', 'sql_files')
    :return: [ENTITY_CORE_FIELDS] + [EntityFields.JSON_SYSTEM, EntityFields.INFO, EntityFields.CODES]
    """
    json_system_builder = JsonSystemBuilder(
        system_for_search=SystemForSearch.DATA_DETECTIVE.name,
        type_for_search=TypeForSearch.JOB.name,
    )

    meta_yaml_code_builder = CodeBuilder(header='DAG main meta.yaml', opened=True, language='yaml')
    dag_code_builder = CodeBuilder(opened=False)

    result = (petl.wrap(dags)
              .addfield(EntityFields.URN, lambda row: get_etl_job('dd', 'airflow', row['dag_id']))
              .addfield(EntityFields.ENTITY_NAME, lambda row: row['dag_id'])
              .addfield(EntityFields.ENTITY_NAME_SHORT, None)
              .addfield(EntityFields.ENTITY_TYPE, EntityTypes.JOB)
              .addfield(EntityFields.JSON_SYSTEM, json_system_builder())
              .addfield(EntityFields.SEARCH_DATA,
                        lambda row: f"{row[EntityFields.URN]} {row[EntityFields.ENTITY_NAME]}")
              .addfield(EntityFields.JSON_DATA, lambda row: dict(factory=row['factory'],
                                                                 default_args=row['default_args'],
                                                                 schedule_interval=row['schedule_interval'],
                                                                 tasks=row['tasks'],
                                                                 meta_yaml=row['meta_yaml'],
                                                                 tags=row['tags']))
              .addfield(EntityFields.CODES,
                        lambda row: (
                            [meta_yaml_code_builder(data=row['meta_yaml'])] +
                            [dag_code_builder(header=code['name'], language=code['type'], data=code['data'])
                             for code in row['yaml_files']] +
                            [dag_code_builder(header=code['name'], language=code['type'], data=code['data'])
                             for code in row['py_files']] +
                            [dag_code_builder(header=code['name'], language=code['type'], data=code['data'])
                             for code in row['sql_files']]
                        )
                        )
              .rename('description', EntityFields.INFO)
              .rename('tags', EntityFields.TAGS)
              # TAGS removed because of bag in dd-airflow JSON_FIELDS, need to add after dd-airflow release
              .cut(list(ENTITY_CORE_FIELDS) + [EntityFields.JSON_SYSTEM, EntityFields.INFO, EntityFields.CODES])
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
        task_id='add_code_files_to_dags',
        description='Add code file contents to dags info',
        source=['get_list_of_dags'],
        transformer_callable=add_code_files_to_dags,
        dag=t_dag,
    )

    PyTransform(
        task_id='transform_dag_to_entity',
        description='Transform dags metadata to dds.entity',
        source=['add_code_files_to_dags'],
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
