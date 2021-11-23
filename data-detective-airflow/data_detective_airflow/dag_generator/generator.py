import logging
import sys
import traceback
from pathlib import Path
from typing import Generator, Iterable

import yaml
from airflow import settings
from airflow.exceptions import AirflowException

from data_detective_airflow.dag_generator.dags import PythonDag, TDag, YamlDag

FACTORIES = {
    'python': PythonDag,
    'yaml': YamlDag,
}


def dag_generator(
    dag_id_whitelist: Iterable[str] = None,
) -> Generator[TDag, None, None]:
    """DAG Object Generator

    :param dag_id_whitelist: A list of dag_ids to be generated. If empty, then all will be generated.
    """
    dag_id_whitelist = dag_id_whitelist or []
    dags: Iterable[Path] = (p.parent for p in Path(settings.DAGS_FOLDER).rglob('meta.yaml'))
    if dag_id_whitelist:
        dags = filter(lambda dag: dag.name in dag_id_whitelist, dags)
    for dag_path in dags:
        try:
            yield generate_dag(dag_path.as_posix())
        except Exception:  # pylint: disable=broad-except
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.error(
                f'{dag_path.name} is broken with '
                f'{str(traceback.format_exception(exc_type, exc_value, exc_traceback))}'
            )


def generate_dag(dag_dir: str, dag_id: str = None) -> TDag:
    """Get the DAG object by dag_dir
    @param dag_dir: Full path to the directory
    @param dag_id: Dag_id is considered the directory name if dag_path is not specified
    @return:
    """
    dag_path = Path(dag_dir)
    dag_id = dag_id or dag_path.stem

    logging.debug(f'dag_id assumed to be {dag_id}')

    if not dag_id.islower():
        logging.error(f'DAG_id must be lowercase, actual value: {dag_id}')
        return None

    return _get_dag_from_fs(dag_dir)


def _get_dag_from_fs(dag_dir):
    """Get a DAG from a directory on disk"""
    logging.debug(f'Creating dag from {dag_dir}')

    with open(f'{dag_dir}/{TDag.META_FILE}', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    factory = config['factory'].lower()

    logging.debug(f'Dag {dag_dir} by {factory} factory')

    if factory not in FACTORIES:
        AirflowException(f'Unknown value for "factory": {factory} in {dag_dir}')

    logging.debug(f'Creating dag {dag_dir}')
    return FACTORIES[factory.lower()](dag_dir, config=config)
