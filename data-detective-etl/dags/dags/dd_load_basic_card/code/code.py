from airflow.models.taskinstance import Context
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSingleTargetLoader, PyTransform
from pandas import DataFrame, read_json

from common.utilities.entity_enums import EntityFields, EntityTypes, RelationTypes, RelationFields
from common.urn import get_tree_node


def dump_json(_context: Context, json_file: str) -> DataFrame:
    """Load json with basic card template

    :param _context: Execution context
    :param json_file: basic card json filename
    :return: DataFrame
    """
    data = read_json(json_file, orient='records')
    data[EntityFields.URN] = get_tree_node(['Basic Card'])
    data[EntityFields.ENTITY_NAME] = 'Basic Card'
    data[EntityFields.ENTITY_TYPE] = EntityTypes.TREE_NODE.key
    return data


def link_root_node_to_basic_card(_context: dict) -> DataFrame:
    """Link dags to root tree node urn:tree_node:root:etl_dags

    :param _context: Execution context
    :returns: DataFrame ['source', 'destination', 'type', 'attribute']
    """
    return DataFrame(
        [[get_tree_node(['root', 'Documentation']), get_tree_node(['Basic Card']), RelationTypes.Contains, None]],
        columns=[RelationFields.SOURCE, RelationFields.DESTINATION, RelationFields.TYPE, RelationFields.ATTRIBUTE],
    )


def fill_dag(t_dag: TDag) -> None:
    PyTransform(
        task_id='dump_basic_card',
        description='Load json with basic card template',
        transformer_callable=dump_json,
        op_kwargs={'json_file': f'{t_dag.code_dir}/basic_card.json'},
        dag=t_dag,
    )

    PyTransform(
        task_id='link_root_node_to_basic_card',
        description='Link basic card to root tree node',
        transformer_callable=link_root_node_to_basic_card,
        dag=t_dag,
    )
    PgSingleTargetLoader.upload_dds_relation(dag=t_dag, sources=['link_root_node_to_basic_card'])
    PgSingleTargetLoader.upload_dds_entity(dag=t_dag, sources=['dump_basic_card'])
