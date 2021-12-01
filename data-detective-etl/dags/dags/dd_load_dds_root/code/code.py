from io import StringIO
from pathlib import Path

import yaml
from pandas import DataFrame

from common.urn import get_tree_node
from common.utilities.entity_enums import EntityTypes, RelationTypes


def walk_relations(nodes: dict, source: tuple[str] = None) -> dict:
    """Observe root_nodes with outputting relationships
    :param nodes: Nested hierarchical dictionary of tree_node
    :param source: List of parents for tree_node
    :return: Dict
    """
    for key, value in nodes.items():
        path = source + (key, ) if source and key != 'root' else (key, )
        if 'contains' in value:
            yield from walk_relations(value['contains'], path)
        yield {'source': source, 'destination': path}


def walk_entities(nodes: dict, source: tuple[str] = None) -> dict:
    """Observe root_nodes with listing entities and their attributes
    :param nodes: Nested hierarchical dictionary of tree_node
    :param source: List of parents for tree_node
    :return: Dict
    """
    for key, value in nodes.items():
        path = source + (key, ) if source and key != 'root' else (key, )
        if 'contains' in value:
            yield from walk_entities(value['contains'], path)
        res = {'path': path}
        json_data = {k: v for k, v in value.items() if k != 'contains'}
        res.update({'json_data': json_data})
        yield res


def dump_root_nodes_entities(context: dict, file_name: str) -> DataFrame:
    """Get entities for tree_node from root_nodes.yaml
    :param context: Execution context
    :param file_name: File name
    :return: DataFrame
    """
    raw = yaml.safe_load(StringIO(Path(f'{context["dag"].etc_dir}/{file_name}').read_text()))
    root_nodes = DataFrame.from_dict(walk_entities(raw))
    root_nodes['urn'] = root_nodes.apply(
        lambda row: get_tree_node(row['path']),
        axis=1
    )
    root_nodes['entity_name'] = root_nodes['path'].apply(lambda path: path[-1])
    root_nodes['loaded_by'] = context['dag'].dag_id
    root_nodes['entity_type'] = EntityTypes.TREE_NODE
    root_nodes['entity_name_short'] = None
    root_nodes['search_data'] = root_nodes['urn'] + ' ' + root_nodes['entity_name'].str.lower()
    return root_nodes[['urn', 'entity_name', 'loaded_by', 'entity_type',
                       'json_data', 'entity_name_short', 'search_data']]


def dump_root_nodes_relations(context: dict, file_name: str) -> DataFrame:
    """Get relations between tree_node from root_nodes.yaml
    :param context: Execution context
    :param file_name: File name
    :return: DataFrame
    """
    raw = yaml.safe_load(StringIO(Path(f'{context["dag"].etc_dir}/{file_name}').read_text()))
    root_nodes = DataFrame.from_dict(walk_relations(raw))

    root_nodes = root_nodes[~root_nodes['source'].isnull()]

    root_nodes['source'] = root_nodes.apply(
        lambda row: get_tree_node(row['source']),
        axis=1
    )
    root_nodes['destination'] = root_nodes.apply(
        lambda row: get_tree_node(row['destination']),
        axis=1
    )
    root_nodes['type'] = RelationTypes.Contains
    root_nodes['loaded_by'] = context['dag'].dag_id
    root_nodes['attribute'] = None

    return root_nodes[['source', 'destination', 'type', 'loaded_by', 'attribute']]
