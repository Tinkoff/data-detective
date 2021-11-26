import json

from pandas import DataFrame, concat


def transform_breadcrumb(_context: dict, df: DataFrame) -> DataFrame:
    """Сформировать цепочку связей
    :param _context: контекст выполнения
    :param df: ['destination', 'source', 'entity_name']
    :return: DataFrame ['urn', 'breadcrumb_urn', 'breadcrumb_entity']
    """
    root = DataFrame()
    root['urn'] = df[df['source'] == 'urn:tree_node:root']['destination']
    root['breadcrumb_urn'] = root['urn'].apply(lambda urn: list())
    root['breadcrumb_entity'] = root['urn'].apply(lambda urn: list())

    result = [root]
    layer_ind = 0
    while not result[layer_ind].empty:
        layer = result[layer_ind].merge(df, left_on='urn', right_on='source')
        layer['urn'] = layer['destination']
        layer['breadcrumb_urn'] = layer['breadcrumb_urn'] + layer['source'].apply(lambda urn: [urn])
        layer['breadcrumb_entity'] = layer['breadcrumb_entity'] + layer['entity_name'].apply(lambda urn: [urn])
        result.append(layer[['urn', 'breadcrumb_urn', 'breadcrumb_entity']])
        layer_ind += 1

    result = concat(result, ignore_index=True)
    result['breadcrumb_urn'] = result['breadcrumb_urn'].apply(json.dumps, ensure_ascii=False)
    result['breadcrumb_entity'] = result['breadcrumb_entity'].apply(json.dumps, ensure_ascii=False)

    return result[['urn', 'breadcrumb_urn', 'breadcrumb_entity']].drop_duplicates(subset='urn')
