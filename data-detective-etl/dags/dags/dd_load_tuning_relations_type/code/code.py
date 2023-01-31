import pandas


def dump_relations_types(context: dict, file_name: str) -> pandas.DataFrame:
    """Get entities for tree_node from root_nodes.yaml
    :param context: Execution context
    :param file_name: File name
    :return: DataFrame ['source_type', 'target_type', 'attribute_type', 'relation_type',
                        'source_group_name', 'target_group_name', 'attribute_group_name',
                        'loaded_by']
    """

    data = pandas.read_csv(f"{context['dag'].etc_dir}/{file_name}", sep='|', skiprows=[1])

    for col in data.columns.tolist():
        data[col] = data[col].str.strip()

    data = data[(data['source_group_code'].str.len() > 0)
                | (data['target_group_code'].str.len() > 0)
                | (data['attribute_group_code'].str.len() > 0)]
    data['loaded_by'] = context['dag'].dag_id

    return data[['source_type', 'target_type', 'attribute_type', 'relation_type',
                 'source_group_code', 'target_group_code', 'attribute_group_code',
                 'loaded_by']]
