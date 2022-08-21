import json
from fnmatch import fnmatch
from pathlib import Path
from typing import Union

import pandas as pd
import yaml
from airflow.models.taskinstance import Context
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook
from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import DBDump, PyTransform


def _apply_rank(entity: pd.Series, rank_groups: list[dict[str, str]]) -> Union[str, list[str], float, int]:
    """Apply search rank for every entity
    If more then one rank for entity choose max rank
    If rank not found set rank 1
    :param entity: dds.entity
    :param rank_groups: search rank group for entity
    :return: search rank
    """
    ranks = []

    entity_name = entity['id']
    entity_type = entity['entity_type']

    for rank_group in rank_groups:
        entity_mask_rank_group = rank_group['entity_name']
        entity_type_rank_group = rank_group['entity_type']

        if fnmatch(entity_name, entity_mask_rank_group) is True and entity_type == entity_type_rank_group:
            ranks.append(rank_group['rank'])

    if ranks:
        return max(ranks)

    return 1


def apply_rank_to_entities(context: Context, source: pd.DataFrame) -> pd.DataFrame:
    """Apply search rank to entities
    :param context: Task running context
    :param source: dds.entity
    :return: dds.entity with rank
    """

    entities_with_weights = source

    rank_file_dir = Path(f"{context['dag'].etc_dir}/{'entity_rank.yml'}")
    rank_file = yaml.safe_load(rank_file_dir.read_text(encoding='utf-8'))
    exploded_rank_file = (
        pd.DataFrame.from_dict(rank_file)[['entity_name', 'entity_type', 'rank']]
        .explode('entity_type')
        .explode('entity_name')
    )
    exploded_dicts = pd.DataFrame.to_dict(exploded_rank_file, orient='records')
    entities_with_weights['rank'] = entities_with_weights.apply(lambda row: _apply_rank(row, exploded_dicts), axis=1)

    return entities_with_weights


def upload_dd_search(_context: Context, source: pd.DataFrame, conn_id: str, index_name: str, chunk_size: int) -> None:
    """Load search data to ElasticSearch
    :param _context: Task running context
    :param source: dds.entity with rank
    :param conn_id: ElasticSearch connection id
    :param index_name: ElasticSearch index to load search data
    :param chunk_size: Number of entity to load with one ElasticSearch bulk operations
    :return:
    """

    es_hook = ElasticsearchSQLHook(elasticsearch_conn_id=conn_id)
    elastic = es_hook.get_conn().es

    shard_num = 0
    chunk_num = len(source.index) // chunk_size
    while shard_num < chunk_num:
        partly_df = source.head(chunk_size).to_dict(orient='records')
        source.drop(source.head(chunk_size).index, inplace=True)
        body = ''
        for item in partly_df:
            body += json.dumps({'index': {'_index': index_name, '_id': item['id']}}) + '\n'
            body += json.dumps(item) + '\n'
        elastic.bulk(body)
        shard_num += 1

    partly_df = source.to_dict(orient='records')
    body = ''
    for item in partly_df:
        body += json.dumps({'index': {'_index': index_name, '_id': item['id']}}) + '\n'
        body += json.dumps(item) + '\n'
    elastic.bulk(body)


def fill_dag(t_dag: TDag) -> None:

    DBDump(
        task_id='dump_search_data',
        description='Dump data from DD pg_base for search',
        conn_id=PG_CONN_ID,
        sql='/code/dump_search_data.sql',
        dag=t_dag,
    )

    PyTransform(
        task_id='apply_rank_to_entities',
        description='Apply search rank to entities',
        source=['dump_search_data'],
        transformer_callable=apply_rank_to_entities,
        dag=t_dag,
    )

    PyTransform(
        task_id='upload_dd_search',
        description='Load search data to ElasticSearch',
        source=['apply_rank_to_entities'],
        transformer_callable=upload_dd_search,
        op_kwargs={
            'conn_id': 'dd-search',
            'index_name': 'dd-search',
            'chunk_size': 10_000,
        },
        dag=t_dag,
    )
