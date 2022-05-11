import json
import logging
from functools import cmp_to_key
from hashlib import md5
from typing import Optional

import numpy
from pandas import DataFrame
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import RELATION_KEY_FIELDS, RELATION_NONE, JSON_FIELDS
from data_detective_airflow.utils.notnull import is_not_empty


def _key_cmp(left, right):
    if len(left[0]) != len(right[0]):
        return len(left[0]) - len(right[0])
    if left[0] < right[0]:
        return -1
    if left[0] == right[0]:
        return 0
    return 1


def _ordering_json(js):
    if isinstance(js, dict):
        return {k: _ordering_json(v) for k, v in sorted(js.items(), key=cmp_to_key(_key_cmp))}
    if isinstance(js, list):
        return [_ordering_json(i) for i in js]
    return js


def _reordering_json_str(js: str) -> Optional[str]:
    return json.dumps(_ordering_json(js), ensure_ascii=False) if is_not_empty(js) else None


def filter_for_entity(source_df: DataFrame, context: dict, hook: PostgresHook):
    dump_hash_query = """
                    SELECT urn, md5(
                                            coalesce(json_data::text,'')
                                         || coalesce(info,'')
                                         || coalesce(json_system::text,'')
                                         || coalesce(codes::text,'')
                                         || coalesce(htmls::text,'')
                                         || coalesce(tables::text,'')
                                         || coalesce(notifications::text,'')
                                         || coalesce(json_data_ui::text,'')
                                         || coalesce(grid::text,'')
                                         || coalesce(links::text,'')
                                         || coalesce(tags::text,'')
                                         ) as hash
                    FROM dds.entity
                    WHERE loaded_by = '{dag_urn}'
                """.strip()

    query_params = {
        'dag_urn': context['dag'].dag_id,
    }

    dump_hash_dd = hook.get_pandas_df(dump_hash_query.format(**query_params))
    number_of_rows = len(dump_hash_dd.index)
    dump_hash_dd = dump_hash_dd.drop_duplicates(subset=['urn'])
    number_of_unique_rows = len(dump_hash_dd.index)
    if number_of_rows > number_of_unique_rows:
        logging.warning(f"Duplicate rows in dds.entity DAG {context['dag'].dag_id}")

    json_columns = list(JSON_FIELDS & set(source_df.columns))
    source_df = source_df.replace({numpy.nan: None})
    source_df[json_columns] = source_df[json_columns].applymap(_reordering_json_str)
    source_df['hash'] = source_df \
        .apply(lambda row: (row.get('json_data') or '')
               + (row.get('info') or '')
               + (row.get('json_system') or '')
               + (row.get('codes') or '')
               + (row.get('htmls') or '')
               + (row.get('tables') or '')
               + (row.get('notifications') or '')
               + (row.get('json_data_ui') or '')
               + (row.get('grid') or '')
               + (row.get('links') or '')
               + (row.get('tags') or ''),
               axis=1, result_type='reduce'
               )
    source_df['hash'] = source_df['hash'].apply(lambda hash: md5(hash.encode('utf-8')).hexdigest())
    source_df = source_df.astype(object).merge(dump_hash_dd, how='outer', on=['urn'], indicator='hash_flg')
    del dump_hash_dd
    source_df = source_df[(source_df['hash_flg'] != 'both') | (source_df['hash_x'] != source_df['hash_y'])]
    source_df['diff_flg'] = source_df['hash_flg'].map({'left_only': 'I', 'right_only': 'D', 'both': 'U'})

    return source_df.drop(columns=['hash_flg', 'hash_x', 'hash_y'])


def filter_for_breadcrumb(source_df, context: dict, hook: PostgresHook):
    dump_hash_query = """
                    SELECT DISTINCT urn,
                                    md5(coalesce(breadcrumb_urn::text,'')
                                    || coalesce(breadcrumb_entity::text,'')) as hash
                    FROM tuning.breadcrumb
                    WHERE loaded_by = '{dag_urn}'
                """.strip()

    query_params = {
        'dag_urn': context['dag'].dag_id,
    }

    dump_hash_dd = hook.get_pandas_df(dump_hash_query.format(**query_params))

    source_df['hash'] = source_df \
        .apply(lambda row: (row['breadcrumb_urn'] if is_not_empty(row['breadcrumb_urn']) else '') +
               (row['breadcrumb_entity'] if is_not_empty(row['breadcrumb_entity']) else ''),
               axis=1, result_type='reduce'
               )
    source_df['hash'] = source_df['hash'].apply(lambda x: md5(x.encode('utf-8')).hexdigest())
    source_df = source_df.astype(object)\
        .merge(dump_hash_dd, how='outer', on=['urn'], indicator='hash_flg')
    del dump_hash_dd
    source_df = source_df[(source_df['hash_flg'] != 'both') | (source_df['hash_x'] != source_df['hash_y'])]
    source_df['diff_flg'] = source_df['hash_flg'].map({'left_only': 'I', 'right_only': 'D', 'both': 'U'})

    return source_df.drop(columns=['hash_flg', 'hash_x', 'hash_y'])


def filter_for_relation(source_df: DataFrame, context: dict, hook: PostgresHook):
    dump_query = \
        """
    SELECT DISTINCT
        source
        ,destination
        ,attribute
        ,type
    FROM dds.relation
    WHERE loaded_by = '{dag_urn}'
        """.strip()

    query_params = {
        'non': RELATION_NONE,
        'dag_urn': context['dag'].dag_id,
    }

    dump_mg = hook.get_pandas_df(dump_query.format(**query_params))
    source_df = source_df.fillna(
        value={key: RELATION_NONE for key in RELATION_KEY_FIELDS}
    )

    source_df = source_df.astype(object).merge(dump_mg, how='outer', on=list(RELATION_KEY_FIELDS), indicator='hash_flg')
    del dump_mg
    source_df = source_df[(source_df['hash_flg'] != 'both') | (source_df['type_x'] != source_df['type_y'])]
    source_df['diff_flg'] = source_df['hash_flg'].map({'left_only': 'I', 'right_only': 'D', 'both': 'U'})
    source_df.rename(columns={'type_x': 'type'}, inplace=True)
    return source_df.drop(columns=['hash_flg', 'type_y'])
