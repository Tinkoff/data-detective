import pytest

from pandas import DataFrame
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_detective_airflow.constants import PG_CONN_ID, RELATION_NONE

dataset = {
    'source_dds.entity': DataFrame(
        [
            ['urn:schema:clickhouse:dwh:ch:s_stg', 's_stg', 'SCHEMA', {"engine": "Ordinary"}, None, 'urn:schema:clickhouse:dwh:ch:s_stg s_stg'],
            ['urn:schema:clickhouse:dwh:ch:stg', 'stg', 'SCHEMA', {"engine": "Ordinary"}, None, 'urn:schema:clickhouse:dwh:ch:stg stg'],
            ['urn:schema:clickhouse:dwh:ch:usermart', 'usermart', 'SCHEMA', {"engine": "Ordinary"}, None, 'urn:schema:clickhouse:dwh:ch:usermart usermart'],
            ['urn:schema:clickhouse:dwh:ch:v_stg', 'v_stg', 'SCHEMA', {"engine": "Ordinary"}, None, 'urn:schema:clickhouse:dwh:ch:v_stg v_stg'],
            ['urn:schema:clickhouse:dwh:ch:v_usermart', 'v_usermart', 'SCHEMA', {"engine": "Ordinary"}, None, 'urn:schema:clickhouse:dwh:ch:v_usermart v_usermart'],
            ['urn:tree_node:zeppelin', 'Zeppelin', 'tree_node', {"info": "Раздел с публичным ноутами из Zeppelin"}, None, 'urn:tree_node:zeppelin zeppelin'],
        ],
        columns=['urn', 'entity_name', 'entity_type', 'json_data', 'entity_name_short', 'search_data']),
    'expected_dds.entity_True_empty': DataFrame(
        [
            ['urn:schema:clickhouse:dwh:ch:s_stg', 's_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:s_stg s_stg'],
            ['urn:schema:clickhouse:dwh:ch:stg', 'stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:stg stg'],
            ['urn:schema:clickhouse:dwh:ch:usermart', 'usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:usermart usermart'],
            ['urn:schema:clickhouse:dwh:ch:v_stg', 'v_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_stg v_stg'],
            ['urn:schema:clickhouse:dwh:ch:v_usermart', 'v_usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_usermart v_usermart'],
            ['urn:tree_node:zeppelin', 'Zeppelin', 'test_pg_single_table_loader_main', 'tree_node', {'info': 'Раздел с публичным ноутами из Zeppelin'}, None, 'urn:tree_node:zeppelin zeppelin']
        ],
        columns=['urn', 'entity_name', 'loaded_by', 'entity_type', 'json_data', 'entity_name_short', 'search_data']),
    'expected_dds.entity_False_empty': DataFrame(
        [
            ['urn:schema:clickhouse:dwh:ch:s_stg', 's_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:s_stg s_stg'],
            ['urn:schema:clickhouse:dwh:ch:stg', 'stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:stg stg'],
            ['urn:schema:clickhouse:dwh:ch:usermart', 'usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:usermart usermart'],
            ['urn:schema:clickhouse:dwh:ch:v_stg', 'v_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_stg v_stg'],
            ['urn:schema:clickhouse:dwh:ch:v_usermart', 'v_usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_usermart v_usermart'],
            ['urn:tree_node:zeppelin', 'Zeppelin', 'test_pg_single_table_loader_main', 'tree_node', {'info': 'Раздел с публичным ноутами из Zeppelin'}, None, 'urn:tree_node:zeppelin zeppelin']
        ],
        columns=['urn', 'entity_name', 'loaded_by', 'entity_type', 'json_data', 'entity_name_short', 'search_data']),
    'expected_dds.entity_True_not_empty': DataFrame(
        [
            ['urn:schema:clickhouse:dwh:ch:stg', 'stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:stg stg'],
            ['urn:schema:clickhouse:dwh:ch:v_stg', 'v_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_stg v_stg'],
            ['urn:schema:clickhouse:dwh:ch:s_stg', 's_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:s_stg s_stg'],
            ['urn:schema:clickhouse:dwh:ch:usermart', 'usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:usermart usermart'],
            ['urn:schema:clickhouse:dwh:ch:v_usermart', 'v_usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_usermart v_usermart'],
            ['urn:tree_node:zeppelin', 'Zeppelin', 'test_pg_single_table_loader_main', 'tree_node', {'info': 'Раздел с публичным ноутами из Zeppelin'}, None, 'urn:tree_node:zeppelin zeppelin']
        ],
        columns=['urn', 'entity_name', 'loaded_by', 'entity_type', 'json_data', 'entity_name_short', 'search_data']),
    'expected_dds.entity_False_not_empty': DataFrame(
        [
            ['urn:schema:clickhouse:dwh:ch:test_s_stg', 'test_s_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:test_s_stg test_s_stg', None],
            ['urn:schema:clickhouse:dwh:ch:stg', 'stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:stg stg', None],
            ['urn:schema:clickhouse:dwh:ch:v_stg', 'v_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_stg v_stg', None],
            ['urn:schema:clickhouse:dwh:ch:s_stg', 's_stg', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:s_stg s_stg', None],
            ['urn:schema:clickhouse:dwh:ch:usermart', 'usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:usermart usermart', None],
            ['urn:schema:clickhouse:dwh:ch:v_usermart', 'v_usermart', 'test_pg_single_table_loader_main', 'SCHEMA', {'engine': 'Ordinary'}, None, 'urn:schema:clickhouse:dwh:ch:v_usermart v_usermart', None],
            ['urn:tree_node:zeppelin', 'Zeppelin', 'test_pg_single_table_loader_main', 'tree_node', {'info': 'Раздел с публичным ноутами из Zeppelin'}, None, 'urn:tree_node:zeppelin zeppelin', None]
        ],
        columns=['urn', 'entity_name', 'loaded_by', 'entity_type', 'json_data', 'entity_name_short', 'search_data', 'json_system']),
    'source_dds.relation': DataFrame(
        [
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:s_stg', 'Contains', None],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:stg', 'Contains', None],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:usermart', 'Contains', None],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_stg', 'Contains', None],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_usermart', 'Contains', None]
        ],
        columns=['source', 'destination', 'type', 'attribute']),
    'expected_dds.relation_True_empty': DataFrame(
        [
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:s_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE]
        ],
        columns=['source', 'destination', 'type', 'loaded_by', 'attribute']),
    'expected_dds.relation_False_empty': DataFrame(
        [
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:s_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE]
        ],
        columns=['source', 'destination', 'type', 'loaded_by', 'attribute']),
    'expected_dds.relation_True_not_empty': DataFrame(
        [
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:s_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE]
        ],
        columns=['source', 'destination', 'type', 'loaded_by', 'attribute']),
    'expected_dds.relation_False_not_empty': DataFrame(
        [
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:test_s_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:s_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_usermart', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE]
        ],
        columns=['source', 'destination', 'type', 'loaded_by', 'attribute']),
    'source_tuning.breadcrumb': DataFrame(
        [
            ['urn:tree_node:hive', '["urn:tree_node:data_platform", "urn:tree_node:physical_model"]',
             '["Data Platform", "Physical model"]'],
            ['urn:tree_node:clickhouse', '["urn:tree_node:data_platform", "urn:tree_node:physical_model"]',
             '["Data Platform", "Physical model"]'],
            ['urn:column:greenplum:dwh:gp:riskmart:feature:processed_dttm',
             '["urn:tree_node:data_platform", "urn:tree_node:physical_model", "urn:tree_node:greenplum", "urn:schema:greenplum:dwh:gp:riskmart", "urn:table:greenplum:dwh:gp:riskmart:feature"]',
             '["Data Platform", "Physical model", "GrEEenpluM", "riskmart", "feature"]'],
            ['urn:job:replication:oracle:abc',
             '["urn:tree_node:data_platform", "urn:tree_node:replication_tasks", "urn:tree_node:oracle_tasks", "urn:tree_node:oracle:dwh", "urn:tree_node:oracle:dwh:abc"]',
             '["Data Platform", "Replication tasks", "Oracle tasks", "ABC"]'],

        ],
        columns=['urn', 'breadcrumb_urn', 'breadcrumb_entity']),
    'expected_tuning.breadcrumb_True_empty': DataFrame(
        [
            ['urn:tree_node:hive', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:clickhouse', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:column:greenplum:dwh:gp:riskmart:feature:processed_dttm', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model', 'urn:tree_node:greenplum', 'urn:schema:greenplum:dwh:gp:riskmart', 'urn:table:greenplum:dwh:gp:riskmart:feature'], ['Data Platform', 'Physical model', 'GrEEenpluM', 'riskmart', 'feature'], 'test_pg_single_table_loader_main'],
            ['urn:job:replication:oracle:abc', ['urn:tree_node:data_platform', 'urn:tree_node:replication_tasks', 'urn:tree_node:oracle_tasks', 'urn:tree_node:oracle:dwh', 'urn:tree_node:oracle:dwh:abc'], ['Data Platform', 'Replication tasks', 'Oracle tasks', 'ABC'], 'test_pg_single_table_loader_main']
        ],
        columns=['urn', 'breadcrumb_urn', 'breadcrumb_entity', 'loaded_by']),
    'expected_tuning.breadcrumb_False_empty': DataFrame(
        [
            ['urn:tree_node:hive', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:clickhouse', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:column:greenplum:dwh:gp:riskmart:feature:processed_dttm', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model', 'urn:tree_node:greenplum', 'urn:schema:greenplum:dwh:gp:riskmart', 'urn:table:greenplum:dwh:gp:riskmart:feature'], ['Data Platform', 'Physical model', 'GrEEenpluM', 'riskmart', 'feature'], 'test_pg_single_table_loader_main'],
            ['urn:job:replication:oracle:abc', ['urn:tree_node:data_platform', 'urn:tree_node:replication_tasks', 'urn:tree_node:oracle_tasks', 'urn:tree_node:oracle:dwh', 'urn:tree_node:oracle:dwh:abc'], ['Data Platform', 'Replication tasks', 'Oracle tasks', 'ABC'], 'test_pg_single_table_loader_main']
        ],
        columns=['urn', 'breadcrumb_urn', 'breadcrumb_entity', 'loaded_by']),
    'expected_tuning.breadcrumb_True_not_empty': DataFrame(
        [
            ['urn:job:replication:oracle:abc', ['urn:tree_node:data_platform', 'urn:tree_node:replication_tasks', 'urn:tree_node:oracle_tasks', 'urn:tree_node:oracle:dwh', 'urn:tree_node:oracle:dwh:abc'], ['Data Platform', 'Replication tasks', 'Oracle tasks', 'ABC'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:hive', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:clickhouse', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:column:greenplum:dwh:gp:riskmart:feature:processed_dttm', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model', 'urn:tree_node:greenplum', 'urn:schema:greenplum:dwh:gp:riskmart', 'urn:table:greenplum:dwh:gp:riskmart:feature'], ['Data Platform', 'Physical model', 'GrEEenpluM', 'riskmart', 'feature'], 'test_pg_single_table_loader_main']
        ],
        columns=['urn', 'breadcrumb_urn', 'breadcrumb_entity', 'loaded_by']),

    'expected_tuning.breadcrumb_False_not_empty': DataFrame(
        [
            ['urn:job:replication:oracle:abc', ['urn:tree_node:data_platform', 'urn:tree_node:replication_tasks', 'urn:tree_node:oracle_tasks', 'urn:tree_node:oracle:dwh', 'urn:tree_node:oracle:dwh:abc'], ['Data Platform', 'Replication tasks', 'Oracle tasks', 'ABC'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:postgres_tasks', ['urn:tree_node:data_platform', 'urn:tree_node:replication_tasks'], ['Data Platform', 'Replication tasks'], 'test_pg_single_table_loader_main'],
            ['urn:schema:greenplum:dwh:gp:riskmart', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model', 'urn:tree_node:greenplum'], ['Data Platform', 'Physical model', 'Greenplum'], 'test_pg_single_table_loader_main'],
            ['urn:table:oracle:dwh:abc:def:foo', ['urn:tree_node:databases', 'urn:tree_node:oracle', 'urn:database:oracle:dwh:abc', 'urn:schema:oracle:dwh:abc:def'], ['Databases', 'Oracle', 'DWH', 'ABC'], 'test_pg_single_table_loader_main'],
            ['urn:table:greenplum:dwh:gp:emart:agents', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model', 'urn:tree_node:greenplum', 'urn:schema:greenplum:dwh:gp:emart'], ['Data Platform', 'Physical model', 'Greenplum', 'emart'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:hive', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:tree_node:clickhouse', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model'], ['Data Platform', 'Physical model'], 'test_pg_single_table_loader_main'],
            ['urn:column:greenplum:dwh:gp:riskmart:feature:processed_dttm', ['urn:tree_node:data_platform', 'urn:tree_node:physical_model', 'urn:tree_node:greenplum', 'urn:schema:greenplum:dwh:gp:riskmart', 'urn:table:greenplum:dwh:gp:riskmart:feature'], ['Data Platform', 'Physical model', 'GrEEenpluM', 'riskmart', 'feature'], 'test_pg_single_table_loader_main']
        ],
        columns=['urn', 'breadcrumb_urn', 'breadcrumb_entity', 'loaded_by']),
}

setup_dataset = {
    'dds.entity': DataFrame(
        [
            ['urn:schema:clickhouse:dwh:ch:test_s_stg', 'test_s_stg', 'SCHEMA', '{"engine": "Ordinary"}', None, 'urn:schema:clickhouse:dwh:ch:test_s_stg test_s_stg', 'test_pg_single_table_loader_main'], #  должна будет удалиться если отслеживаются удаления
            ['urn:schema:clickhouse:dwh:ch:s_stg', 's_stg', 'SCHEMA', '{"engine": "NOT_Ordinary"}', None, 'urn:schema:clickhouse:dwh:ch:s_stg s_stg', 'test_pg_single_table_loader_main'], #  должна будет обновиться
            ['urn:schema:clickhouse:dwh:ch:stg', 'stg', 'SCHEMA', '{"engine": "Ordinary"}', None, 'urn:schema:clickhouse:dwh:ch:stg stg', 'test_pg_single_table_loader_main'],
            ['urn:schema:clickhouse:dwh:ch:v_stg', 'v_stg', 'SCHEMA', '{"engine": "Ordinary"}', None, 'urn:schema:clickhouse:dwh:ch:v_stg v_stg', 'test_pg_single_table_loader_main'],
            ['urn:tree_node:zeppelin', 'Zeppelin', 'tree_node', '{"info": "Раздел с публичным ноутами из Zeppelin"}', None, 'urn:tree_node:zeppelin zeppelin','test_pg_single_table_loader_main'] #  должна будет обновиться
        ],
        columns=['urn', 'entity_name', 'entity_type', 'json_data', 'entity_name_short', 'search_data', 'loaded_by']),
    'dds.relation': DataFrame(
        [
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:test_s_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],  # должна будет удалиться если отслеживаются удаления
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:s_stg', 'Includes', 'test_pg_single_table_loader_main', RELATION_NONE],  # должна будет обновиться
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE],
            ['urn:tree_node:clickhouse', 'urn:schema:clickhouse:dwh:ch:v_stg', 'Contains', 'test_pg_single_table_loader_main', RELATION_NONE]
        ],
        columns=['source', 'destination', 'type', 'loaded_by', 'attribute']),
    'tuning.breadcrumb': DataFrame(
        [
            ["urn:column:greenplum:dwh:gp:riskmart:feature:processed_dttm",
             '["urn:tree_node:data_platform", "urn:tree_node:physical_model", "urn:tree_node:greenplum", "urn:schema:greenplum:dwh:gp:riskmart", "urn:table:greenplum:dwh:gp:riskmart:feature"]',
             '["Data Platform", "Physical model", "Greenplum", "riskmart", "feature"]',
             "test_pg_single_table_loader_main"],  # должна будет обновиться
            ["urn:job:replication:oracle:abc",
             '["urn:tree_node:data_platform", "urn:tree_node:replication_tasks", "urn:tree_node:oracle_tasks", "urn:tree_node:oracle:dwh", "urn:tree_node:oracle:dwh:abc"]',
             '["Data Platform", "Replication tasks", "Oracle tasks", "ABC"]',
             "test_pg_single_table_loader_main"],
            ["urn:tree_node:postgres_tasks", '["urn:tree_node:data_platform", "urn:tree_node:replication_tasks"]',
             '["Data Platform", "Replication tasks"]',"test_pg_single_table_loader_main"],
            ["urn:schema:greenplum:dwh:gp:riskmart",
             '["urn:tree_node:data_platform", "urn:tree_node:physical_model", "urn:tree_node:greenplum"]',
             '["Data Platform", "Physical model", "Greenplum"]', "test_pg_single_table_loader_main"],
            ['urn:table:oracle:dwh:abc:def:foo',
             '["urn:tree_node:databases", "urn:tree_node:oracle", "urn:database:oracle:dwh:abc", "urn:schema:oracle:dwh:abc:def"]',
             '["Databases", "Oracle", "DWH", "ABC"]', "test_pg_single_table_loader_main"],  #  должна будет удалиться если отслеживаются удаления
            ['urn:table:greenplum:dwh:gp:emart:agents',
             '["urn:tree_node:data_platform", "urn:tree_node:physical_model", "urn:tree_node:greenplum", "urn:schema:greenplum:dwh:gp:emart"]',
             '["Data Platform", "Physical model", "Greenplum", "emart"]',"test_pg_single_table_loader_main"],  #  должна будет удалиться если отслеживаются удаления
        ],
        columns=['urn', 'breadcrumb_urn', 'breadcrumb_entity', 'loaded_by'])
}

queries = ['truncate dds.relation;',
           'truncate dds.entity;',
           'truncate tuning.breadcrumb;']


@pytest.fixture
def setup_tables_empty():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(queries)
    yield
    hook.run(queries)


@pytest.fixture
def setup_tables():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(queries)
    for df_name in setup_dataset:
        df = setup_dataset[df_name]
        schema, table = df_name.split('.')
        df.to_sql(con=hook.get_uri(), schema=schema, name=table,
                  if_exists='append', index=False)
    yield
    hook.run(queries)
