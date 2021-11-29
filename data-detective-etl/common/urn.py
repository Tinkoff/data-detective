import re
from collections.abc import Sequence, Iterable
from typing import Pattern

COMMON_REG = r'[^\wа-яА-Я0-9]'
PARTS_DELIMITER = ':'


def _clear_and_lower_parts(parts: Iterable[str], regex: Pattern = COMMON_REG) -> str:
    """Clear parts from non-alphanumerical letters. Then lowercase.

    :param parts: String list
    :param regex: Regex pattern
    :return: parts, joined with PARTS_DELIMITER
    """
    return PARTS_DELIMITER.join(re.sub(regex, "_", value).lower() for value in parts)


def get_etl_job(project: str, system: str, name: str) -> str:
    """Return the URN of ETL job
    :param project: Name of the project: DWH, oracle, postgres
    :param system: ETL tool name, e.g. sasdi
    :param name: ETL job's name, e.g. DDS LOAD PARTY
    :return: urn
    """
    return f'urn:job:{_clear_and_lower_parts(locals().values())}'


def get_database(db_type: str, location: str, database: str) -> str:
    """Return the URN of the database
    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :return: urn, example - urn:database:postgres:pg:airflow
    """
    return f'urn:database:{_clear_and_lower_parts(locals().values())}'


def get_schema(db_type: str, location: str, database: str, schema: str) -> str:
    """Return the URN of the database schema

    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :param schema: schema name
    :return: urn, example - urn:schema:postgres:pg:airflow:mart
    """
    return f'urn:schema:{_clear_and_lower_parts(locals().values())}'


def split_schema(urn: str) -> tuple[str, str, str, str]:
    """Split the URN of the database schema
    :param urn: Schema URN
    :return: tuple from parameters required for URN
    """
    _, _, db_type, location, database, schema = urn.split(':')
    return db_type, location, database, schema


def get_table(db_type: str, location: str, database: str, schema: str, table: str) -> str:
    """Return the URN of the database table

    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :param schema: schema name
    :param table: table name, example - entity
    :return: urn, example - urn:schema:postgres:pg:airflow:dds:entity
    """
    return f'urn:schema:{_clear_and_lower_parts(locals().values())}'


def split_table(urn: str) -> tuple[str, str, str, str, str]:
    """Split the URN of the database table

    :param urn: Database table URN
    :return: tuple from parameters required for URN
    """
    _, _, db_type, location, database, schema, table = urn.split(':')
    return db_type, location, database, schema, table


def get_column(db_type: str, location: str, database: str, schema: str, table: str, column: str) -> str:
    """Return the URN of the database table column

    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :param schema: schema name
    :param table: table name, example - entity
    :param column: column name, example - json_data
    :return: urn, example: urn:schema:postgres:pg:airflow:dds:entity:json_data
    """
    return f'urn:schema:{_clear_and_lower_parts(locals().values())}'


def split_column(urn: str) -> tuple[str, str, str, str, str]:
    """Split the URN of the database column
    :param urn: Column URN
    :return: tuple from parameters required for URN
    """
    _, _, _, project, system, schema, table, column = urn.split(':')
    return project, system, schema, table, column


def get_tree_node(nodes: Sequence[str]) -> str:
    """Return URN tree_node
    :param nodes: Hierarchy node name
    :return:
    """
    return f'urn:tree_node:{_clear_and_lower_parts(nodes)}'
