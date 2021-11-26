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
    """Вернуть URN ETL job'а
    :param project: Название проекта: DWH, oracle, postgres
    :param system: название ETL инструмента, например, sasdi
    :param name: название etl job'а например DDS LOAD PARTY
    :return: urn
    """
    return f'urn:job:{_clear_and_lower_parts(locals().values())}'


def get_database(db_type: str, location: str, database: str) -> str:
    """Вернуть URN физической базы данных
    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :return: urn, example - urn:database:postgres:pg:airflow
    """
    return f'urn:database:{_clear_and_lower_parts(locals().values())}'


def get_schema(db_type: str, location: str, database: str, schema: str) -> str:
    """Вернуть URN физической схемы базы данных

    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :param schema: schema name
    :return: urn, example - urn:schema:postgres:pg:airflow:mart
    """
    return f'urn:schema:{_clear_and_lower_parts(locals().values())}'


def split_schema(urn: str) -> tuple[str, str, str, str]:
    """Разложить URN физической схемы базы данных
    :param urn: URN схемы
    :return: tuple из требуемых для URN параметров
    """
    _, _, db_type, location, database, schema = urn.split(':')
    return db_type, location, database, schema


def get_table(db_type: str, location: str, database: str, schema: str, table: str) -> str:
    """Вернуть URN физической таблицы базы данных

    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :param schema: schema name
    :param table: table name, example - entity
    :return: urn, example - urn:schema:postgres:pg:airflow:dds:entity
    """
    return f'urn:schema:{_clear_and_lower_parts(locals().values())}'


def split_table(urn: str) -> tuple[str, str, str, str, str]:
    """Разложить URN физической таблицы базы данных

    :param urn: URN таблицы
    :return: tuple из требуемых для URN параметров
    """
    _, _, db_type, location, database, schema, table = urn.split(':')
    return db_type, location, database, schema, table


def get_column(db_type: str, location: str, database: str, schema: str, table: str, column: str) -> str:
    """Вернуть URN физической колонки таблицы базы данных

    :param db_type: DB type - postgres, mysql, greenplum, logical_model
    :param location: address, url, uri
    :param database: database name
    :param schema: schema name
    :param table: table name, example - entity
    :param column: column name, example - json_data
    :return: urn, например urn:schema:postgres:pg:airflow:dds:entity:json_data
    """
    return f'urn:schema:{_clear_and_lower_parts(locals().values())}'


def split_column(urn: str) -> tuple[str, str, str, str, str]:
    """Разложить URN физического поля базы данных
    :param urn: URN поля
    :return: tuple из требуемых для URN параметров
    """
    _, _, _, project, system, schema, table, column = urn.split(':')
    return project, system, schema, table, column


def get_tree_node(nodes: Sequence[str]) -> str:
    """Вернуть URN tree_node
    :param nodes: Название узла иерархии
    :return:
    """
    return f'urn:tree_node:{_clear_and_lower_parts(nodes)}'
