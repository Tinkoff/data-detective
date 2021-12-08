from collections import namedtuple
from enum import Enum

SYSTEM_FOR_SEARCH = 'system_for_search'
TYPE_FOR_SEARCH = 'type_for_search'


# https://stackoverflow.com/a/62601113/4545870
class SystemForSearch(namedtuple('SearchSystem', 'name description'), Enum):
    ORACLE = 'Oracle', 'Oracle databases from replication'
    POSTGRES = 'Postgres', 'Postgres databases from replication'
    LOGICAL_MODEL = 'Logical Model', 'Logical Model DWH'
    DATA_DETECTIVE = 'Data Detective', 'Data Catalog for entities of any type'

    def __str__(self) -> str:
        return self.name


class TypeForSearch(namedtuple('SearchType', 'name description'), Enum):
    COLUMN = 'Column', 'Physical column of physical table'
    TABLE = 'Table', 'Physical table or view in database'
    SCHEMA = 'Schema', 'Physical schema in database'
    DATABASE = 'Database', 'Database from DBMS'
    JOB = 'Job', 'ETL job, pipeline, DAG or workflow'
    LOGICAL_SCHEMA = 'Logical Schema', 'Logical schema in model'
    LOGICAL_TABLE = 'Logical Table', 'Logical table in model'
    LOGICAL_COLUMN = 'Logical Column', 'Column in logical table'

    def __str__(self) -> str:
        return self.name


class CardType(namedtuple('CardType', 'name description'), Enum):
    TABLE = 'Table', 'Table with column list joined logical model'
    SCHEMA = 'Schema', 'Schema with table list joined logical model'
    LOGICAL_SCHEMA = 'Logical Schema', 'Schema with table list joined logical model'
    LOGICAL_TABLE = 'Logical Table', 'Table with column list joined logical model'

    def __str__(self) -> str:
        return self.name
