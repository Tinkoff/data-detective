from collections import namedtuple
from enum import Enum
from typing import Union

SYSTEM_FOR_SEARCH = 'system_for_search'
TYPE_FOR_SEARCH = 'type_for_search'


# https://stackoverflow.com/a/62601113/4545870
class SystemForSearch(namedtuple('SearchSystem', 'name title_code info_code'), Enum):
    ORACLE = 'Oracle', 'data.search.filters.system.oracle.title', 'data.search.filters.system.oracle.info'
    POSTGRES = 'Postgres', 'data.search.filters.system.postgres.title', 'data.search.filters.system.postgres.info'
    LOGICAL_MODEL = 'Logical_Model', 'data.search.filters.system.logical-model.title', 'data.search.filters.system.logical-model.info'
    DATA_DETECTIVE = 'Data Detective', 'data.search.filters.system.data-detective.title', 'data.search.filters.system.data-detective.info'

    def __str__(self) -> str:
        return self.name


class TypeForSearch(namedtuple('SearchType', 'name title_code info_code'), Enum):
    COLUMN = 'Column', 'data.search.filters.type.column.title', 'data.search.filters.type.column.info'
    TABLE = 'Table', 'data.search.filters.type.table.title', 'data.search.filters.type.table.info'
    SCHEMA = 'Schema', 'data.search.filters.type.schema.title', 'data.search.filters.type.schema.info'
    DATABASE = 'Database', 'data.search.filters.type.database.title', 'data.search.filters.type.database.info'
    JOB = 'Job', 'data.search.filters.type.job.title', 'data.search.filters.type.job.info'
    LOGICAL_SCHEMA = 'Logical Schema', 'data.search.filters.type.logical-schema.title', 'data.search.filters.type.logical-schema.info'
    LOGICAL_TABLE = 'Logical Table', 'data.search.filters.type.logical-table.title', 'data.search.filters.type.logical-table.info'
    LOGICAL_COLUMN = 'Logical Column', 'data.search.filters.type.logical-column.title', 'data.search.filters.type.logical-column.info'

    def __str__(self) -> str:
        return self.name


class CardType(namedtuple('CardType', 'name description'), Enum):
    TABLE = 'Table', 'Table with column list joined logical model'
    SCHEMA = 'Schema', 'Schema with table list joined logical model'
    LOGICAL_SCHEMA = 'Logical Schema', 'Schema with table list joined logical model'
    LOGICAL_TABLE = 'Logical Table', 'Table with column list joined logical model'

    def __str__(self) -> str:
        return self.name


system_for_search_x_type_for_search: list[dict[str, Union[str, list[str]]]] = [
    {
        "system_name": SystemForSearch.ORACLE.name,
        "type_name": [
            TypeForSearch.COLUMN.name,
            TypeForSearch.TABLE.name,
            TypeForSearch.SCHEMA.name,
            TypeForSearch.DATABASE.name,
        ],
    },
    {
        "system_name": SystemForSearch.POSTGRES.name,
        "type_name": [
            TypeForSearch.COLUMN.name,
            TypeForSearch.TABLE.name,
            TypeForSearch.SCHEMA.name,
            TypeForSearch.DATABASE.name,
        ],
    },
    {
        "system_name": SystemForSearch.LOGICAL_MODEL.name,
        "type_name": [
            TypeForSearch.LOGICAL_COLUMN.name,
            TypeForSearch.LOGICAL_TABLE.name,
            TypeForSearch.LOGICAL_SCHEMA.name,
        ],
    },
    {
        "system_name": SystemForSearch.DATA_DETECTIVE.name,
        "type_name": [
            TypeForSearch.JOB.name,
        ],
    },
]
