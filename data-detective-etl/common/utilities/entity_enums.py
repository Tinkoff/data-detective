from collections import namedtuple
from enum import Enum
from typing import FrozenSet


class EntityTypes(namedtuple('EntityType', 'key message_code'), Enum):
    TREE_NODE = 'TREE_NODE', 'data.entity.entity-type.tree-node'
    SCHEMA = 'SCHEMA', 'data.entity.entity-type.schema'
    TABLE = 'TABLE', 'data.entity.entity-type.table'
    COLUMN = 'COLUMN', 'data.entity.entity-type.column'
    JOB = 'JOB', 'data.entity.entity-type.job'
    LOGICAL_SCHEMA = 'LOGICAL_SCHEMA', 'data.entity.entity-type.logical-schema'
    LOGICAL_TABLE = 'LOGICAL_TABLE', 'data.entity.entity-type.logical-table'
    LOGICAL_COLUMN = 'LOGICAL_COLUMN', 'data.entity.entity-type.logical-column'
    LOGICAL_REPORT = 'LOGICAL_REPORT', 'data.entity.entity-type.logical-report'


class EntityFields:
    URN = 'urn'
    ENTITY_NAME = 'entity_name'
    ENTITY_NAME_SHORT = 'entity_name_short'
    ENTITY_TYPE = 'entity_type'
    JSON_DATA = 'json_data'
    JSON_SYSTEM = 'json_system'
    TABLES = 'tables'
    CODES = 'codes'
    HTMLS = 'htmls'
    NOTIFICATIONS = 'notifications'
    GRID = 'grid'
    JSON_DATA_UI = 'json_data_ui'
    SEARCH_DATA = 'search_data'
    LINKS = 'links'
    INFO = 'info'
    TAGS = 'tags'


class RelationTypes:
    Describes = 'Describes'
    Contains = 'Contains'
    Loads = 'Loads'


class RelationFields:
    SOURCE = 'source'
    DESTINATION = 'destination'
    TYPE = 'type'
    ATTRIBUTE = 'attribute'


ENTITY_CORE_FIELDS: FrozenSet[str] = frozenset({EntityFields.URN, EntityFields.ENTITY_TYPE,
                                                EntityFields.ENTITY_NAME, EntityFields.ENTITY_NAME_SHORT,
                                                EntityFields.JSON_DATA, EntityFields.SEARCH_DATA})
RELATION_CORE_FIELDS: FrozenSet[str] = frozenset({RelationFields.SOURCE, RelationFields.DESTINATION,
                                                  RelationFields.TYPE, RelationFields.ATTRIBUTE})
