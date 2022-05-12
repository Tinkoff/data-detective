from datetime import datetime
from typing import FrozenSet

from airflow.models.variable import Variable

# works and connections
PG_CONN_ID: str = 'pg'
S3_CONN_ID: str = 's3'
SFTP_CONN_ID: str = 'ssh_service'

# variables
WORK_S3_BUCKET: str = Variable.get('WORK_S3_BUCKET', default_var='dev')
WORK_S3_PREFIX: str = 'dd_airflow'
WORK_FILE_PREFIX: str = 'wrk'
WORK_PG_SCHEMA_PREFIX: str = 'wrk'

# DAG
DEFAULT_START_DATE = datetime(2020, 4, 8)
DAG_ID_KEY: str = 'dag_id'
TASK_ID_KEY: str = 'task_id'

CLEAR_WORK_KEY: str = 'clear_work'

# Timeouts in SECONDS
EXECUTION_TIMEOUT: int = 6000


# Catalog
class RelationFields:
    SOURCE = 'source'
    DESTINATION = 'destination'
    TYPE = 'type'
    ATTRIBUTE = 'attribute'


class EntityFields:
    URN = 'urn'
    ENTITY_NAME = 'entity_name'
    ENTITY_NAME_SHORT = 'entity_name_short'
    ENTITY_TYPE = 'entity_type'
    INFO = 'info'
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
    TAGS = 'tags'
    FILTERS = 'filters'


RELATION_NONE = 'non'
RELATION_KEY_FIELDS: FrozenSet[str] = frozenset({RelationFields.SOURCE, RelationFields.DESTINATION,
                                                 RelationFields.ATTRIBUTE})
JSON_FIELDS: FrozenSet[str] = frozenset({EntityFields.JSON_DATA, EntityFields.JSON_SYSTEM, EntityFields.CODES,
                                         EntityFields.HTMLS, EntityFields.TABLES, EntityFields.NOTIFICATIONS,
                                         EntityFields.JSON_DATA_UI, EntityFields.GRID,
                                         EntityFields.LINKS, EntityFields.TAGS, EntityFields.FILTERS})
