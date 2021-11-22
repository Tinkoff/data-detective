from mg_airflow.dag_generator.works.base_db_work import DBObjectType
from mg_airflow.dag_generator.works.base_work import WorkType
from mg_airflow.dag_generator.works.file_work import FileWork
from mg_airflow.dag_generator.works.pg_work import PgWork
from mg_airflow.dag_generator.works.s3_work import S3Work
from mg_airflow.dag_generator.works.sftp_work import SFTPWork

__all__ = (
    'WorkType',
    'DBObjectType',
    'FileWork',
    'PgWork',
    'S3Work',
    'SFTPWork',
)
