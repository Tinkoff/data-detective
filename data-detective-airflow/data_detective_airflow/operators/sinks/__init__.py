from data_detective_airflow.operators.sinks.pg_scd1 import PgSCD1
from data_detective_airflow.operators.sinks.s3_delete import S3Delete
from data_detective_airflow.operators.sinks.s3_load import S3Load

__all__ = (
    'PgSCD1',
    'S3Delete',
    'S3Load',
)
