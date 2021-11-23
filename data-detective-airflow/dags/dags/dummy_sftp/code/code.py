import pandas

from data_detective_airflow.operators.extractors import TSFTPOperator
from data_detective_airflow.dag_generator.dags import TDag
from data_detective_airflow.operators.transformers import PyTransform


def put_data_to_df(_context, data: bytes) -> pandas.DataFrame:
    decoded = data.decode()
    return pandas.DataFrame([[decoded]],
                            columns=['path'])


def fill_dag(tdag: TDag):
    TSFTPOperator(
        task_id='test1',
        description='Забрать /opt/common.sh',
        conn_id='ssh_service',
        remote_filepath='/opt/common.sh',
        dag=tdag
    )

    PyTransform(
        task_id='put_data_to_df',
        source=['test1'],
        transformer_callable=put_data_to_df,
        dag=tdag
    )
