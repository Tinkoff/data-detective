import pandas

from mg_airflow.constants import S3_CONN_ID, PG_CONN_ID, SFTP_CONN_ID
from mg_airflow.dag_generator.results.base_result import ResultType
from mg_airflow.dag_generator.works.base_work import WorkType
from mg_airflow.dag_generator.dags import TDag
from mg_airflow.operators.extractors import PythonDump
from mg_airflow.operators.sinks import PgSCD1
from mg_airflow.operators.transformers import PyTransform


def get_src_dataframe(_context: dict):
    src_dataframe = pandas.DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc'],
        ],
        columns=['c1', 'c2', 'c3']
    )
    return src_dataframe


def fill_dag(tdag: TDag):
    PythonDump(
        task_id='t1',
        python_callable=get_src_dataframe,
        work_type=WorkType.WORK_FILE.value,
        result_type=ResultType.RESULT_PICKLE.value,
        dag=tdag
    )

    PyTransform(
        task_id='t2',
        source=['t1'],
        transformer_callable=lambda context, df: df,
        work_type=WorkType.WORK_S3.value,
        result_type=ResultType.RESULT_PICKLE.value,
        work_conn_id=S3_CONN_ID,
        dag=tdag
    )

    PyTransform(
        task_id='t3',
        source=['t2'],
        transformer_callable=lambda context, df: df,
        result_type=ResultType.RESULT_PG.value,
        work_type=WorkType.WORK_PG.value,
        work_conn_id=PG_CONN_ID,
        dag=tdag
    )

    PyTransform(
        task_id='t4',
        source=['t3'],
        transformer_callable=lambda context, df: df,
        result_type=ResultType.RESULT_PICKLE.value,
        work_type=WorkType.WORK_SFTP.value,
        work_conn_id=SFTP_CONN_ID,
        dag=tdag
    )

    PgSCD1(
        source=['t4'],
        conn_id=PG_CONN_ID,
        table_name='test_table',
        partition_column='c1',
        task_id='test_pg_sink',
        key='c1',
        dag=tdag
    )
