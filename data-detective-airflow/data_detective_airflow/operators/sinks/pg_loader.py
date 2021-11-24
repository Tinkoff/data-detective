from contextlib import closing

import pandas
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as psycopg2_connection

from data_detective_airflow.operators.tbaseoperator import TBaseOperator

MAX_INSERT_ROWS_NUMBER = 2 ** 31  # 2147483648


class PgLoader(TBaseOperator):
    """Abstract loader for postgres"""

    def __init__(self, conn_id: str = None, table_name: str = None, **kwargs):
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.table_name = table_name

    @staticmethod
    def get_table_columns(table_name: str, conn: psycopg2_connection) -> list[str]:
        """Get a list of the names of its fields by the name of the table
        :param table_name:
        :param conn:
        :return: Tuple with field names
        """
        with closing(conn.cursor()) as cursor:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            return [str(desc[0]) for desc in cursor.description]

    @staticmethod
    def _get_chunk_number(data_row_number: int, chunk_row: int) -> int:
        """Calculate the number of chunks with rounding up

        :param data_row_number: Number of rows in the input dataset
        :param chunk_row: Number of rows in one chunk
        :return: Chunk number
        """
        return int((data_row_number + chunk_row - 1) // chunk_row)

    def read_result(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        schema, table = None, self.table_name
        if '.' in self.table_name:
            schema, table = self.table_name.split('.', 1)

        return pandas.read_sql_table(table_name=table, con=hook.get_uri(), schema=schema)
