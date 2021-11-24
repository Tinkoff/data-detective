from contextlib import closing
from io import StringIO

import numpy
import pandas
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as psycopg2_connection

from data_detective_airflow.dag_generator.works import WorkType
from data_detective_airflow.operators.sinks.pg_loader import PgLoader, MAX_INSERT_ROWS_NUMBER


class PgSCD1DFUpdateInsert(PgLoader):
    """Update the target table by SCD 1 by diff_change_operation

    :param source: Source
    :param conn_id: Connection id
    :param table_name: Table name for update
    :param key: The key by which update. Avoid NULL for the key.
    :param diff_change_oper: Field with the flag of the operation to be applied to the record D,U,I
    :param chunk_row_number: The number of rows in the chunk to load into the database and apply to the table
    """
    ui_color = '#DDF4ED'

    def __init__(
        self,
        source: list,
        conn_id: str,
        table_name: str,
        key: list[str],
        diff_change_oper: str,
        chunk_row_number: int,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.key = key
        self.diff_change_oper = diff_change_oper
        self.chunk_row_number = chunk_row_number or MAX_INSERT_ROWS_NUMBER
        self.source = source[0]
        self.source_task = self.dag.task_dict[self.source]
        self.source_task >> self  # pylint: disable=pointless-statement

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        work = self.dag.get_work(work_type=WorkType.WORK_PG.value, work_conn_id=self.conn_id)
        work.create(context)

        source_df = self.source_task.result.read(context)
        df_rows = len(source_df.index)

        if not df_rows:
            self.log.info('Source dataset is empty. Finishing task.')
            return

        if self.chunk_row_number and self.chunk_row_number < 1:
            raise RuntimeError('chunk_row_number must be positive integer or None '
                               f'Current value is "{self.chunk_row_number}".'
                               )

        chunk_number = self._get_chunk_number(data_row_number=df_rows, chunk_row=self.chunk_row_number)
        self.log.info(f'Will process {df_rows} rows in {chunk_number} chunks.')
        source_split = numpy.array_split(source_df, chunk_number)
        del source_df

        source = f"{work.get_path(context)}.{self.table_name.split('.')[-1]}"
        for it, chunk in enumerate(source_split):
            self.log.info(f'Process chunk #{it + 1} of {chunk_number}.')
            with closing(hook.get_conn()) as session:
                self._unload_source_to_pg(tmp_table=source, conn=session, unload_df=chunk)
                self._apply_diff_change_oper(source_table=source, conn=session)
                session.commit()

    def _unload_source_to_pg(self, tmp_table: str, conn: psycopg2_connection, unload_df: pandas.DataFrame):
        """Upload DataFrame to TEMPORARY TABLE in postgres
        :param tmp_table: Name of the temporary table
        :param conn: Connection to the database
        :param unload_df: DataFrame to upload to the database
        """
        create_query = """
        DROP TABLE IF EXISTS {tmp_table} CASCADE;
        CREATE TABLE {tmp_table} AS
        SELECT {target_columns}, '' as {diff_change_oper}
        FROM {target_table}
        LIMIT 0
        """.strip()

        copy_query = """
        COPY {tmp_table} ({source_columns})
        FROM STDIN WITH (format csv, delimiter ';')
        """.strip()

        query_params = {
            'tmp_table': tmp_table,
            'target_columns': ','.join(
                self.get_table_columns(table_name=self.table_name, conn=conn)),
            'source_columns': ','.join(unload_df.columns),
            'target_table': self.table_name,
            'diff_change_oper': self.diff_change_oper
        }
        with closing(conn.cursor()) as cursor:
            cursor.execute(create_query.format(**query_params))
            s_buf = StringIO()
            unload_df.to_csv(
                path_or_buf=s_buf, index=False, header=False, sep=';')
            s_buf.seek(0)
            cursor.copy_expert(copy_query.format(**query_params), s_buf)

    def _apply_diff_change_oper(self, source_table: str, conn: psycopg2_connection):
        """Apply diff_change_oper by key, ignores unmodified columns"""
        query_params = self._get_query_params(source_table, conn)

        delete_query = """
        DELETE FROM {target_table} trg
        USING {source_table} src
        WHERE {key_eq_cond} AND src.{diff_change_oper} = 'D'
        """.strip()

        update_query = """
        UPDATE {target_table} trg
        SET {set_term}
        FROM {source_table} src
        WHERE {key_eq_cond} AND src.{diff_change_oper} = 'U'
        """.strip()

        insert_query = """
        INSERT INTO {target_table}({target_columns})
        SELECT {target_columns}
        FROM {source_table} src
        WHERE src.{diff_change_oper} = 'I'
        """.strip()

        with closing(conn.cursor()) as cursor:
            cursor.execute(delete_query.format(**query_params))
            cursor.execute(update_query.format(**query_params))
            cursor.execute(insert_query.format(**query_params))

    def _get_query_params(self, source_table: str, conn: psycopg2_connection) -> dict[str, str]:
        """Creating parameters for queries"""
        all_tgt_columns = self.get_table_columns(self.table_name, conn)
        tgt_columns = [col for col in all_tgt_columns if col != 'processed_dttm']

        key = self.key if isinstance(self.key, list) else [self.key]
        key_eq_cond = ' and '.join(f"trg.{column}=src.{column}" for column in key)

        changed_cond = [col for col in tgt_columns if col not in key]

        set_term = ', '.join(f"{col} = src.{col}" for col in changed_cond)
        if 'processed_dttm' in all_tgt_columns:
            set_term = f'{set_term}, processed_dttm = now()'

        target_columns = ','.join(tgt_columns)

        return {
            'target_table': self.table_name,
            'source_table': source_table,
            'key_eq_cond': key_eq_cond,
            'target_columns': target_columns,
            'set_term': set_term,
            'diff_change_oper': self.diff_change_oper
        }
