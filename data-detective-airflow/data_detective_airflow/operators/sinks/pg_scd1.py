from contextlib import closing
from io import StringIO

import numpy
import pandas
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as psycopg2_connection

from data_detective_airflow.dag_generator.works.base_work import WorkType
from data_detective_airflow.operators.tbaseoperator import TBaseOperator

MAX_INSERT_ROWS_NUMBER = 2 ** 31  # 2147483648


class LoadingMethod:
    Delete_Insert = 'D/I'
    Update_Insert = 'U/I'


class PgSCD1(TBaseOperator):
    """Update the target table by SCD1

    :param source: Source
    :param conn_id: Connection id
    :param table_name: Table name for update
    :param key: The key by which update. Avoid NULL for the key.
    :param deleted_flg_column: Field with the delete flag, takes the values 0/1
    :param kwargs:
            loading_method - Download method: Update/Insert (U/I), Delete/Insert(D/I)
            process_deletions - records that are not in the input temporary will be deleted
            process_existing_records - records that are not different will not be affected
            chunk_row_number - the number of rows in the chunk to load into the database and apply to the table
                               only for Update/Insert and DataFrame mode

    process_existing_records for U/I is enabled by default.
    """

    ui_color = '#dde4ed'

    def __init__(self, source: list[str], conn_id: str, table_name: str, key: list[str],
                 deleted_flg_column: str = None, **kwargs):
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.table_name = table_name
        self.key = key
        self.source = source[0]
        self.source_task = self.dag.task_dict[self.source]
        self.source_task >> self  # pylint: disable=pointless-statement
        self.deleted_flg_column = deleted_flg_column
        self.process_existing_records = kwargs.get('process_existing_records') or False
        self.process_deletions = kwargs.get('process_deletions') or False
        self.loading_method = kwargs.get('loading_method') or LoadingMethod.Update_Insert
        self.chunk_row_number = kwargs.get('chunk_row_number') or MAX_INSERT_ROWS_NUMBER

    def execute(self, context: dict):
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        source = None
        work = self.dag.get_work(work_type=WorkType.WORK_PG.value, work_conn_id=self.conn_id)
        work.create(context)
        is_upload_mode = (self.source_task.result.work.conn_id != self.conn_id)

        source_df = self.source_task.result.read(context)

        if self.loading_method == LoadingMethod.Update_Insert:
            if is_upload_mode:
                df_rows = len(source_df.index)

                if not df_rows:
                    self.log.info('Source dataset is empty. Finishing task.')
                    return

                if self.chunk_row_number and self.chunk_row_number < 1:
                    raise RuntimeError('chunk_row_number must be positive integer or None '
                                       f'Current value is "{self.chunk_row_number}".'
                                       )

                chunk_number = self._get_chunk_number(data_row_number=df_rows, chunk_row=self.chunk_row_number)

                if self.process_deletions and chunk_number > 1:
                    raise RuntimeError('"process_deletions" works only for single chunk. '
                                       f'Current chunk_number is {chunk_number}.'
                                       )

                self.log.info(f'Will process {df_rows} rows in {chunk_number} chunks.')
                source_split = numpy.array_split(source_df, chunk_number)
                del source_df

                source = f"{work.get_path(context)}.{self.table_name.split('.')[-1]}"
                for it, chunk in enumerate(source_split):
                    self.log.info(f'Process chunk #{it + 1} of {chunk_number}.')
                    self.upload_and_update_insert(hook, source, chunk)
            else:
                with closing(hook.get_conn()) as session:
                    source = self.source_task.result.get_table_name(context)
                    if self.process_deletions:
                        self._process_deletions(source_table=source, conn=session)

                    self._update_insert(source_table=source, conn=session)
                    session.commit()

            return

        # LoadingMethod.Delete_Insert
        with closing(hook.get_conn()) as session:
            if is_upload_mode:
                source = f"{work.get_path(context)}.{self.table_name.split('.')[-1]}"
                self._unload_source_to_pg(tmp_table=source, conn=session, unload_df=source_df)
            session.commit()

            source = source or self.source_task.result.get_table_name(context)

            if self.process_deletions:
                self._process_deletions(source_table=source, conn=session)

            if self.process_existing_records:
                self._delete_insert_yes(source_table=source, conn=session)
            else:
                self._delete_insert_no(source_table=source, conn=session)

            session.commit()

    def _process_deletions(self, source_table: str, conn: psycopg2_connection):
        """Delete target entries that are not in the source"""
        delete_query = """
        DELETE
        FROM {target_table} t1 USING (
            SELECT {key_string}
              FROM {target_table} t1
                       LEFT JOIN {source_table} t2 ON {key_eq_condition}
              WHERE t2.* IS NULL) t2
        WHERE {key_eq_condition}
            """.strip()

        key = self.key
        if not isinstance(key, list):
            key = [key]
        key_eq_condition = "1=1 and {0}".format(
            ' and '.join(f"t1.{column}=t2.{column}" for column in key))

        query_params = {
            'target_table': self.table_name,
            'source_table': source_table,
            'key_eq_condition': key_eq_condition,
            'key_string': ','.join(f't1.{col}' for col in key)
        }
        with closing(conn.cursor()) as cursor:
            cursor.execute(delete_query.format(**query_params))

    def _unload_source_to_pg(self, tmp_table: str, conn: psycopg2_connection, unload_df: pandas.DataFrame):
        """Upload DataFrame to TEMPORARY TABLE in postgres
        :param tmp_table: Name of the temporary table
        :param conn: Connection to the database
        :param unload_df: DataFrame to upload to the database
        """
        create_query = """
    DROP TABLE IF EXISTS {tmp_table} CASCADE;
    CREATE TABLE {tmp_table} AS
    SELECT {target_columns}{deleted_flg_column_addition} FROM {target_table}
    LIMIT 0
        """.strip()

        copy_query = """
    COPY {tmp_table} ({source_columns}) FROM STDIN WITH (format csv, delimiter ';')
        """.strip()

        deleted_flg_column_addition = ", 0 as " + \
                                      f"{self.deleted_flg_column}" if self.deleted_flg_column else ""

        query_params = {
            'tmp_table': tmp_table,
            'target_columns': ','.join(
                PgSCD1.get_table_columns(table_name=self.table_name, conn=conn)),
            'source_columns': ','.join(unload_df.columns),
            'target_table': self.table_name,
            'deleted_flg_column_addition': deleted_flg_column_addition,
        }

        with closing(conn.cursor()) as cursor:
            cursor.execute(create_query.format(**query_params))
            s_buf = StringIO()
            unload_df.to_csv(
                path_or_buf=s_buf, index=False, header=False, sep=';')
            s_buf.seek(0)
            cursor.copy_expert(copy_query.format(**query_params), s_buf)

    def _delete_insert_no(self, source_table: str, conn: psycopg2_connection):
        """Delete/Insert by the key
        :param source_table: Input table name
        :param conn:
        """
        query_params = self._get_query_params(source_table, conn)

        delete_query = """
        DELETE FROM {target_table} as trg
        USING {source_table} as src
        WHERE {key_eq_cond}
        """.strip()

        insert_query = """
        INSERT INTO {target_table}({target_columns})
        SELECT {target_columns}
        FROM {source_table} as src
        WHERE NOT ({deleted_row_condition})
        """.strip()

        with closing(conn.cursor()) as cursor:
            cursor.execute(delete_query.format(**query_params))
            cursor.execute(insert_query.format(**query_params))

    def _delete_insert_yes(self, source_table: str, conn: psycopg2_connection):
        """Delete/Insert by the key, ignores unmodified columns"""
        query_params = self._get_query_params(source_table, conn)

        delete_query = """
        DELETE FROM {target_table} trg USING
        (SELECT src.* FROM {source_table} src
             LEFT JOIN {target_table} trg
             ON {key_eq_cond}
             WHERE ({changed_cond}) or ({deleted_row_condition})) as src
        WHERE {key_eq_cond}
        """.strip()

        insert_query = """
        INSERT INTO {target_table}({target_columns})
        SELECT src.* FROM
         (SELECT {target_columns} FROM {source_table} src
          WHERE NOT ({deleted_row_condition})) src
         LEFT JOIN {target_table} trg
         ON {key_eq_cond}
         WHERE ({changed_cond}) or trg.* is NULL""".strip()

        with closing(conn.cursor()) as cursor:
            cursor.execute(delete_query.format(**query_params))
            cursor.execute(insert_query.format(**query_params))

    def _update_insert(self, source_table: str, conn: psycopg2_connection):
        """Update/Insert by the key, ignores unmodified columns"""
        query_params = self._get_query_params(source_table, conn)

        delete_by_flg_query = """
        DELETE FROM {target_table} trg USING
        {source_table} src WHERE {key_eq_cond} and ({deleted_row_condition})
        """.strip()

        update_query = """
        UPDATE {target_table} trg
        SET {set_term}
        FROM {source_table} src
       WHERE {key_eq_cond}
         AND ({changed_cond})
        """.strip()

        insert_query = """
        INSERT INTO {target_table}({target_columns})
        SELECT src.* FROM
         (SELECT {target_columns} FROM {source_table} src
          WHERE NOT ({deleted_row_condition})) src
         LEFT JOIN {target_table} trg
         ON {key_eq_cond}
         WHERE trg.* is NULL""".strip()

        with closing(conn.cursor()) as cursor:
            if self.deleted_flg_column:
                cursor.execute(delete_by_flg_query.format(**query_params))
            cursor.execute(update_query.format(**query_params))
            cursor.execute(insert_query.format(**query_params))

    def _get_query_params(self, source_table: str, conn: psycopg2_connection) -> dict[str, str]:
        """Creating parameters for queries"""
        all_tgt_columns = PgSCD1.get_table_columns(self.table_name, conn)
        tgt_columns = [col for col in all_tgt_columns if col != 'processed_dttm']

        deleted_row_condition = f"coalesce(src.{self.deleted_flg_column},0)=1" if \
            self.deleted_flg_column else '1!=1'

        key = self.key if isinstance(self.key, list) else [self.key]

        key_eq_cond = ' and '.join(f"trg.{column}=src.{column}" for column in key)

        changed_cond = [col for col in tgt_columns if col not in key]

        set_term = ', '.join(f"{col} = src.{col}" for col in changed_cond)
        if 'processed_dttm' in all_tgt_columns:
            set_term = f'{set_term}, processed_dttm = now()'

        changed_cond = ' or '.join(
            f"coalesce(src.{col}::text,'NULL') != coalesce(trg.{col}::text,'NULL')"
            for col in changed_cond)
        changed_cond = f'1=0 or {changed_cond}'

        target_columns = ','.join(tgt_columns)

        return {
            'target_table': self.table_name,
            'source_table': source_table,
            'key_eq_cond': key_eq_cond,
            'deleted_row_condition': deleted_row_condition,
            'target_columns': target_columns,
            'changed_cond': changed_cond,
            'set_term': set_term,
        }

    def upload_and_update_insert(self,
                                 hook: PostgresHook,
                                 source_table: str,
                                 dataframe: pandas.DataFrame
                                 ) -> None:
        """Load the DataFrame into the database and apply the table to the target.

        :param hook: Hook for connecting to the database
        :param source_table: Name for the temporary table in the database
        :param dataframe: DataFrame to apply to a table
        """
        with closing(hook.get_conn()) as session:
            self._unload_source_to_pg(tmp_table=source_table, conn=session, unload_df=dataframe)

            if self.process_deletions:
                self._process_deletions(source_table=source_table, conn=session)

            self._update_insert(source_table=source_table, conn=session)
            session.commit()

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
        :return: chunk number
        """
        return int((data_row_number + chunk_row - 1) // chunk_row)

    def read_result(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        schema, table = None, self.table_name
        if '.' in self.table_name:
            schema, table = self.table_name.split('.', 1)

        return pandas.read_sql_table(table_name=table, con=hook.get_uri(), schema=schema)
