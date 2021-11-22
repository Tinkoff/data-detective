# -*- coding: utf-8 -*-
import time
from contextlib import closing
from functools import wraps
from typing import Callable, Optional, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import DataFrame, read_sql_query
from psycopg2.extensions import connection, cursor

from data_detective_airflow.constants import PG_CONN_ID, WORK_PG_SCHEMA_PREFIX
from data_detective_airflow.dag_generator.works.base_db_work import BaseDBWork, DBObjectType
from data_detective_airflow.dag_generator.works.base_work import WorkType, synchronize


def provide_arg(arg_name: str, provider):  # pylint: disable=no-self-argument
    def _provide_arg(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            in_kwargs = arg_name in kwargs

            if in_kwargs:
                return method(self, *args, **kwargs)
            return provider(self, method, arg_name, *args, **kwargs)

        return wrapper

    return _provide_arg


# pylint: disable = arguments-differ
class PgWork(BaseDBWork):
    """Work based on postgres"""

    def __init__(self, dag, conn_id: str = PG_CONN_ID):
        super().__init__(dag=dag, work_type=WorkType.WORK_PG.value, conn_id=conn_id)
        self._hook = None

    def get_hook(self):
        if self._hook is None:
            self._hook = PostgresHook(postgres_conn_id=self.conn_id)
        return self._hook

    def get_path(self, context: Optional[dict], prefix: str = WORK_PG_SCHEMA_PREFIX):
        return super().get_path(context, prefix)[:63]

    def _create_logic(self, context: dict = None):
        path = self.get_path(context)
        self.log.info(f'Init postgres work schema {path}')
        self.execute(f'create schema if not exists {path};')

    def _clear_logic(self, context: dict = None):
        path = self.get_path(context)
        self.log.info(f'Cleaning pg work schema {path}')
        self.drop(path)

    def provide_conn(self, method: Callable, arg_name, *args, **kwargs) -> Callable:
        with closing(self.get_hook().get_conn()) as conn:
            kwargs[arg_name] = conn
            result_ = method(self, *args, **kwargs)
            conn.commit()
            return result_

    def provide_cur(self, method: Callable, arg_name, *args, **kwargs) -> Callable:
        with closing(kwargs['conn'].cursor()) as cur:
            kwargs[arg_name] = cur
            return method(self, *args, **kwargs)

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def execute(
        self,
        sql: str,
        conn: connection = None,  # pylint: disable=inconsistent-return-statements,unused-argument
        cur: cursor = None,
        fetch: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> Union[None, tuple, list]:
        """Execute SQL script
        @param sql: SQL-script
        @param conn: Connection to db
        @param cur: Cursor
        @param fetch: None - do not get the result, one - return 1, all - return all
        @param context: None - Context for syncing a work in XCom
        @return:
        """
        self.log.info(sql)
        if context:
            self.sync_current_session_pid(context, conn)
        start_time = time.perf_counter()
        cur.execute(sql)  # type: ignore
        self.log.info('==> Complete in %s sec', round(time.perf_counter() - start_time, 1))
        for notice in conn.notices:  # type: ignore
            self.log.info(notice)
        if cur.rowcount != -1:  # type: ignore
            self.log.info('==> Processed %s rows', cur.rowcount)  # type: ignore
        if fetch == 'one':
            return cur.fetchone()[0]  # type: ignore
        if fetch == 'all':
            return cur.fetchall()  # type: ignore
        return None

    @synchronize
    def sync_current_session_pid(self, context: Optional[dict], conn: connection):  # pylint: disable=unused-argument
        self._current_session_pid = conn.get_backend_pid()

    @staticmethod
    def _get_query_to_terminate_pid(pid):
        return f'SELECT pg_cancel_backend({pid})'

    def terminate_failed_task_query(self, context):
        attrs_values = self._get_attrs_from_xcom(context)
        pid = attrs_values.get('_current_session_pid')
        self.log.info(f'PID to terminate is {pid}')
        if pid:
            self.execute(self._get_query_to_terminate_pid(pid=pid))
            self.log.error(f'Query was terminated with pid: {pid}')
            return
        self.log.info('No query to terminate')

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def get_object_type_by_path(self, path: str, conn: connection = None, cur: cursor = None) -> Union[str, None]:
        """The method determines the result type by path. If there is no object
        DBObjectType.NONE.value will be returned.

        The input is expected to be path in the form of schema.table_name, so tables and views
        from the schema located in search_path must be set EXPLICITLY using the schema.

        Using the DB name in path will also lead to an ERROR"""
        path_parts = path.split('.')
        schema = path_parts[0]
        table = None
        if len(path_parts) == 2:
            table = path_parts[1]
        sql_query = f"""
        SELECT
            CASE
                WHEN c.relkind = 'r' THEN '{DBObjectType.TABLE.value}'
                WHEN c.relkind = 'v' THEN '{DBObjectType.VIEW.value}'
                WHEN lower(n.nspname) = lower('{schema}') THEN '{DBObjectType.SCHEMA.value}'
            END AS type
        FROM pg_catalog.pg_namespace n
        LEFT JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace
                AND c.relkind IN ('r', 'v')
                AND lower(c.relname) = lower('{table}')
        WHERE lower(n.nspname) = lower('{schema}')
        """
        object_type = self.execute(sql_query, conn=conn, cur=cur, fetch='one')
        # Identify non-existent tables when the schema exists
        if object_type == DBObjectType.SCHEMA.value and table:
            object_type = DBObjectType.NONE.value
        return object_type if object_type else DBObjectType.NONE.value

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def is_schema(self, path: str, conn: connection = None, cur: cursor = None) -> bool:
        """Check if the object is a schema
        @param path: Schema.table
        @param conn: Connection to db
        @param cur: Cursor
        @return:
        """
        object_type = self.get_object_type_by_path(path, conn=conn, cur=cur)
        return object_type == DBObjectType.SCHEMA.value

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def is_table(self, path: str, conn: connection = None, cur: cursor = None) -> bool:
        """Check if the object is a table
        @param path: schema.table
        @param conn: Connection to db
        @param cur: Cursor
        @return:
        """
        object_type = self.get_object_type_by_path(path, conn=conn, cur=cur)
        return object_type == DBObjectType.TABLE.value

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def is_view(self, path: str, conn: connection = None, cur: cursor = None) -> bool:
        """Check if the object is a view
        @param path: schema.table
        @param conn: Connection to db
        @param cur: Cursor
        @return:
        """
        object_type = self.get_object_type_by_path(path, conn=conn, cur=cur)
        return object_type == DBObjectType.VIEW.value

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def exists(self, path: str, conn: connection = None, cur: cursor = None) -> bool:
        """Check if the object exists
        @param path: schema.table
        @param conn: Connection to db
        @param cur: Cursor
        @return:
        """
        return bool(self.get_object_type_by_path(path, conn=conn, cur=cur))

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def set_search_path(self, search_path: str, conn: connection = None, cur: cursor = None):
        """Set search_path
        @param search_path: search_path
        @param conn: Connection to db
        @param cur: Cursor
        """
        sql = f'SET SEARCH_PATH TO {search_path}, public;'
        self.execute(sql=sql, conn=conn, cur=cur)

    @provide_arg('conn', provide_conn)
    @provide_arg('cur', provide_cur)
    def drop(self, path: str, conn: connection = None, cur: cursor = None, cascade: bool = True):
        """Drop the object by path
        @param path: Path to the object
        @param conn: Connection to db
        @param cur: Cursor
        @param cascade: Delete dependent objects
        """
        sql = None
        object_type = self.get_object_type_by_path(path, conn=conn, cur=cur)
        if object_type == DBObjectType.SCHEMA.value:
            sql = f'DROP SCHEMA {path}'
        if object_type == DBObjectType.TABLE.value:
            sql = f'DROP TABLE {path}'
        if object_type == DBObjectType.VIEW.value:
            sql = f'DROP VIEW {path}'
        if not sql:
            self.log.info(f'Object {path} cannot be dropped due to it does not exist.')
            return
        if cascade:
            sql += ' CASCADE'
        self.execute(sql=sql, conn=conn, cur=cur)
        self.log.info(f'Object {path} have been dropped successfully.')

    def write_df(self, path: str, frame: DataFrame, **kwargs):
        """Upload DataFrame to TEMPORARY TABLE postgres
        :param path: Table name
        :param frame: DataFrame
        :param kwargs: Additional params

        The Data Frame index is excluded from loading.
        Default params:
            chunksize: 1000
            method: 'multi'
        """
        parts = path.split('.')
        schema = parts[0]
        table = parts[1]
        chunksize = kwargs.pop('chunksize', 1000)
        method = kwargs.pop('method', 'multi')
        # Using get_uri, provide_conn uses get_conn
        conn = self.get_hook().get_uri()
        frame.to_sql(schema=schema, name=table, con=conn, chunksize=chunksize, method=method, index=False, **kwargs)

    def read_df(self, sql: str, **kwargs) -> DataFrame:
        con = self.get_hook().get_uri()
        return read_sql_query(sql=sql, con=con, **kwargs)

    def get_size(self, path: str) -> str:
        """Get the size of the object in the database.
        In case of no table returns -1

        :param path: Object name
        :return: Rounded object size
        """
        size = -1
        if self.is_table(path):
            query = f"select pg_total_relation_size('{path}'::regclass)"
            size = self.execute(sql=query, fetch='one')
        return self.get_readable_size_bytes(size)
