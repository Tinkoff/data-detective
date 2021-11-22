# -*- coding: utf-8 -*-
"""TBaseFileWork

The module contains an abstract base class TBaseDBWork
Describes the work interface for databases (Postgres, Greenplum, MySQL)
"""
from abc import abstractmethod
from enum import Enum

from data_detective_airflow.dag_generator.works.base_work import BaseWork


class DBObjectType(Enum):
    TABLE = 'table'
    VIEW = 'view'
    SCHEMA = 'schema'
    NONE = None


class BaseDBWork(BaseWork):
    """Base class for working on a database (Postgres, Greenplum, MySQL)"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # PID сессии при исполнении execute
        self._current_session_pid = None

    def get_xcom_params(self, context: dict) -> dict:
        """Serialize DBwork into a dictionary for writing to XCom"""
        xcom_params = super().get_xcom_params(context)
        value = xcom_params.pop('value')
        value['_current_session_pid'] = self._current_session_pid
        xcom_params['value'] = value

        return xcom_params

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if the object exists
        @param path: Path to the object
        @return:
        """

    @abstractmethod
    def drop(self, path: str):
        """Delete an object by path
        @param path: Path to the object
        @return:
        """

    @abstractmethod
    def set_search_path(self, search_path: str):
        """Set up the search_path
        @param search_path: search_path
        @return:
        """

    @abstractmethod
    def is_schema(self, path: str) -> bool:
        """Check if the object is a schema
        @param path: Path to the object
        @return:
        """

    @abstractmethod
    def is_table(self, path: str) -> bool:
        """Check if the object is a table
        @param path: Path to the object
        @return:
        """

    @abstractmethod
    def is_view(self, path: str) -> bool:
        """Check whether an object is a view
        @param path: Path to the object
        @return:
        """

    @abstractmethod
    def execute(self, sql: str):
        """Exeute sql
        @param sql: SQL query
        @return:
        """

    @abstractmethod
    def terminate_failed_task_query(self, context: dict):
        """Stop executing the request after receiving the fail status of the task
        @param context: Task context
        @return:
        """
