# -*- coding: utf-8 -*-
"""TBaseFileWork

Модуль содержит абстрактный базовый класс TBaseDBWork
Описывает интерфейс work для баз данных (Postgres, Greenplum, MySQL)
"""
from abc import abstractmethod
from enum import Enum

from mg_airflow.dag_generator.works.base_work import BaseWork


class DBObjectType(Enum):
    TABLE = 'table'
    VIEW = 'view'
    SCHEMA = 'schema'
    NONE = None


class BaseDBWork(BaseWork):
    """Базовый класс для work на базе данных (Postgres, Greenplum, MySQL)"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # PID сессии при исполнении execute
        self._current_session_pid = None

    def get_xcom_params(self, context: dict) -> dict:
        """Сериализовать DBwork в словарь для записи в XCom"""
        xcom_params = super().get_xcom_params(context)
        value = xcom_params.pop('value')
        value['_current_session_pid'] = self._current_session_pid
        xcom_params['value'] = value

        return xcom_params

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Проверить, существует ли объект
        @param path: путь к объекту
        @return:
        """

    @abstractmethod
    def drop(self, path: str):
        """Удалить объект по path
        @param path: путь к объекту
        @return:
        """

    @abstractmethod
    def set_search_path(self, search_path: str):
        """Установить search_path
        @param search_path: search_path
        @return:
        """

    @abstractmethod
    def is_schema(self, path: str) -> bool:
        """Проверить, является ли объект схемой
        @param path: путь к объекту
        @return:
        """

    @abstractmethod
    def is_table(self, path: str) -> bool:
        """Проверить, является ли объект таблицей
        @param path: путь к объекту
        @return:
        """

    @abstractmethod
    def is_view(self, path: str) -> bool:
        """Проверить, является ли объект представлением
        @param path: путь к объекту
        @return:
        """

    @abstractmethod
    def execute(self, sql: str):
        """Выполнить sql
        @param sql: код запроса
        @return:
        """

    @abstractmethod
    def terminate_failed_task_query(self, context: dict):
        """Прекратить выполнение запроса после получения fail статуса таска
        @param context: контекст таска
        @return:
        """
