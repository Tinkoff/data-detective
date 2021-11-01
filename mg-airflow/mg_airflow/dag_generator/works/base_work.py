# -*- coding: utf-8 -*-
"""TBaseWork

Модуль содержит абстрактный базовый классы:
TBaseWork, описывающий логику работы с Work
TWorktype, содержащий все возможные типы ворков
Work - пространство для размещения временных объектов, появляющихся в результате работы Task
"""
from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import Enum
from functools import wraps
from typing import Any, Dict, Optional, Text, Union

from airflow.models.xcom import XCom
from airflow.utils.log.logging_mixin import LoggingMixin

from mg_airflow.constants import CLEAR_WORK_KEY, DAG_ID_KEY, TASK_ID_KEY


class WorkType(Enum):
    """Класс для всех типов ворков.
    Разрешается использовать только эти типы ворков.
    Писать типы ворков руками крайне не рекомендуется.
    """

    WORK_SFTP = 'sftp'
    WORK_FILE = 'file'
    WORK_S3 = 's3'
    WORK_PG = 'pg'

    @classmethod
    def values(cls) -> list[str]:
        return list(map(lambda c: c.value, cls))


def synchronize(func: Callable) -> Callable:
    """Декоратор, который означает, что действие обновит состояние объекта в xcom"""

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        attrs_values = self._get_attrs_from_xcom(context)  # pylint: disable = protected-access
        if attrs_values and isinstance(attrs_values, dict):
            for key, value in attrs_values.items():
                setattr(self, key, value)
        func(self, context, *args, **kwargs)

        to_set = self.get_xcom_params(context)
        XCom.set(**to_set)

    return wrapper


class BaseWork(ABC, LoggingMixin):
    """Базовый класс для работы с work
    work_type: тип work (pickle, pg, gp)
    Надо понимать что для каждого worker будет создан свой экземпляр этого класса, поэтому один
    worker не будет знать что work уже создан в другом worker'е
    """

    XCOM_TASK_KEY = 'work'

    def __init__(self, dag, work_type: str, conn_id: str = None, **kwargs):  # pylint: disable=super-init-not-called
        del kwargs

        self.work_type = work_type
        self.dag = dag
        self.conn_id = conn_id

        """Признак того что метод create уже вызывался для данного worker'а"""
        self._exists = False

    def __hash__(self):
        return hash(f'{self.work_type}-{self.conn_id}')

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_path(self, context: Dict, prefix: str = 'work_airflow'):
        """Вернуть уникальное название work для данного context
        (на самом деле - для данного dag_run)
        Внимание! Название work однозначно определяется dag_run
        """
        if context is not None and 'dag_run' in context and context['dag_run'] is not None:
            return f'{prefix}_{self.dag.dag_id}_{context["dag_run"].id}'
        return f'{prefix}_{self.dag.dag_id}'

    @synchronize
    def create(self, context: dict):
        """Создать ворк физически
        Этот метод не содержит логики создания.
        Логика создания конкретных ворков лежит в _create_logic
        """
        if not self._exists:
            self._create_logic(context)
            self._exists = True

    @synchronize
    def clear(self, context: dict):
        """Удалить ворк физически
        Этот метод не содержит логики удаления.
        Логика удаления конкретных ворков лежит в _clear_logic
        """
        dag_run = context.get('dag_run')
        ex_triggered = False
        clear_work_flg = True
        if dag_run:
            if hasattr(dag_run, 'external_trigger'):
                ex_triggered = dag_run.external_trigger
            if hasattr(dag_run, 'conf'):
                clear_work_flg = dag_run.conf.get(CLEAR_WORK_KEY, clear_work_flg)

        # внешний вызов и установлен флаг "Не очищать work" clear_work = False
        if ex_triggered and not clear_work_flg:
            return
        if self._exists:
            self._clear_logic(context)
            self._exists = False

    def get_xcom_key(self, context: Dict):
        """Вернуть ключ для xcom для текущего ворка и таска"""
        task = context.get('task')
        base_xcom_key = f'{self.work_type}-{self.conn_id}'
        return f'{base_xcom_key}-{task.task_id}' if task else base_xcom_key

    def get_xcom_params(self, context: Dict) -> Dict:
        """Сериализовать work в словарь для записи в XCom"""
        return {
            'key': self.get_xcom_key(context),
            # ключи должны совпадать с одноименными приватными параметрами
            # для корректной работы synchronize
            'value': {
                '_exists': self._exists,
            },
            'execution_date': context['execution_date'],
            TASK_ID_KEY: BaseWork.XCOM_TASK_KEY,
            DAG_ID_KEY: self.dag.dag_id,
        }

    def _get_attrs_from_xcom(self, context: Dict) -> Dict:
        """Вернуть из XCom атрибуты для текущего таска"""
        to_get = self.get_xcom_params(context)
        to_get.pop('value')
        return XCom.get_one(**to_get)

    def set_params(self, params: Optional[dict[str, Any]] = None):
        """Установить параметры для класса"""
        if params is None:
            return
        for key, value in params.items():
            setattr(self, key, value)

    @abstractmethod
    def _create_logic(self, context: Dict):
        """Создать уникальный для context work
        Внимание! В наследниках нужно делать этот метод идемпотентным, т.е. метод можно вызывать
        сколько угодно раз
        Более того, нужно предусмотреть возможность параллельного
        запуска этого метода из разных worker'ов
        Внимание! Название work должно однозначно определяться dag_run
        """

    @abstractmethod
    def _clear_logic(self, context: Dict):
        """Деинициализация"""

    @abstractmethod
    def get_hook(self):
        """Вернуть hook для work connection_id"""

    @abstractmethod
    def get_size(self, path) -> str:
        """Вернуть размер result в читаемом формате"""

    @staticmethod
    def get_readable_size_bytes(size: Union[float, int], decimal_places=2) -> Optional[str]:
        if not isinstance(size, (float, int)):
            return None
        if size < 0:
            return str(size)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
            if size < 1024 or unit == 'YB':
                return f'{size:.{decimal_places}f} {unit}'
            size /= 1024
        return '0 B'
