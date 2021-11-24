# -*- coding: utf-8 -*-
"""TBaseWork

The module contains abstract base classes:
TBaseWork, describing the logic of working with Work
TWorktype, containing all possible types of Works
Work - a place to store temporary objects that appear as a result of the Task
"""
from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import Enum
from functools import wraps
from typing import Any, Dict, Optional, Text, Union

from airflow.models.xcom import XCom
from airflow.utils.log.logging_mixin import LoggingMixin

from data_detective_airflow.constants import CLEAR_WORK_KEY, DAG_ID_KEY, TASK_ID_KEY


class WorkType(Enum):
    """A class for all types of works.
    It is allowed to use only these types of works.
    Writing types of works is highly not recommended..
    """

    WORK_SFTP = 'sftp'
    WORK_FILE = 'file'
    WORK_S3 = 's3'
    WORK_PG = 'pg'

    @classmethod
    def values(cls) -> list[str]:
        return list(map(lambda c: c.value, cls))


def synchronize(func: Callable) -> Callable:
    """Decorator, which means that the action is to update the state of the object in xcom"""

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
    """Base class for processing work
    work_type: Work type (pickle, pg, gp)
    Note: each worker will have its own instance of this class created, so one worker will not know that a work has already been created in another worker
    """

    XCOM_TASK_KEY = 'work'

    def __init__(self, dag, work_type: str, conn_id: str = None, **kwargs):  # pylint: disable=super-init-not-called
        del kwargs

        self.work_type = work_type
        self.dag = dag
        self.conn_id = conn_id

        """Indicates that the create method has already been called for this worker"""
        self._exists = False

    def __hash__(self):
        return hash(f'{self.work_type}-{self.conn_id}')

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_path(self, context: Dict, prefix: str = 'work_airflow'):
        """Return a unique work name for this context
        (for dag_run)
        Important note! The name of the work is uniquely determined by dag_run
        """
        if context is not None and 'dag_run' in context and context['dag_run'] is not None:
            return f'{prefix}_{self.dag.dag_id}_{context["dag_run"].id}'
        return f'{prefix}_{self.dag.dag_id}'

    @synchronize
    def create(self, context: dict):
        """Create a work.
        This method does not contain creation logic.
        The logic of creating specific works in _create_logic
        """
        if not self._exists:
            self._create_logic(context)
            self._exists = True

    @synchronize
    def clear(self, context: dict):
        """Delete a work
        This method does not contain delete logic.
        The logic of deleting specific works in _clear_logic
        """
        dag_run = context.get('dag_run')
        ex_triggered = False
        clear_work_flg = True
        if dag_run:
            if hasattr(dag_run, 'external_trigger'):
                ex_triggered = dag_run.external_trigger
            if hasattr(dag_run, 'conf'):
                clear_work_flg = dag_run.conf.get(CLEAR_WORK_KEY, clear_work_flg)

        # An external call with the "Do not clear work" flag is set to clear_work = False
        if ex_triggered and not clear_work_flg:
            return
        if self._exists:
            self._clear_logic(context)
            self._exists = False

    def get_xcom_key(self, context: Dict):
        """Return the key for XCom for the current work and task"""
        task = context.get('task')
        base_xcom_key = f'{self.work_type}-{self.conn_id}'
        return f'{base_xcom_key}-{task.task_id}' if task else base_xcom_key

    def get_xcom_params(self, context: Dict) -> Dict:
        """Serialize work to a dictionary for writing to XCom"""
        return {
            'key': self.get_xcom_key(context),
            # the keys must match the private parameters of the same name
            # to synchronize correctly
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
        """Create a work unique for the context.
        Important note! The inheritors need to make this method idempotent, i.e. the method can be called
        as many times as needed.
        Moreover, it is necessary to provide for the possibility of parallel
        running this method from different workers.
        Note! The name of the work must be uniquely determined by dag_run
        """

    @abstractmethod
    def _clear_logic(self, context: Dict):
        """Deinitialization"""

    @abstractmethod
    def get_hook(self):
        """Return the hook for work connection_id"""

    @abstractmethod
    def get_size(self, path) -> str:
        """Return the size of result in a readable format"""

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
