from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models import BaseOperator, TaskInstance

from mg_airflow.constants import EXECUTION_TIMEOUT
from mg_airflow.dag_generator.works.base_db_work import BaseDBWork
from mg_airflow.utils.logging_thread import LoggingThread


# pylint: disable=too-many-instance-attributes
class TBaseOperator(BaseOperator, ABC):
    """Базовый оператор в mg-airflow
    Все остальные операторы нужно наследовать это этого.

    :param description: Описание таска
    :param conn_id: Опциональный connection для подключения к тому или иному источнику
    :param work_conn_id: Опциональный connection work для оператора (по умолчанию берется из dag)
    :param result_type: Опциональный тип/формат результата (по умолчанию берется из dag)
    :param work_type: Опциональный тип/формат ворка (по умолчанию берется из dag)
    """

    def __init__(
        self,
        description: str = None,
        conn_id: str = None,
        work_conn_id: str = None,
        result_type: str = None,
        work_type: str = None,
        **kwargs,
    ):

        #  В Airflow 2.0 передача kwargs и args в конструктор baseoperator вызовет исключение
        #  Здесь лишние аргументы фильтруются, но записываются в отдельную переменную
        #  Чтобы оригинальный kwargs оставался доступным и неизменным
        allowed_params = inspect.signature(super().__init__).parameters
        _kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}
        self._set_execution_timeout(_kwargs)

        super().__init__(do_xcom_push=False, **_kwargs)

        self.description = description
        self.conn_id = conn_id
        self.work_conn_id = work_conn_id
        self.result_type = result_type
        self.work_type = work_type

        self.result = self.dag.get_result(
            self,
            result_name=self.task_id,
            result_type=self.result_type,
            work_conn_id=self.work_conn_id,
            work_type=self.work_type,
            **kwargs,
        )
        self._set_on_failure_callback()

        self.logging_thread_flg = kwargs.get('logging_thread_flg', True)
        self.thread = None  # pylint: disable=too-many-instance-attributes

    def get_conn_id(self) -> str:
        """Получить conn_id из task или из настроек DAG по умолчанию"""
        if hasattr(self, 'conn_id'):
            return getattr(self, 'conn_id')
        return self.dag.default_args.get('work_conn_id')

    @staticmethod
    def _set_execution_timeout(kwargs: Dict[str, Any]):
        execution_timeout = kwargs.get('execution_timeout')
        converted_timeout = timedelta(seconds=EXECUTION_TIMEOUT)
        if isinstance(execution_timeout, timedelta):
            converted_timeout = execution_timeout
        elif isinstance(execution_timeout, int) or (
            isinstance(execution_timeout, str) and execution_timeout.isnumeric()
        ):
            converted_timeout = timedelta(seconds=int(execution_timeout))
        kwargs['execution_timeout'] = converted_timeout

    def _set_on_failure_callback(self):
        work = self.dag.get_work(self.work_type, self.work_conn_id)
        if isinstance(work, BaseDBWork):
            self.on_failure_callback = work.terminate_failed_task_query

    @abstractmethod
    def execute(self, context):
        """Выполнить оператор"""

    @prepare_lineage
    def pre_execute(self, context: dict = None):
        """Метод вызывается до вызова self.execute()"""
        work = self.dag.get_work(self.work_type, self.work_conn_id)
        work.create(context)
        self.log.info(f'{self.task_id} started')
        if self.logging_thread_flg:
            self.thread = LoggingThread(context=context)

    @apply_lineage
    def post_execute(self, context: dict = None, result=None):
        """Метод вызывается сразу после вызова self.execute()"""
        if self.thread:
            self.thread.stop()
        self.log.info(f'{self.task_id} finished')

    def generate_context(self, execution_date=datetime.now()) -> Dict[str, Any]:
        """Сгенерировать контекст на произвольное execution_date."""
        task_instance = TaskInstance(task=self, execution_date=execution_date)
        return task_instance.get_template_context()

    @property
    def exclude_columns(self):
        return frozenset({'processed_dttm'})

    @property
    def include_columns(self):
        return frozenset()

    def read_result(self, context):
        """Прочитать result. Используется в тестах.
        В операторах, которые не пишут в work, нужно переопределить.

        :param context: Контекст выполнения
        :return: датасет
        """
        return self.result.read(context)
