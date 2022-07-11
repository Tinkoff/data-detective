from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Dict

from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models import BaseOperator
from airflow.utils.context import Context

from data_detective_airflow.constants import EXECUTION_TIMEOUT
from data_detective_airflow.dag_generator.works.base_db_work import BaseDBWork
from data_detective_airflow.utils.logging_thread import LoggingThread


# pylint: disable=too-many-instance-attributes
class TBaseOperator(BaseOperator, ABC):
    """Base operator.
    All other operators need to inherit from this one.

    :param description: Task description
    :param conn_id: Optional connection for connecting to a particular source
    :param work_conn_id: Optional connection work for the operator (taken from the dag by default)
    :param result_type: Optional result type/format (taken from dag by default)
    :param work_type: Optional work type/format (taken from dag by default)
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

        #  In Airflow 2.0, passing kwargs and args to the base operator constructor will cause an exception
        #  Here the extra arguments are filtered, but written to a separate variable
        #  So that the original kvargs remains accessible and unchanged
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
        """Get conn_id from the task or from default DAG settings"""
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
        """Execute the operator"""

    @prepare_lineage
    def pre_execute(self, context: Context = None):
        """The method is called before calling self.execute()"""
        work = self.dag.get_work(self.work_type, self.work_conn_id)
        work.create(context)
        self.log.info(f'{self.task_id} started')
        if self.logging_thread_flg:
            self.thread = LoggingThread(context=context)

    @apply_lineage
    def post_execute(self, context: Context = None, result=None):
        """The method is called immediately after calling self.execute()"""
        if self.thread:
            self.thread.stop()
        self.log.info(f'{self.task_id} finished')

    @property
    def exclude_columns(self):
        return frozenset({'processed_dttm'})

    @property
    def include_columns(self):
        return frozenset()

    def read_result(self, context):
        """Read the result. Used in tests.
        In statements that do not write to work, you need to redefine.

        :param context: Execution context
        :return: Dataset
        """
        return self.result.read(context)
