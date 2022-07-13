# -*- coding: utf-8 -*-
from typing import Callable

from airflow import DAG
from airflow.models.xcom import XCom
from airflow.utils.context import Context
from airflow.utils.module_loading import import_string

from data_detective_airflow.constants import PG_CONN_ID, S3_CONN_ID, SFTP_CONN_ID
from data_detective_airflow.dag_generator.results import PgResult, PickleResult
from data_detective_airflow.dag_generator.results.base_result import BaseResult, ResultType
from data_detective_airflow.dag_generator.works import FileWork, PgWork, S3Work, SFTPWork
from data_detective_airflow.dag_generator.works.base_work import BaseWork, WorkType
from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class TDag(DAG):
    """The Airflow DAG extension

    :raises Exception, FileNotFoundError:

    :param dag_dir: The path to the directory with the dag file (or its YAML)
    :param factory: The type of factory to generate, if 'None', then the dag was created without generating
    :param kwargs: Additional arguments
    """

    META_FILE = 'meta.yaml'
    CODE_FILE = 'code.py'

    def __init__(self, dag_dir: str, factory: str = 'None', **kwargs):

        kwargs['on_success_callback'] = self.clear_all_works
        kwargs['on_failure_callback'] = self.clear_all_works

        super().__init__(**kwargs)

        self.dag_dir = dag_dir
        self.factory = factory

    def get_result(
        self,
        operator: TBaseOperator,
        result_name: str,
        result_type: str,
        work_type: str,
        work_conn_id: str = None,
        **kwargs,
    ) -> BaseResult:
        """Return result.
        No physical initialization is taking place

        :raises ValueError:
        :return: The result of the operator
        """
        result_type = result_type.lower()
        if result_type not in ResultType.values():
            raise ValueError(f'Invalid result_type value: {result_type}')

        params = {
            **kwargs,
            'operator': operator,
            'name': result_name,
            'work': self.get_work(work_type, work_conn_id),
        }
        if result_type == ResultType.RESULT_PG.value:
            return PgResult(**params)
        return PickleResult(**params)

    def get_work(self, work_type: str = None, work_conn_id: str = None) -> BaseWork:
        """Get work by conn_id.
        Note! Only the class itself is created here, work is not initialized and is not created

        :raises ValueError:
        :return: Work with work_type located in work_conn_id"""
        work_type = work_type or WorkType.WORK_FILE.value
        work_type = work_type.lower()
        if work_type not in WorkType.values():
            raise ValueError(f'Illegal work_type value: {work_type}')

        if work_type == WorkType.WORK_SFTP.value:
            return SFTPWork(self, work_conn_id or SFTP_CONN_ID)
        if work_type == WorkType.WORK_S3.value:
            return S3Work(self, work_conn_id or S3_CONN_ID)
        if work_type == WorkType.WORK_PG.value:
            return PgWork(self, work_conn_id or PG_CONN_ID)
        return FileWork(self)

    def clear_all_works(self, context: Context):
        """Clearing all works after execution"""
        for work in self.get_all_works(context):
            work.clear(context)

    def get_all_works(self, context: Context):
        """Clearing all work on completion of execution"""
        dag_id = self.dag_id
        execution_date = context['logical_date']
        xcoms = XCom.get_many(task_ids='work', dag_ids=dag_id, execution_date=execution_date)
        works = set()
        for xcom in xcoms:
            work = self.get_work(work_type=xcom.key.split('-')[0], work_conn_id=xcom.key.split('-')[1])
            work.set_params(params=dict(xcom.value))
            works.add(work)

        return works

    @property
    def etc_dir(self) -> str:
        return f'{self.dag_dir}/etc'

    @property
    def result_type(self) -> str:
        return self.default_args.get('result_type', ResultType.RESULT_PICKLE.value)

    @property
    def work_type(self) -> str:
        return self.default_args.get('work_type')

    @property
    def work_conn_id(self) -> str:
        return self.default_args.get('work_conn_id')

    @property
    def conn_id(self) -> str:
        return self.work_conn_id

    @property
    def code_dir(self) -> str:
        return f'{self.dag_dir}/code'

    def get_callable_by_def(self, func_def: str) -> Callable:
        """Get a function by its description from yaml. Lambdas are supported.
        If the function is in the project, use it directly.
        If the function is in code.py - take it from there, otherwise from the global

        :param func_def: Function description
        :return: Callable
        """
        try:
            return import_string(func_def)
        except ImportError:
            pass
        module_path = f'{self.code_dir}/{TDag.CODE_FILE}'
        available_modules = ['tests_data', 'dags', 'data_detective_airflow']
        for av_mod in available_modules:
            if av_mod in module_path:
                module_path = f'{av_mod}{module_path.rsplit(av_mod)[-1]}'
                break
        module_path = module_path.replace('/', '.')
        module_path = module_path.rstrip('.py')
        module_path = f'{module_path}.{func_def}'
        if func_def.strip().startswith('lambda'):
            # pylint: disable=eval-used
            return eval(func_def.strip())
        try:
            return import_string(module_path)
        except ImportError:
            return globals()[func_def]
