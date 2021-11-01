# -*- coding: utf-8 -*-
import pickle
from typing import Any

from mg_airflow.dag_generator.results.base_result import BaseResult
from mg_airflow.dag_generator.works.base_file_work import BaseFileWork
from mg_airflow.operators.tbaseoperator import TBaseOperator


class PickleResult(BaseResult):
    """Работает с результатом таска, сохраняемым в битовую последовательность в файл"""

    def __init__(self, operator: TBaseOperator, work: BaseFileWork, name: str, **kwargs):
        super().__init__(operator, work, name)

    def get_file(self, context: dict):
        return self.work.get_path(context) / (self.name + '.p')

    def write_df(self, obj: Any, context: dict):
        """Записывает DataFrame в файловое хранилище.
        Передает вызов write

        :param obj: датасет для записи
        :param context: context
        """
        self.write(obj, context)

    def write(self, obj: Any, context: dict):
        """Записывает объект в файловое хранилище.

        :param obj: объект для записи
        :param context: context
        """
        file = self.get_file(context).as_posix()
        self.work.write_bytes(file, pickle.dumps(obj))
        self.log.info(f'Dumped {self.work.get_size(file)}')
        self.status = 'ready'

    def read_df(self, context: dict):
        """Загрузить DataFrame из файлового хранилища.
        Передает вызов work.read

        :param context: context
        """
        self.read(context)

    def read(self, context: dict) -> Any:
        """Загрузить объект из файлового хранилища.

        :param context: context
        :return: DataFrame
        """
        file = self.get_file(context)
        self.log.info(f'Loading {self.work.get_size(file.as_posix())}')
        return pickle.loads(self.work.read_bytes(file.as_posix()))
