# -*- coding: utf-8 -*-
import pickle
from typing import Any

from data_detective_airflow.dag_generator.results.base_result import BaseResult
from data_detective_airflow.dag_generator.works.base_file_work import BaseFileWork
from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class PickleResult(BaseResult):
    """Processes the result of a task stored in a bit sequence in a file"""

    def __init__(self, operator: TBaseOperator, work: BaseFileWork, name: str, **kwargs):
        super().__init__(operator, work, name)

    def get_file(self, context: dict):
        return self.work.get_path(context) / (self.name + '.p')

    def write_df(self, obj: Any, context: dict):
        """Write the Data Frame to the file storage.
        Sends a call to write

        :param obj: Dataset for writing
        :param context: context
        """
        self.write(obj, context)

    def write(self, obj: Any, context: dict):
        """Write an object to file storage.

        :param obj: Object for writing
        :param context: context
        """
        file = self.get_file(context).as_posix()
        self.work.write_bytes(file, pickle.dumps(obj))
        self.log.info(f'Dumped {self.work.get_size(file)}')
        self.status = 'ready'

    def read_df(self, context: dict):
        """Read DataFrame from file storage.
        Sends a call to work.read

        :param context: context
        """
        self.read(context)

    def read(self, context: dict) -> Any:
        """Read object from file storage.

        :param context: context
        :return: DataFrame
        """
        file = self.get_file(context)
        self.log.info(f'Loading {self.work.get_size(file.as_posix())}')
        return pickle.loads(self.work.read_bytes(file.as_posix()))
