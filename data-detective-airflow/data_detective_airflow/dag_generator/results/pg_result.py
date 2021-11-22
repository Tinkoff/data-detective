# -*- coding: utf-8 -*-
from typing import Any

from pandas import DataFrame

from data_detective_airflow.dag_generator.results.base_result import BaseResult
from data_detective_airflow.dag_generator.works.base_db_work import BaseDBWork
from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class PgResult(BaseResult):
    """The result stored in the Postgres database"""

    def __init__(
        self,
        operator: TBaseOperator,
        work: BaseDBWork,
        name: str,
        obj_type: str = 'table',
        analyze: str = None,
        **kwargs,
    ):
        super().__init__(operator, work, name, **kwargs)
        self.obj_type = obj_type
        self.analyze = analyze

    def get_table_name(self, context):
        """Return the full name of the table with work"""
        return self.work.get_path(context) + '.' + self.name

    def write_df(self, obj: DataFrame, context: dict) -> None:
        """Write DataFrame to the database

        :param obj: Dataset for writing
        :param context: context
        """
        path = self.get_table_name(context)
        self.work.drop(path=path)
        self.log.info(f'Start uploading {path} with {len(obj.index)} rows')
        self.work.write_df(path=path, frame=obj)
        self.log.info(f'Dumped {self.work.get_size(path)}')

    def write(self, obj: Any, context: dict) -> None:
        """Write DataFrame to the database.
        Sends a call to write_df

        :param obj: Dataset for writing
        :param context: context

        :raises TypeError: If the input dataset type is not DataFrame
        """
        if not isinstance(obj, DataFrame):
            raise TypeError('Only pandas.DataFrame allowed.')
        self.write_df(obj, context)
        self.status = 'ready'

    def read_df(self, context: dict) -> DataFrame:
        """Read DataFrame from teh database.
        Sends a call to work.read_df

        :param context: context
        :return: DataFrame
        """
        path = self.get_table_name(context)
        self.log.info(f'Start downloading {path}')
        obj = self.work.read_df(f'SELECT * FROM {path}')
        self.log.info(f'Finish loading {path} with {obj.shape[0]} rows')
        return obj

    def read(self, context: dict) -> DataFrame:
        """Read DataFrame from the database.
        Sends a call to work.read_df

        :param context: context
        :return: DataFrame
        """
        return self.read_df(context)
