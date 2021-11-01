# -*- coding: utf-8 -*-
from typing import Any

from pandas import DataFrame

from mg_airflow.dag_generator.results.base_result import BaseResult
from mg_airflow.dag_generator.works.base_db_work import BaseDBWork
from mg_airflow.operators.tbaseoperator import TBaseOperator


class PgResult(BaseResult):
    """Результат сохраняемый в базу данных Postgres"""

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
        """Вернуть полное имя таблицы с work"""
        return self.work.get_path(context) + '.' + self.name

    def write_df(self, obj: DataFrame, context: dict) -> None:
        """Записывает DataFrame в базу

        :param obj: датасет для записи
        :param context: context
        """
        path = self.get_table_name(context)
        self.work.drop(path=path)
        self.log.info(f'Start uploading {path} with {len(obj.index)} rows')
        self.work.write_df(path=path, frame=obj)
        self.log.info(f'Dumped {self.work.get_size(path)}')

    def write(self, obj: Any, context: dict) -> None:
        """Записывает DataFrame в базу.
        Передает вызов write_df

        :param obj: датасет для записи
        :param context: context

        :raises TypeError: если тип входного датасета не DataFrame
        """
        if not isinstance(obj, DataFrame):
            raise TypeError('Only pandas.DataFrame allowed.')
        self.write_df(obj, context)
        self.status = 'ready'

    def read_df(self, context: dict) -> DataFrame:
        """Загрузить DataFrame из базы.
        Передает вызов work.read_df

        :param context: context
        :return: DataFrame
        """
        path = self.get_table_name(context)
        self.log.info(f'Start downloading {path}')
        obj = self.work.read_df(f'SELECT * FROM {path}')
        self.log.info(f'Finish loading {path} with {obj.shape[0]} rows')
        return obj

    def read(self, context: dict) -> DataFrame:
        """Загрузить DataFrame из базы.
        Передает вызов work.read_df

        :param context: context
        :return: DataFrame
        """
        return self.read_df(context)
