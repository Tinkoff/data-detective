# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Union

from airflow.utils.log.logging_mixin import LoggingMixin
from pandas import DataFrame

from mg_airflow.dag_generator.works.base_work import BaseWork
from mg_airflow.operators.tbaseoperator import TBaseOperator


class ResultType(Enum):
    """Класс для всех типов результатов.
    Разрешается использовать только эти типы результатов.
    Писать типы результатов руками крайне не рекомендуется.
    """

    RESULT_PICKLE = 'pickle'
    RESULT_PG = 'pg'

    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))


class BaseResult(LoggingMixin, metaclass=ABCMeta):
    def __init__(self, operator: TBaseOperator, work: BaseWork, name: str, **kwargs):
        """Базовый класс для всех результатов (result)
        Результат создаётся в конце работы оператора и сохраняется в work
        Оператор всегда возвращает один результат

        :param operator: оператор который генерит результат
        :param work: work, в который будет сохранен результат
        :param name: имя результата, с которым он будет сохранен в work
        :param kwargs: Дополнительные параметры для BaseResult
        """
        del kwargs
        super().__init__()
        self.name = name
        self.operator = operator
        self.work = work
        self.status = 'empty'

    @abstractmethod
    def write_df(self, obj: DataFrame, context: dict):
        """Отвечает за режим работы с DataFrame

        :param obj: объект, который нужно записать в work
        :param context: context
        """

    @abstractmethod
    def write(self, obj: Union[Any, DataFrame], context: dict):
        """Базовая логика записи результата

        :param obj: объект который нужно записать
        :param context: context
        """

    @abstractmethod
    def read_df(self, context: dict) -> DataFrame:
        """Логика чтения DataFrame

        :param context: context
        :return: объект, прочитанный из work
        """

    @abstractmethod
    def read(self, context: dict) -> Union[Any, DataFrame]:
        """Логика чтения любого результата

        :param context: context
        :return: объект, прочитанный из work
        """
