# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Union

from airflow.utils.log.logging_mixin import LoggingMixin
from pandas import DataFrame

from data_detective_airflow.dag_generator.works.base_work import BaseWork
from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class ResultType(Enum):
    """A class for all types of results.
    Only these types of results are allowed to be used.
    Try to avoid writing the result types manually
    """

    RESULT_PICKLE = 'pickle'
    RESULT_PG = 'pg'

    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))


class BaseResult(LoggingMixin, metaclass=ABCMeta):
    def __init__(self, operator: TBaseOperator, work: BaseWork, name: str, **kwargs):
        """Base class for all results.
        The result is created at the end of the operator's work and stored into work.
        The operator always returns one result

        :param operator: Operator that generates the result
        :param work: Work in which the result will be saved
        :param name: Name of the result with which it will be saved in work
        :param kwargs: Additional parameters for BaseResult
        """
        del kwargs
        super().__init__()
        self.name = name
        self.operator = operator
        self.work = work
        self.status = 'empty'

    @abstractmethod
    def write_df(self, obj: DataFrame, context: dict):
        """This method is responsible for working with the DataFrame

        :param obj: object to be written to work
        :param context: context
        """

    @abstractmethod
    def write(self, obj: Union[Any, DataFrame], context: dict):
        """Basic logic of writing the result

        :param obj: Object to be written
        :param context: context
        """

    @abstractmethod
    def read_df(self, context: dict) -> DataFrame:
        """Read DataFrame

        :param context: context
        :return: Object read from work
        """

    @abstractmethod
    def read(self, context: dict) -> Union[Any, DataFrame]:
        """Reading of any result

        :param context: context
        :return: Object read from work
        """
