from typing import Callable

from mg_airflow.operators.tbaseoperator import TBaseOperator


class PythonDump(TBaseOperator):
    """Загрузить данные посредством python-кода

    :param python_callable: функция
    :param op_kwargs: дополнительные параметры для python_callable
    :param kwargs: дополнительные параметры TBaseOperator
    """

    ui_color = '#4eb6c2'

    def __init__(self, python_callable: Callable, op_kwargs: dict = None, **kwargs):

        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}

    def execute(self, context):
        self.log.info('Executing callable')
        task_result = self.python_callable(context, **self.op_kwargs)
        self.log.info('Writing pickle result')
        self.result.write(task_result, context)
        self.log.info('Finish')
