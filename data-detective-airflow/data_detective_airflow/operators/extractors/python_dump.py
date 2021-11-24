from typing import Callable

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class PythonDump(TBaseOperator):
    """Download data using python code

    :param python_callable: Python function
    :param op_kwargs: Additional params for python_callable
    :param kwargs: Additional params for TBaseOperator
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
