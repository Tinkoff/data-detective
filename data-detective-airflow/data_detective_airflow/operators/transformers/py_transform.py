from typing import Callable, Union

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class PyTransform(TBaseOperator):
    """Perform in-memory conversion

    :param source: List of sources
    :param transformer: Handler (transformer) function
    :param op_kwargs: Additional params for callable
    :param kwargs: Additional params for TBaseOperator
    """

    ui_color = '#39b54a'

    template_fields = ('template_kwargs',)
    template_ext = ('.sql', '.py', '.json', '.yaml')

    def __init__(
        self,
        transformer_callable: Callable,
        source: Union[list[str], None] = None,
        op_kwargs: dict = None,
        template_kwargs: dict = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.transformer = transformer_callable
        self.op_kwargs = op_kwargs or {}
        self.template_kwargs = template_kwargs or {}
        self.source = source or []
        for src in self.source:
            self.dag.task_dict[src] >> self  # pylint: disable=pointless-statement

    def execute(self, context: dict) -> None:
        """Only in-memory mode is supported
        Sources are unpacked before entering the function.
        If there are no sources, the function will not receive them in an empty list.
        This can be avoided by using the following signature: def transformer(context, *sources):

        @param context: Execution context
        """
        src = []
        self.log.info('Reading pickle source(s)')
        if self.source:
            src = [
                self.dag.task_dict[src].result.read(context)
                for src in self.source
                if hasattr(self.dag.task_dict[src], 'result')
            ]
        self.log.info('Executing transformer')
        task_result = self.transformer(context, *src, **self.op_kwargs, **self.template_kwargs)
        self.log.info('Writing pickle result')
        self.result.write_df(task_result, context)
        self.log.info('Finish')
