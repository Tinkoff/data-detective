from typing import Callable, Union

from mg_airflow.operators.tbaseoperator import TBaseOperator


class PyTransform(TBaseOperator):
    """Выполнить преобразование in-memory

    :param source: Список источников
    :param transformer: функция-обработчик
    :param op_kwargs: Дополнительные параметры для callable
    :param kwargs: Дополнительные параметры для TBaseOperator
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
        """Поддерживается только in-memory режим
        Источники перед попаданием в функцию распаковываются.
        Если источников нет - функция их не получит в пустом списке.
        Обыграть это можно с помощью такой сигнатуры: def transformer(context, *sources):

        @param context: Контекст выполнения
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
