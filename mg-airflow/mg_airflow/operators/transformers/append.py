from airflow import AirflowException

from mg_airflow.operators.tbaseoperator import TBaseOperator


class Append(TBaseOperator):
    """Объединить несколько объектов в один
    У объекта должен быть метод append

    :param source: Источник
    :param kwargs: Дополнительные параметры для TBaseOperator
    """

    ui_color = '#8f75d1'

    def __init__(self, source: list[str], **kwargs):
        super().__init__(**kwargs)
        self.source = source
        for src in self.source:
            self.dag.task_dict[src] >> self  # pylint: disable=pointless-statement

    def execute(self, context):
        result = None
        self.log.info('Start appending')
        for src in self.source:
            read_result = self.dag.task_dict[src].result.read(context)
            if result is None:
                result = read_result
                if 'append' not in dir(result):
                    raise AirflowException('Object should have "append" method.')
                continue
            result = result.append(read_result, sort=False)
        self.log.info('Writing pickle result')
        self.result.write(result, context)
        self.log.info('Finish')
