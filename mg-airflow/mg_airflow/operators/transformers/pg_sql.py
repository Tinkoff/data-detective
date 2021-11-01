from typing import Union

from mg_airflow.operators.tbaseoperator import TBaseOperator


class PgSQL(TBaseOperator):
    """Класс выполняет sql в указанной БД Postgres
    Создает таблицу или представление на основании переданного запроса

    :param sql: sql-код, который надо выполнить
    :param source: Источник
    :param obj_type: тип создаваемого объекта: table, view
    :param analyze: анализировать (только для таблиц) (None | * | 'col1, col2, ..., colN')
    :param kwargs: Дополнительные параметры для TBaseOperator
    """

    template_fields = ('sql', 'source')
    template_ext = ('.sql',)

    ui_color = '#8f75d1'

    def __init__(
        self, sql: str, source: Union[list[str], None] = None, obj_type: str = 'table', analyze: str = None, **kwargs
    ):

        super().__init__(
            **dict(kwargs, obj_type=obj_type.strip().lower(), analyze=analyze.strip().lower() if analyze else analyze)
        )
        self.conn_id = self.conn_id or self.work_conn_id
        self.sql = sql
        self.obj_type = obj_type.strip().lower()
        self.analyze = analyze.strip().lower() if analyze else analyze
        self.source = source or []
        for src in self.source:
            self.dag.task_dict[src] >> self  # pylint: disable=pointless-statement

    def execute(self, context: dict):
        """Выполнить запрос в Postgres

        :param context: context
        """
        path = self.result.get_table_name(context)
        work = self.result.work
        work.drop(path=path)
        self.log.info(f'Creating {path} in pass-through mode')
        work.execute(f'CREATE {self.obj_type} {path} AS {self.sql}', context=context)
        if self.obj_type == 'table' and self.analyze:
            sql_analyze = f'ANALYZE {path} ({self.analyze});'
            if self.analyze == '*':
                sql_analyze = f'ANALYZE {path};'
            work.execute(sql=sql_analyze, context=context)
        self.log.info('Finish creating')
