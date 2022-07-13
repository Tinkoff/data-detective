from typing import Union

from airflow.utils.context import Context

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class PgSQL(TBaseOperator):
    """The class executes sql in the specified Postgres database
    Creates a table or view based on the passed request

    :param sql: SQL query
    :param source: Source
    :param obj_type: Type of the creating object: table, view
    :param analyze: Analyze (for tables only) (None | * | 'col1, col2, ..., colN')
    :param kwargs: Additional params for TBaseOperator
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

    def execute(self, context: Context):
        """Execute the query to the Postgres

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
