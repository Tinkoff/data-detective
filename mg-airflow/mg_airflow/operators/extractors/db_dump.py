from airflow.hooks.base import BaseHook

from mg_airflow.operators.tbaseoperator import TBaseOperator


class DBDump(TBaseOperator):
    """Выполнить SQL код в базе и результат записать в work

    :param conn_id: connection подключения к postgres
    :param sql: Текст sql запроса, который надо выполнить,
                или название файла с запросом в папке code
    :param kwargs: дополнительные параметры TBaseOperator
    """

    ui_color = '#4eb6c2'
    template_fields = ('sql',)
    template_ext = ('.sql',)

    def __init__(self, conn_id: str, sql: str, **kwargs):

        super().__init__(conn_id=conn_id, **kwargs)
        self.sql = sql

    def execute(self, context):
        self.log.info(f'Dumping from query:\n{self.sql}')
        df_data = BaseHook.get_connection(self.conn_id).get_hook().get_pandas_df(self.sql)
        self.log.info('Writing to work')
        self.result.write(df_data, context)
