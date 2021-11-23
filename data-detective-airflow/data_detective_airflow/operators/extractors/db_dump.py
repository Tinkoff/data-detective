from airflow.hooks.base import BaseHook

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class DBDump(TBaseOperator):
    """Execute the SQL code in the database and write the result to work

    :param conn_id: Connection for postgres
    :param sql: The text of the sql query to be executed,
                or the name of the file with the query in the code folder
    :param kwargs: Additional params for TBaseOperator
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
