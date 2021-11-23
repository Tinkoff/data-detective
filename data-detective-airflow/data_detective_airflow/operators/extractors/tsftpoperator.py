from airflow.providers.ssh.hooks.ssh import SSHHook

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class TSFTPOperator(TBaseOperator):
    """Get the file `remote_filepath` from the server with `conn_id`
    :param conn_id: Name of the Airflow `Connection`
    :param remote_filepath: Path to the file
    :param kwargs: Additional params for TBaseOperator
    """

    ui_color = '#4eb6c2'
    template_fields = ('remote_filepath',)

    def __init__(self, conn_id: str = None, remote_filepath: str = None, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)
        self.remote_filepath = remote_filepath

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            with sftp_client.file(filename=self.remote_filepath, mode='r') as remote_file:
                self.result.write(remote_file.read(), context=context)
