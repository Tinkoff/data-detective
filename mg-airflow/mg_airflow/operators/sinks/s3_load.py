from io import BytesIO
from typing import Optional

from airflow.exceptions import AirflowBadRequest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from mg_airflow.operators.tbaseoperator import TBaseOperator


class S3Load(TBaseOperator):
    """Сохранить данные из `bytes_column` по пути `filename_column` с метаданными в `metadata_column`
    в S3 с подключением `conn_id`

    :param source: List
            имя источника, положить в лист
    :param conn_id: Text
            id подключения
    :param bucket: Text
            имя бакета
    :param filename_column: Text
            Имя колонки, содержащей путь к файлу в S3
    :param bytes_column: Text
            Имя колонки, содержащей данные. Тип данных внутри: bytes
    :param metadata_column: Text
            Имя колонки, содержащей метаданные. Сожержимое колонки быть пустым или содержать словарь с метаданными
    :param kwargs: Дополнительные параметры для TBaseOperator
    """

    ui_color = '#dde4ed'

    template_fields = ('bucket',)

    def __init__(
        self,
        source: list,
        conn_id: str,
        bucket: str,
        filename_column: str,
        bytes_column: str,
        metadata_column: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.bucket = bucket
        self.filename_column = filename_column
        self.bytes_column = bytes_column
        self.metadata_column = metadata_column
        self.source = source[0]
        self.source_operator = self.dag.task_dict[self.source]
        self.source_operator >> self  # pylint: disable=pointless-statement

    def execute(self, context: Optional[dict]):
        """Загрузка идет с через boto3.s3.inject.upload_fileobj

        :raises AirflowBadRequest: - при несуществующем бакете
        """
        hook = S3Hook(self.conn_id)
        client = hook.get_conn()

        # Скопировано из airflow.hooks.S3_hook.py:56
        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            raise AirflowBadRequest('Bucket {bucket} does not exists'.format(bucket=self.bucket)) from error

        self.source_operator.result.read(context).apply(
            lambda row: client.upload_fileobj(
                Fileobj=BytesIO(row[self.bytes_column]),
                Bucket=self.bucket,
                Key=row[self.filename_column],
                ExtraArgs=row[self.metadata_column]
                if self.metadata_column and isinstance(row[self.metadata_column], dict)
                else None,
            ),
            axis=1,
        )

    def read_result(self, context):
        return None
