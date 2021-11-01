from typing import Optional

from airflow.exceptions import AirflowBadRequest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from pandas import DataFrame

from mg_airflow.operators.tbaseoperator import TBaseOperator


class S3Dump(TBaseOperator):
    """Скачать файлы из S3 в колонку DataFrame с именем 'response'

    :param conn_id: Text
            id подключения
    :param bucket: Text
            Имя бакета
    :param object_path: Text
            Путь к объекту для скачивания
    :param source: List
            Оператор может использовать результаты других операторов
            для параметризации своих запросов.
    :param object_column: Text
            Название колонки с именем файла для скачивания
    :param kwargs: Дополнительные параметры для TBaseOperator
    """

    ui_color = '#4eb6c2'

    template_fields = ('bucket',)

    def __init__(
        self,
        conn_id: str,
        bucket: str,
        object_path: str = None,
        source: list = None,
        object_column: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.bucket = bucket
        self.object_path = object_path
        self.object_column = object_column
        self.source = source
        # mypy fix: Value of type "Optional[List[Any]]" is not indexable
        if self.source and len(self.source) == 1:
            self.dag.task_dict[self.source[0]] >> self  # pylint: disable=pointless-statement

    def execute(self, context: Optional[dict]):
        hook = S3Hook(self.conn_id)
        client = hook.get_conn()

        # Скопировано из airflow.hooks.S3_hook.py:56
        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            raise AirflowBadRequest('Bucket {bucket} does not exists'.format(bucket=self.bucket)) from error

        if self.object_path is not None:
            object_data = client.get_object(Bucket=self.bucket, Key=self.object_path)['Body'].read()
            result = DataFrame({'response': object_data}, index=[0])
        # Еще один if из-за 'Value of type "Optional[List[Any]]" is not indexable'
        elif self.source and len(self.source) == 1:
            result = self.dag.task_dict[self.source[0]].result.read(context)
            result['response'] = result[self.object_column].apply(
                lambda row: client.get_object(Bucket=self.bucket, Key=row)['Body'].read()
            )
        self.result.write(result, context)
