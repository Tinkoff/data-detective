from typing import Optional

from airflow.exceptions import AirflowBadRequest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from pandas import DataFrame

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class S3Dump(TBaseOperator):
    """Download files from S3 to DataFrame column named 'response'

    :param conn_id: Text
            Connection id
    :param bucket: Text
            Bucket name
    :param object_path: Text
            Path to the object
    :param source: List
            The operator can use the results of other operators
            to parameterize their queries.
    :param object_column: Text
            The name of the column with the name of the file to download
    :param kwargs: Additional params for TBaseOperator
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

        # Copied from airflow.hooks.S3_hook.py:56
        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            raise AirflowBadRequest('Bucket {bucket} does not exists'.format(bucket=self.bucket)) from error

        if self.object_path is not None:
            object_data = client.get_object(Bucket=self.bucket, Key=self.object_path)['Body'].read()
            result = DataFrame({'response': object_data}, index=[0])
        # Another if due to 'Value of type "Optional[List[Any]]" is not indexable'
        elif self.source and len(self.source) == 1:
            result = self.dag.task_dict[self.source[0]].result.read(context)
            result['response'] = result[self.object_column].apply(
                lambda row: client.get_object(Bucket=self.bucket, Key=row)['Body'].read()
            )
        self.result.write(result, context)
