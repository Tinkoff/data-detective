from io import BytesIO
from typing import Optional

from airflow.exceptions import AirflowBadRequest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class S3Load(TBaseOperator):
    """Save data forom `bytes_column` by the path `filename_column` with metadata in `metadata_column`
    in S3 with connection `conn_id`

    :param source: List
            Name of the source, put in the list
    :param conn_id: Text
            Connection id
    :param bucket: Text
            Bucket name
    :param filename_column: Text
            The name of the column containing the path to the file in S3
    :param bytes_column: Text
            The name of the column containing the data. Data type inside: bytes
    :param metadata_column: Text
            The name of the column containing metadata. The column content should be empty or contain a dictionary with metadata.
    :param kwargs: Additional params for the TBaseOperator
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
        """The download comes from via boto3.s3.inject.upload_fileobj

        :raises AirflowBadRequest: - at a non-existent bucket
        """
        hook = S3Hook(self.conn_id)
        client = hook.get_conn()

        # Copied form airflow.hooks.S3_hook.py:56
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
