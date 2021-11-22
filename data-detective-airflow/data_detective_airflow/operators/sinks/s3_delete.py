from typing import Dict, List, Optional, Text

import numpy
from airflow.exceptions import AirflowBadRequest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class S3Delete(TBaseOperator):
    """Delete from `bucket` objects with key `filename_column`
    in S3 with connection `conn_id`
    Up to 1000 objects can be deleted in one request.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects

    :param source: List
            Name of the source, put in the list
    :param conn_id: Text
            Connection id
    :param bucket: Text
            Bucket name
    :param filename_column: Text
            The name of the column containing the path to the file in S3
    :param batch_size: int
            Number of keys to delete
    :param kwargs: Additional params for the TBaseOperator
    """

    ui_color = '#dde4ed'

    template_fields = ('bucket',)

    def __init__(
        self, source: List, conn_id: Text, bucket: Text, filename_column: Text, batch_size: int = 1000, **kwargs
    ):

        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.bucket = bucket
        self.filename_column = filename_column
        self.batch_size = batch_size
        self.source = source[0]
        self.source_operator = self.dag.task_dict[self.source]
        self.source_operator >> self  # pylint: disable=pointless-statement

    def execute(self, context: Optional[Dict]):
        """Extended the implementation of airflow.hooks.S3_hook.py:559

        :raises AirflowBadRequest: - at a non-existent bucket
        """
        hook = S3Hook(self.conn_id)
        client = hook.get_conn()

        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            raise AirflowBadRequest('Bucket {bucket} does not exists'.format(bucket=self.bucket)) from error

        deleted_cnt = 0
        src_df = self.source_operator.result.read(context)
        if src_df.shape[0] > 0:
            # pylint: disable=unused-variable
            for grp, batch_df in src_df.groupby(numpy.arange(len(src_df)) // self.batch_size):
                delete_dict = {'Objects': [{'Key': k} for k in batch_df[self.filename_column].values.tolist()]}
                response = client.delete_objects(Bucket=self.bucket, Delete=delete_dict)
                if 'Deleted' in response:
                    self.log.info(response['Deleted'])
                    deleted_cnt = deleted_cnt + len(response['Deleted'])

            self.log.info(f'{deleted_cnt} objects were deleted successfully')

    def read_result(self, context):
        return None
