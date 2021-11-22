from typing import Dict, List, Optional

import pandas
from airflow.exceptions import AirflowBadRequest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class S3ListBucket(TBaseOperator):
    """
    Lists keys in a bucket under prefix and not containing delimiter
    execute returns
        DataFrame [['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner']]

    :param conn_id: Text
            Connection id
    :param bucket: Text
            Bucket name
    :param prefix: Text
            Limits the response to keys that begin with the specified prefix.
    :param delimiter: Text
            A delimiter is a character you use to group keys.
    :param page_size: int
            Pagination size.
    :param max_items: int
            Maximum items to return.
    :param kwargs: Additional params for TBaseOperator
    """

    ui_color = '#4eb6c2'

    template_fields = ('bucket',)

    def __init__(
        self,
        conn_id: str,
        bucket: str,
        prefix: str = '',
        delimiter: str = '',
        page_size: int = None,
        max_items: int = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.page_size = page_size
        self.max_items = max_items

    def execute(self, context: Optional[dict]):
        """Extended implementation of  airflow.hooks.S3_hook.py:155 list_keys.
        list_keys returns only keys. This implementation returns
        size and date of last modification
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects

        A DataFrame is written to result:
            ['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner']

        :raises AirflowBadRequest: With the non-existent bucket

        :param context: context
        """
        keys: List[Dict] = []
        config = {
            'PageSize': self.page_size,
            'MaxItems': self.max_items,
        }

        hook = S3Hook(self.conn_id)
        client = hook.get_conn()

        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            raise AirflowBadRequest('Bucket {bucket} does not exists'.format(bucket=self.bucket)) from error

        paginator = client.get_paginator('list_objects')
        response = paginator.paginate(
            Bucket=self.bucket, Prefix=self.prefix, Delimiter=self.delimiter, PaginationConfig=config
        )

        for page in response:
            if 'Contents' in page:
                keys.extend(page['Contents'])

        df_data = pandas.DataFrame([], columns=['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner'])
        if len(keys) > 0:
            df_data = pandas.DataFrame(keys)
            df_data.columns = map(str.lower, df_data.columns)

        self.result.write(df_data, context)

    @property
    def include_columns(self):
        return frozenset({'key', 'etag', 'size'})
