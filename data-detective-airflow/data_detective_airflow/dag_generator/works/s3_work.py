# -*- coding: utf-8 -*-
"""S3Work

The module contains the S3Work class, which is the inheritor of TBaseFileWork, implementing the logic of working with
file Work in S3 object storage
"""
from pathlib import Path
from typing import Optional, IO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from data_detective_airflow.constants import S3_CONN_ID, WORK_S3_BUCKET, WORK_S3_PREFIX
from data_detective_airflow.dag_generator.works.base_file_work import BaseFileWork
from data_detective_airflow.dag_generator.works.base_work import WorkType


# pylint: disable = arguments-differ
class S3Work(BaseFileWork):
    """Work based on s3"""

    def __init__(self, dag, conn_id: str = S3_CONN_ID):
        super().__init__(dag=dag, work_type=WorkType.WORK_S3.value, conn_id=conn_id)
        self._hook = None

    @property
    def bucket(self):
        return WORK_S3_BUCKET

    @property
    def hook(self):
        if not self._hook:
            self._hook = self.get_hook()
        return self._hook

    def get_path(self, context: Optional[dict], prefix: str = WORK_S3_PREFIX):
        return Path(self.bucket, super().get_path(context, prefix))

    def _create_logic(self, context: dict = None):
        """Initialization"""
        self.log.info('Init s3 work')
        self.log.info(f'S3Work is {self.get_path(context)}')

    def _clear_logic(self, context: dict = None):
        """Deinitialization"""
        path = self.get_path(context=context)
        self.log.info(f'Cleaning s3 work directory {path}')
        for file in self.listdir(path.as_posix()):
            self.unlink(file)

    def exists(self, path):
        bucket = self.hook.get_bucket(self.bucket)
        return any(bucket.objects.filter(Prefix=path))

    def iterdir(self, path):
        bucket = self.hook.get_bucket(self.bucket)
        return (obj.key for obj in bucket.objects.filter(Prefix=path))

    def listdir(self, path):
        return list(self.iterdir(path))

    def unlink(self, path):
        bucket = self.hook.get_bucket(self.bucket)
        try:
            bucket.meta.client.delete_object(Bucket=self.bucket, Key=path)
        except ClientError as error:
            raise OSError('/{0}/{1}'.format(self.bucket, path)) from error

    def mkdir(self, _path: str):
        self.log.info('There are no directories in S3. You can only create a bucket')
        if not self.hook.check_for_bucket(self.bucket):
            self.hook.create_bucket(Bucket=self.bucket)
            self.log.info(f'Bucket {self.bucket} has been created')
        else:
            self.log.info(f'Bucket {self.bucket} already exists')

    def rmdir(self, path: str, recursive: bool = False):
        self.log.info('There are no directories in S3')
        if recursive:
            self.log.info(f'Remove every object with prefix {path}')
            for pth in self.iterdir(path):
                self.unlink(pth)
        raise FileNotFoundError

    def write_bytes(self, path, bts: bytes):
        self.hook.load_bytes(bytes_data=bts, key=path, bucket_name=self.bucket, replace=True)

    def read_bytes(self, path):
        return self.hook.get_key(key=path, bucket_name=self.bucket).get()['Body'].read()

    def is_dir(self, path) -> bool:
        bucket = self.hook.get_bucket(self.bucket)
        if not self.is_file(path):
            return any(bucket.objects.filter(Prefix=path))
        return False

    def is_file(self, path):
        return self.hook.check_for_key(key=path, bucket_name=self.bucket)

    def get_hook(self):
        return S3Hook(aws_conn_id=self.conn_id)

    def get_size(self, path: str) -> str:
        """Get the size of the object in s3.
        If the object is missing, it returns -1

        :param path: Object name
        :return: Rounded object size
        """
        size = -1
        if self.exists(path):
            object_metadata = self.hook.head_object(path, bucket_name=self.bucket)
            if 'ContentLength' in object_metadata:
                size = object_metadata['ContentLength']
        return self.get_readable_size_bytes(size)
