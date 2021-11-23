# -*- coding: utf-8 -*-
"""TBaseFileWork

The module contains an abstract base class TBaseFileWork
Describes the work interface for file systems (sftp, s3, etc.)
"""
from abc import abstractmethod
from typing import IO, Generator, List

from data_detective_airflow.dag_generator.works.base_work import BaseWork


class BaseFileWork(BaseWork):
    """Base class for work on file system (sftp, s3 и др.)"""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if an object located on path exists
        @param path: The path to the fs object
        @return:
        """

    @abstractmethod
    def iterdir(self, path: str) -> Generator[str, None, None]:
        """Return the path's generator by the specified filepath
        @param path: The path to the fs object
        @return:
        """

    @abstractmethod
    def listdir(self, path: str) -> List[str]:
        """Return the path's list by the provided path
        @param path: The path to the fs object
        @return:
        """

    @abstractmethod
    def unlink(self, path: str):
        """Delete an object by the provided path
        @param path: The path to the fs object
        @return:
        """

    @abstractmethod
    def mkdir(self, path: str):
        """Create a directory by the provided path
        @param path: The path to the fs object
        @return:
        """

    @abstractmethod
    def rmdir(self, path: str, recursive: bool = False):
        """Delete a directory by the provided path
        @param path: The path to the fs object
        @param recursive: Delete attached files and folders
        @return:
        """

    @abstractmethod
    def write_bytes(self, path: str, bts: bytes):
        """Write bytes to a file using the provided path
        @param path: The path to the fs object
        @param bts: Content to write
        @return:
        """

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read bytes to a file using the provided path
        @param path: The path to the fs object
        @return:
        """

    @abstractmethod
    def is_dir(self, path: str) -> bool:
        """Check whether an object is a directory by the provided path
        @param path:
        @return:
        """

    @abstractmethod
    def is_file(self, path: str) -> bool:
        """Check if an object is a file by the provided path
        @param path:
        @return:
        """
