# -*- coding: utf-8 -*-
"""TBaseFileWork

Модуль содержит абстрактный базовый класс TBaseFileWork
Описывает интерфейс work для файловых систем (sftp, s3 и др.)
"""
from abc import abstractmethod
from typing import IO, Generator, List

from mg_airflow.dag_generator.works.base_work import BaseWork


class BaseFileWork(BaseWork):
    """Базовый класс для work на файловой системе (sftp, s3 и др.)"""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Проверить, существует ли объект, находящийся на path
        @param path: путь к объекту ФС
        @return:
        """

    @abstractmethod
    def iterdir(self, path: str) -> Generator[str, None, None]:
        """Вернуть генератор path's по заданному filepath
        @param path: путь к объекту ФС
        @return:
        """

    @abstractmethod
    def listdir(self, path: str) -> List[str]:
        """Вернуть список path's по заданному path
        @param path: путь к объекту ФС
        @return:
        """

    @abstractmethod
    def unlink(self, path: str):
        """Удалить объект по заданному path
        @param path: путь к объекту ФС
        @return:
        """

    @abstractmethod
    def mkdir(self, path: str):
        """Создать директорию по заданному path
        @param path: путь к объекту ФС
        @return:
        """

    @abstractmethod
    def rmdir(self, path: str, recursive: bool = False):
        """Удалить директорию по заданному path
        @param path: путь к объекту ФС
        @param recursive: удалить вложенные файлы и папки
        @return:
        """

    @abstractmethod
    def write_bytes(self, path: str, bts: bytes):
        """Записать байты в файл по заданному path
        @param path: путь к объекту ФС
        @param bts: содержимое для записи
        @return:
        """

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Прочесть содержимое файла по заданному path
        @param path: путь к объекту ФС
        @return:
        """

    @abstractmethod
    def is_dir(self, path: str) -> bool:
        """Проверить, является ли директорией объект по заданному path
        @param path:
        @return:
        """

    @abstractmethod
    def is_file(self, path: str) -> bool:
        """Проверить, является ли файлом объект по заданному path
        @param path:
        @return:
        """
