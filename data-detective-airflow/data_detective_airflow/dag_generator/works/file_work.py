# -*- coding: utf-8 -*-
import shutil
import tempfile
from pathlib import Path
from typing import IO, Generator, List

from data_detective_airflow.constants import WORK_FILE_PREFIX
from data_detective_airflow.dag_generator.works.base_file_work import BaseFileWork
from data_detective_airflow.dag_generator.works.base_work import WorkType


# pylint: disable = arguments-differ
class FileWork(BaseFileWork):
    """File system work"""

    def __init__(self, dag):
        super().__init__(dag=dag, work_type=WorkType.WORK_FILE.value)

    def get_path(self, context: dict, prefix: str = WORK_FILE_PREFIX) -> Path:
        return Path(Path(tempfile.gettempdir()), super().get_path(context, prefix))

    def _create_logic(self, context=None):
        path = self.get_path(context)
        if not path.exists():
            self.log.info(f'Creating file work directory {path}')
            self.mkdir(path.as_posix(), parents=True, exist_ok=True)
            self.log.info('Created work directory')

    def _clear_logic(self, context=None):
        path = self.get_path(context)
        if self.exists(path.as_posix()):
            self.log.info(f'Cleaning file work directory {path}')
            self.rmdir(path.as_posix(), recursive=True)

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def is_dir(self, path: str) -> bool:
        return Path(path).is_dir()

    def is_file(self, path: str) -> bool:
        return Path(path).is_file()

    def iterdir(self, path: str) -> Generator[str, None, None]:
        return (pth.as_posix() for pth in Path(path).iterdir())

    def listdir(self, path: str) -> List[str]:
        return list(self.iterdir(path))

    def mkdir(self, path: str, parents=False, exist_ok=False, mode=0o777):
        Path(path).mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def read_bytes(self, path: str) -> bytes:
        return Path(path).read_bytes()

    def rmdir(self, path: str, recursive: bool = False):
        if recursive:
            shutil.rmtree(path=path, ignore_errors=True)
        else:
            for file in self.iterdir(path):
                Path(file).unlink()

    def unlink(self, path: str):
        Path(path).unlink(missing_ok=True)

    def write_bytes(self, path: str, bts: bytes):
        Path(path).write_bytes(bts)

    def get_hook(self):
        return None

    def get_size(self, path: str) -> str:
        """Get the size of the object in the local work.
        If the object is missing, it returns -1

        :param path: Object name
        :return: Rounded object size
        """
        size = -1
        if self.exists(path):
            size = Path(path).stat().st_size
        return self.get_readable_size_bytes(size)
