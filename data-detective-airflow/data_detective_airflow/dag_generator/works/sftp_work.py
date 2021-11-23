# -*- coding: utf-8 -*-
"""TSFTPWork

The module contains the TSFTPWork class, which implements the logic of working with
file Work on a remote machine via the SFTP protocol
"""

import logging
import stat
from functools import wraps
from pathlib import Path
from tempfile import gettempdir
from typing import Callable, List, Optional

from airflow.providers.ssh.hooks.ssh import SSHHook
from paramiko.sftp_client import SFTPClient

from data_detective_airflow.constants import SFTP_CONN_ID, WORK_FILE_PREFIX
from data_detective_airflow.dag_generator.works.base_file_work import BaseFileWork
from data_detective_airflow.dag_generator.works.base_work import WorkType


def provide_sftp(method: Callable):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        arg_sc = 'sftp_client'

        func_params = method.__code__.co_varnames
        sc_in_args = arg_sc in func_params and func_params.index(arg_sc) < len(args)
        sc_in_kwargs = arg_sc in kwargs

        if sc_in_args or sc_in_kwargs:
            return method(self, *args, **kwargs)  # pylint: disable = not-callable

        sftp_client = self.ssh_client.open_sftp()
        kwargs[arg_sc] = sftp_client
        return method(self, *args, **kwargs)  # pylint: disable = not-callable

    return wrapper


# pylint: disable = arguments-differ
class SFTPWork(BaseFileWork):
    def __init__(self, dag, conn_id: str = SFTP_CONN_ID):
        super().__init__(dag=dag, work_type=WorkType.WORK_SFTP.value, conn_id=conn_id)
        self._ssh_client = None

    def get_path(self, context: Optional[dict], prefix: str = WORK_FILE_PREFIX):
        return Path(gettempdir(), super().get_path(context, prefix))

    @property
    def ssh_client(self):
        if not self._ssh_client:
            self._ssh_client = self.get_hook().get_conn()  # pylint: disable=attribute-defined-outside-init
        return self._ssh_client

    def _create_logic(self, context=None):
        path = self.get_path(context)
        if not self.exists(path.as_posix()):
            logging.info(f'Creating file work directory {path}')
            self.mkdir(path.as_posix())
            logging.info('Created work directory')
        super()._create_logic(context)

    def _clear_logic(self, context=None):
        path = self.get_path(context)
        logging.info(f'Clearing sftp work directory {path}')
        self.rmdir(path.as_posix(), recursive=True)
        logging.info('Clearing work directory')
        self.ssh_client.close()

    @provide_sftp
    def exists(self, path: str, sftp_client: Optional[SFTPClient] = None) -> bool:
        """Check if the path exists on the remote machine
        :param path:
        :param sftp_client:
        :return:
        """
        try:
            sftp_client.stat(path)  # type: ignore
            return True
        except (FileNotFoundError, AttributeError):
            return False

    def execute(self, command: str, sync: bool = False) -> int:
        """Run a command on a remote machine
        :param command:
        :param sync:
        :return:
        """
        stdin, stdout, stderr = self.ssh_client.exec_command(command)
        del stdin, stderr
        if sync:
            return stdout.channel.recv_exit_status()
        return 0

    def mkdir(self, path: str, mode: int = 0o777):
        """Recursive directory creation
        :param path: Path to the directory
        :param mode: Rights granted to directories
        :raises IOError:
        :raises OSError:
        """
        mode_ = oct(mode).replace('0o', '')
        command = f'mkdir -m {mode_} -p {path}'
        exit_code = self.execute(command, sync=True)
        if exit_code != 0:
            raise IOError

    def rmdir(self, path: str, recursive: bool = False):
        """Delete by path on a remote machine
        :param path: Path to the fs object
        :param recursive: Delete nested objects
        :raises IOError:
        :raises OSError:
        """
        command = f'rm -f {path}'
        if recursive:
            command = f'rm -rf {path}'
        exit_code = self.execute(command, sync=True)
        if exit_code != 0:
            raise IOError

    @provide_sftp
    def unlink(self, path: str, sftp_client=None):
        sftp_client.unlink(path)

    @provide_sftp
    def iterdir(self, path: str, sftp_client=None):
        return (pth for pth in self.listdir(path=path, sftp_client=sftp_client))

    @provide_sftp
    def listdir(self, path: str, sftp_client=None) -> List[str]:
        return sftp_client.listdir(path=path)

    @provide_sftp
    def open(self, path, mode='r', encoding=None, buffering=-1, sftp_client=None):
        return sftp_client.open(path, mode=mode, bufsize=buffering)

    @provide_sftp
    def write(self, path: str, text: str, sftp_client: SFTPClient = None):
        """Writing text to a file by the path
        :param path:
        :param text:
        :param sftp_client:
        """
        with self.open(path, mode='w', sftp_client=sftp_client) as file:
            file.write(text)

    @provide_sftp
    def write_bytes(self, path: str, bts: bytes, sftp_client: SFTPClient = None):
        """Writing bytes to a file by the path
        :param path:
        :param bts:
        :param sftp_client:
        """
        with self.open(path, mode='wb', sftp_client=sftp_client) as file:
            file.write(bts)

    @provide_sftp
    def read(self, path: str, sftp_client: SFTPClient = None) -> str:
        """Reading text by the path
        :param path:
        :param sftp_client:
        :return:
        """
        with self.open(path, mode='r', sftp_client=sftp_client) as file:
            return file.read().decode('utf-8')

    @provide_sftp
    def read_bytes(self, path: str, sftp_client: SFTPClient = None) -> bytes:
        """Reading bytes from a file by the path
        :param path:
        :param sftp_client:
        :return:
        """
        with self.open(path, mode='rb', sftp_client=sftp_client) as file:
            return file.read()

    @provide_sftp
    def is_dir(self, path: str, sftp_client: SFTPClient = None) -> bool:
        """Check if the specified path is a directory on the remote machine
        :param path:
        :param sftp_client:
        :raises FileNotFoundError:
        :return:
        """
        if self.exists(path):
            fileattr = sftp_client.lstat(path)  # type: ignore
            return stat.S_ISDIR(fileattr.st_mode)
        raise FileNotFoundError

    @provide_sftp
    def is_file(self, path: str, sftp_client: SFTPClient = None) -> bool:
        """Check if the specified path is a file on the remote machine
        :param path:
        :param sftp_client:
        :return:
        :raises FileNotFoundError:
        """
        if self.exists(path):
            fileattr = sftp_client.lstat(path)  # type: ignore
            return stat.S_ISREG(fileattr.st_mode)
        raise FileNotFoundError

    def get_hook(self):
        return SSHHook(ssh_conn_id=self.conn_id)

    @provide_sftp
    def get_size(self, path: str, sftp_client: SFTPClient = None) -> str:
        """Get the size of the object in the sftp work.
        If the object is missing, it returns -1

        :param path: Object name
        :param sftp_client: Connection
        :return: Rounded object size
        """
        size = -1
        if self.exists(path):
            fileattr = sftp_client.stat(path)  # type: ignore
            size = fileattr.st_size
        return self.get_readable_size_bytes(size)
