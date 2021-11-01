from collections import namedtuple
from time import sleep, time
from os.path import join as os_path_join

import logging
import pytest

from paramiko import SSHClient
from paramiko.ssh_exception import SSHException
from airflow.providers.ssh.hooks.ssh import SSHHook
from tests_data.operators.extractors.tsftp_dump_data import jobs_df, users_df, cyrillic_df, special_df


WORK_DIR = r'/opt/tsftp/test_data'

file_param = namedtuple("file_param", ["path", "data", "error"])


def __exec_sftp_cmd(sftp: SSHClient, cmd: str, wait_sec=30) -> bool:
    """Вспомогательный метад для выполения команды на удаленном сервере
    :param sftp: - клиент для подключения по SSH
    :param cmd: - строка с командой, которую необходимо выполнить
    :param wait_sec: - отведенноое команде количество секунд, иначе останавливаем
    :return: признак успешного выполнения команды
    """
    sleep_time = 1
    start_time = time()
    end_time = start_time + wait_sec
    scs = False

    try:
        logging.info(f'TEST: Run the command: {cmd} .')
        stdin, stdout, stderr = sftp.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            sleep(sleep_time)
            cur_time = time()
            if cur_time > end_time:
                return scs
        scs = True
    except (SSHException, StopIteration, Exception) as exc:
        logging.error(f'TEST: The command {cmd} was not executed. Details {exc}.')
    return scs


@pytest.fixture(scope='session')
def prepare_data_files():
    """Фикстура с подготовкой файловых данных для тестов"""
    files = {}
    file_data = None
    work_dir_path = WORK_DIR
    ssh_hook = SSHHook(ssh_conn_id='ssh_service')
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()

        # Проверяем наличие директории и создаем, если не существует
        try:
            stat_info = sftp_client.stat(work_dir_path)
            work_dir_exists = True
        except (IOError, Exception) as exc:
            work_dir_exists = False
        if not work_dir_exists:
            create_dirs_cmd = f"mkdir -p {work_dir_path}"
            scs = __exec_sftp_cmd(ssh_client, create_dirs_cmd)
            if scs:
                work_dir_exists = True
            else:
                logging.error(f'TEST: Working directory: {work_dir_path} was not created.')

        # К созданию файлов переходим только при наличии директории
        if work_dir_exists:
            logging.info(f'TEST: Working directory: {work_dir_path}.')

            # файл с кириллицей в имени
            file_path = os_path_join(work_dir_path, "джобы.csv")
            with sftp_client.open(filename=file_path, mode='w') as f:
                file_data = jobs_df.to_string(index=False)
                f.write(file_data)
            files["cyrillic name"] = file_param(file_path, file_data, None)
            logging.info(f'TEST: The "{file_path}" file was created successfully.')

            # пустой файл
            file_path = os_path_join(work_dir_path, "empty.txt")
            with sftp_client.file(filename=file_path, mode='w') as f:
                pass
            files["empty file"] = file_param(file_path, None, None)
            logging.info(f'TEST: The "{file_path}" file was created successfully.')

            # файл, к которому нет прав на чтение
            file_path = os_path_join(work_dir_path, "secret.csv")
            with sftp_client.file(filename=file_path, mode='w') as f:
                f.write(users_df.to_string(index=False))
            sftp_client.chmod(file_path, 0o010)
            files["without read permissions"] = file_param(file_path, None, PermissionError)
            logging.info(f'TEST: The "{file_path}" file was created successfully.')

            # файл с кириллицей
            file_path = os_path_join(work_dir_path, "cyrillic.csv")
            with sftp_client.file(filename=file_path, mode='w') as f:
                file_data = cyrillic_df.to_string(index=False)
                f.write(file_data)
            files["cyrillic"] = file_param(file_path, file_data, None)
            logging.info(f'TEST: The "{file_path}" file was created successfully.')

            # файл со спецсимволами
            file_path = os_path_join(work_dir_path, "special.csv")
            with sftp_client.file(filename=file_path, mode='w') as f:
                file_data = special_df.to_string(index=False)
                f.write(file_data)
            files["special characters"] = file_param(file_path, file_data, None)
            logging.info(f'TEST: The "{file_path}" file was created successfully.')

            # путь смотрит на директорию, а не на файл
            files["directory"] = file_param(work_dir_exists, None, TypeError)

            # несуществующий файл
            file_path = os_path_join(work_dir_path, "file.data")
            files["non-existent file"] = file_param(file_path, None, FileNotFoundError)
            logging.info(f'TEST: The "{file_path}" file was not created.')

            # файл c данными
            files["real file"] = file_param("/opt/common.sh",
                                            "#!/bin/bash\\nDUMMY=echo\n",
                                            None)

            yield files

            # полностью удаляем директорию
            drop_cmd = f"rm -rf {work_dir_path}"
            scs = __exec_sftp_cmd(ssh_client, drop_cmd)
            if scs:
                logging.info(f'TEST: Working directory: {work_dir_path} was deleted.')
            else:
                logging.error(f'TEST: Working directory: {work_dir_path} was not deleted.')


def get_keys_of_correct_files():
    """Вспомогательный метод для получения ключей файлов с валидными данными
    :return: список ключей
    """
    correct_file_pars = []
    correct_file_pars.append("real file")
    correct_file_pars.append("cyrillic name")
    correct_file_pars.append("cyrillic")
    correct_file_pars.append("special characters")
    return correct_file_pars


def get_keys_of_empty_files():
    """Вспомогательный метод для получения ключей пустых файлов
    :return: список ключей
    """
    file_pars = []
    file_pars.append("empty file")
    return file_pars


def get_keys_of_invalid_files():
    """Вспомогательный метод для получения ключей невалидных файлов
    :return: список ключей
    """
    file_pars = []
    file_pars.append("without read permissions")
    file_pars.append("directory")
    file_pars.append("non-existent file")
    return file_pars
