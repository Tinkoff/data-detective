import logging
from typing import Iterable

import petl
from airflow.hooks.base import BaseHook
from pandas import DataFrame


def dump_sql_petl_tupleoftuples(conn_id, sql) -> tuple[tuple]:
    """Выполнить запрос в базе и отдать результат в виде кортежа кортежей

    @param conn_id: ID подключения
    @param sql: Текст запроса
    @return: tuple[tuple]
    """
    hook = BaseHook.get_connection(conn_id).get_hook()
    logging.info(f'Running sql:\n{sql}')
    return petl.fromdb(hook.get_conn(), sql).tupleoftuples()


def appender_petl2pandas(_context, *sources: Iterable[tuple[tuple]]) -> DataFrame:
    """Объединить petl-датасеты и вывести DataFrame

    @param _context: Контекст выполнения. Для запуска в transformer
    @param sources: Список датасетов
    @return: DataFrame
    """
    res = petl.cat(*sources)
    return petl.todataframe(res)


def exploder(row: petl.Record, field: str) -> Iterable[list]:
    """Вспомогательная функция для petl.rowmapmany и unpackdict

    @param row: ячейка со списком
    @param field: название ячейки
    @yield: оставшиеся поля плюс элемент списка ячейки
    """
    save_fields = [row[col] for col in row.flds if col != field]
    for item in row[field]:
        yield save_fields + [item]
