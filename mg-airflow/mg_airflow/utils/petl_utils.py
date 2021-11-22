import logging
from typing import Iterable

import petl
from airflow.hooks.base import BaseHook
from pandas import DataFrame


def dump_sql_petl_tupleoftuples(conn_id, sql) -> tuple[tuple]:
    """Execute a query in the database and give the result as a tuple of tuples

    @param conn_id: Connection id
    @param sql: Query text
    @return: tuple[tuple]
    """
    hook = BaseHook.get_connection(conn_id).get_hook()
    logging.info(f'Running sql:\n{sql}')
    return petl.fromdb(hook.get_conn(), sql).tupleoftuples()


def appender_petl2pandas(_context, *sources: Iterable[tuple[tuple]]) -> DataFrame:
    """Combine petl-datasets and output DataFrame

    @param _context: Execution context. To run in transformer
    @param sources: List of datasets
    @return: DataFrame
    """
    res = petl.cat(*sources)
    return petl.todataframe(res)


def exploder(row: petl.Record, field: str) -> Iterable[list]:
    """Helper function for petl.rowmapmany and unpackdict

    @param row: Cell with a list
    @param field: Cell name
    @yield: Remaining fields and the cell list item
    """
    save_fields = [row[col] for col in row.flds if col != field]
    for item in row[field]:
        yield save_fields + [item]
