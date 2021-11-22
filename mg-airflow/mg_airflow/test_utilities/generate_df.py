import logging
from io import StringIO
from typing import Dict, List

import psycopg2
from numpy import random as numpy_random
from pandas import DataFrame, Series
from pandas import _testing as pd_tst


def generate_single_dataframe(
    columns: Dict, records_count: int = 2, max_str_len: int = 10, max_val_for_numeric_types: int = 100
) -> DataFrame:
    """A method for generating a dataframe with random data
    :param columns: Dictionary with columns, key - column, value - type (e.g. id: 'int')
    :param records_count: Number of records in the source
    :param max_str_len: Maximum length for string data
    :param max_val_for_numeric_types: Maximum value for numeric types
    :return: DataFrame filled with generated data
    """
    df_new = DataFrame()
    for key, value in columns.items():
        if value == 'int':
            df_new[key] = Series(numpy_random.randint(0, max_val_for_numeric_types, size=records_count))
        elif value == 'str':
            df_new[key] = pd_tst.rands_array(numpy_random.randint(1, max_str_len), records_count)
        else:
            df_new[key] = Series(numpy_random.uniform(0, max_val_for_numeric_types, size=records_count))
    return df_new


def generate_dfs_with_random_data(
    columns: Dict,
    dataframes_count: int = 10,
    records_count: int = 2,
    max_str_len: int = 10,
    max_val_for_numeric_types: int = 100,
) -> List:
    """Method for generating dataframes with random data
    :param columns: Dictionary with columns, key - column, value - type (e.g. id: 'int')
    :param dataframes_count: Number of sources
    :param records_count: Number of records in the source
    :param max_str_len: Maximum length for string data
        :param max_val_for_numeric_types: Maximum value for numeric types
    :return: List of DataFrame with generated data
    """
    return [
        generate_single_dataframe(columns, records_count, max_str_len, max_val_for_numeric_types)
        for _ in range(dataframes_count)
    ]


def fill_table_from_dataframe(conn: psycopg2.extensions.connection, dframe: DataFrame, schema: str, table: str) -> bool:
    """A method for filling a table with data from a dataframe.
    The table must exist.
    :param conn: Connection to the database
    :param dframe: Dataframe with data
    :param schema: The scheme of the table in the database
    :param table: Table name in the database
    :return: Operation execution status: true-completed, otherwise false.
    """
    full_table_name = f'{schema}.{table}'
    trunc_cmd = f'TRUNCATE {full_table_name};'
    sep = ';'

    buffer = StringIO()
    cur = conn.cursor()

    dframe.to_csv(path_or_buf=buffer, index=False, header=False, sep=sep)
    buffer.seek(0)
    try:
        logging.info(f'Run sql: {trunc_cmd}')
        cur.execute(trunc_cmd)
        logging.info('Fill data')
        cur.copy_from(file=buffer, table=full_table_name, sep=sep)
        conn.commit()
        return True
    except psycopg2.DatabaseError as err:
        logging.info(f'Error: {err}')
        conn.rollback()
        logging.info('The table will not change')
        return False
    finally:
        cur.close()
        logging.info('Done')
