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
    """Метод для генерации датафрейма со случайными данными
    :param columns: словарь с колонками, ключ - колонка, значение - тип (прим. id: 'int')
    :param records_count: количество записей в источнике
    :param max_str_len: максимальная длина для строковых данных
    :param max_val_for_numeric_types: максимальное значение для числовых типов
    :return: DataFrame, заполненный сгенерированными данными
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
    """Метод для генерации датафреймов со случайными данными
    :param columns: словарь с колонками, ключ - колонка, значение - тип (прим. id: 'int')
    :param dataframes_count: количество источников
    :param records_count: количество записей в источнике
    :param max_str_len: максимальная длина для строковых данных
    :param max_val_for_numeric_types: максимальное значение для числовых типов
    :return: список из DataFrame с сгенерированными данными
    """
    return [
        generate_single_dataframe(columns, records_count, max_str_len, max_val_for_numeric_types)
        for _ in range(dataframes_count)
    ]


def fill_table_from_dataframe(conn: psycopg2.extensions.connection, dframe: DataFrame, schema: str, table: str) -> bool:
    """Метод для наполнения таблицы данными из датафрейма.
    Таблица должна существовать.
    :param conn: connection для подключения к базе
    :param dframe: датафрейм с данными
    :param schema: схема таблицы в базе
    :param table: название таблицы в базе
    :return: статус выполнения операции: true-выполнено, иначе false.
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
