from airflow.utils.context import Context
from pandas import DataFrame, concat


def append_dfs(_context: Context, *sources: DataFrame) -> DataFrame:
    """Append several dfs in one
    :param _context: Airflow context
    :param sources: dfs
    :return:
    """
    return concat([*sources])
