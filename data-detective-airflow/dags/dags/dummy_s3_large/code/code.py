from io import StringIO

from data_detective_airflow.test_utilities.generate_df import generate_single_dataframe
from pandas import DataFrame

from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PyTransform, PythonDump


def gen_dataset(_context: dict, **_kwargs: dict) -> DataFrame:
    """Создание датасета из одной колонки
    :param _context: контекст выполнения
    :param _kwargs: параметры
    :return: ['id', 'data1', 'data2', 'data3']
    """
    rec_count = 1_000
    max_val_for_numeric_types = 32_000
    columns1 = {'id': 'int', 'data1': 'int', 'data2': 'int', 'data3': 'int', 'data4': 'int'}
    return generate_single_dataframe(columns=columns1,  records_count=rec_count,
                                     max_val_for_numeric_types=max_val_for_numeric_types)


def target(context: dict, df: DataFrame) -> DataFrame:
    """Вывод в лог
    :param context: контекст выполнения
    :param df:
    :return: ['double', 'data']
    """
    task = context['task']

    for line in StringIO(df[:50].to_csv(index=False, header=False)):
        task.log.info(line.rstrip())
    return df[:10]


def fill_dag(tdag: TDag):

    PythonDump(
        task_id='gen_dataset',
        python_callable=gen_dataset,
        dag=tdag
    )

    PyTransform(
        task_id='target',
        source=['gen_dataset'],
        transformer_callable=target,
        dag=tdag
    )

