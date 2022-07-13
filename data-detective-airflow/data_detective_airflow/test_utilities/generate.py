import logging
import os
from typing import Union

import pandas
import petl as etl
from airflow.models import BaseOperator, TaskInstance
from airflow.utils import timezone

from data_detective_airflow.operators.tbaseoperator import TBaseOperator
from data_detective_airflow.test_utilities.datasets import JSONPandasDataset, JSONPetlDataset
from data_detective_airflow.test_utilities.test_helper import run_and_read


# pylint: disable=too-many-statements
def run_and_gen_ds(task: Union[TBaseOperator, BaseOperator], folder, dag_run, drop_cols: list[str] = None):
    drop_cols = drop_cols or []
    if 'processed_dttm' not in drop_cols:
        drop_cols.append('processed_dttm')

    task_instance = TaskInstance(task=task, run_id=dag_run.run_id, execution_date=dag_run.execution_date)
    context = task_instance.get_template_context()

    task_result = run_and_read(task=task, context=context)

    if isinstance(task_result, tuple):
        task_result = etl.wrap(task_result)

    if isinstance(task_result, pandas.DataFrame):
        pd_result = task_result.drop(labels=drop_cols, axis=1, errors='ignore')

        dataset = JSONPandasDataset(folder)

        dataset[task.task_id] = pd_result
        try:
            dataset.save_md(task.task_id, pd_result)
        except UnicodeDecodeError:
            logging.warning(f'Cant save md for {task.task_id}: containing non-ascii bytes')
    elif isinstance(task_result, etl.Table):
        header = etl.header(task_result)
        cut_header = [col for col in header if col not in drop_cols]
        cut_table = task_result.cut(cut_header)

        dataset = JSONPetlDataset(folder)

        dataset[task.task_id] = cut_table
        try:
            dataset.save_md(task.task_id, cut_table)
        except UnicodeDecodeError:
            logging.warning(f'Cant save md for {task.task_id}: containing non-ascii bytes')
    else:
        return


def is_gen_dataset_mode() -> bool:
    """Switching the mode of checking and creating test data

    Add to environment variables
    export GEN_DATASET=Any # creating
    unset GEN_DATASET     # checking
    """
    return bool(os.getenv('GEN_DATASET', ''))
