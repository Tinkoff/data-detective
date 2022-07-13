"""Helper for creating DAG tests
"""
import logging
from typing import Any, Union

import petl
from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.utils.context import Context
from pandas import DataFrame
from pytest_mock.plugin import MockerFixture

from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators.tbaseoperator import TBaseOperator
from data_detective_airflow.test_utilities.assertions import assert_frame_equal


def run_task(task: Union[TBaseOperator, BaseOperator], context: Context):
    """Run a task"""
    logging.info(f'Running task {task.task_id}')
    task.trigger_rule = 'dummy'
    task.render_template_fields(context)
    task.pre_execute(context)
    task.execute(context)
    task.post_execute(context)


def mock_task_inputs(task, dataset, mocker):
    for i, uptask in enumerate(task.upstream_list):
        task.upstream_list[i].result.read = mocker.MagicMock(return_value=dataset[uptask.task_id])


def run_and_read(task: Union[TBaseOperator, BaseOperator], context: Context) -> DataFrame:
    """Run the task and return the DataFrame from the BaseResult instance."""
    run_task(task, context)
    return task.read_result(context)


def run_and_assert_task(
    task: Union[TBaseOperator, BaseOperator],
    dataset: dict[str, Any],
    dag_run: DagRun,
    mocker: MockerFixture = None,
    exclude_cols: list = None,
    **kwargs
) -> None:
    """Run the task, get the result and compare.

    :param task: Id of the running task
    :param dataset: Dictionary with comparison examples. Output and input datasets are needed.
    :param dag_run: Airflow DagRun
    :param exclude_cols: Columns excluded from comparison
    :param mocker: MockerFixture fixture
    """
    logging.info(f'Running task {task.task_id}')
    task_instance = TaskInstance(task=task, run_id=dag_run.run_id, execution_date=dag_run.execution_date)
    context = task_instance.get_template_context()

    if mocker and dataset:
        mock_task_inputs(task, dataset, mocker)
    actual = run_and_read(task=task, context=context)

    if actual is not None:
        expected = dataset[task.task_id]

        if isinstance(expected, DataFrame):
            if task.include_columns:
                actual = actual[list(task.include_columns)]
                expected = expected[list(task.include_columns)]
            if isinstance(actual, tuple):
                # pylint: disable=no-member
                actual = petl.wrap(actual).todataframe()

            exclude_cols = exclude_cols or []
            e_cols = list(task.exclude_columns) + exclude_cols

            actual = actual.drop(e_cols, axis=1, errors='ignore')
            expected = expected.drop(e_cols, axis=1, errors='ignore')

            assert_frame_equal(actual, expected, **kwargs)
        else:
            assert actual == expected


def run_and_assert(
    dag: TDag, task_id: str, test_datasets: dict, mocker: MockerFixture, dag_run: DagRun, exclude_cols: list = None
):
    """Run task and assert with dataset in test_datasets
    :param dag: TDag
    :param task_id: Id of the running task
    :param test_datasets: Dictionary with examples
    :param mocker: MockerFixture fixture
    :param dag_run: Airflow DagRun
    :param exclude_cols: Columns excluded from comparison
    """
    task: BaseOperator = dag.task_dict[task_id]
    run_and_assert_task(task, dataset=test_datasets, dag_run=dag_run, mocker=mocker, exclude_cols=exclude_cols)
