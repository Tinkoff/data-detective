"""Helper for creating DAG tests
"""
import logging
from typing import Any, Union

import petl
from airflow.models import BaseOperator, TaskInstance
from airflow.utils import timezone
from pandas import DataFrame
from pytest_mock.plugin import MockerFixture

from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators.tbaseoperator import TBaseOperator
from data_detective_airflow.test_utilities.assertions import assert_frame_equal


def run_task(task: Union[TBaseOperator, BaseOperator], context: dict = None):
    """Run a task"""
    # Removing launch restrictions based on the status of previous operators
    task.trigger_rule = 'dummy'
    task.render_template_fields(task.generate_context())
    task.pre_execute(context)
    task.execute(context)
    task.post_execute(context)


def mock_task_inputs(task, dataset, mocker):
    for i, uptask in enumerate(task.upstream_list):
        task.upstream_list[i].result.read = mocker.MagicMock(return_value=dataset[uptask.task_id])


def run_and_read(task: Union[TBaseOperator, BaseOperator], context: dict = None) -> DataFrame:
    """Run the task and return the DataFrame from the BaseResult instance."""
    logging.info(f'Running task {task.task_id}')
    run_task(task, context)
    return task.read_result(context)


def run_and_assert_task(
    task: Union[TBaseOperator, BaseOperator],
    dataset: dict[str, Any],
    mocker: MockerFixture = None,
    exclude_cols: list = None,
    **kwargs
):
    """Run the task, get the result and compare

    :param task: Id of the running task
    :param dataset: Dictionary with comparison examples. Output and input datasets are needed.
    :param exclude_cols: Columns excluded from comparison
    :param mocker: MockerFixture fixture
    """
    task_instance = TaskInstance(task=task, execution_date=timezone.utcnow())
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
    dag: TDag, task_id: str, test_datasets: dict, mocker: MockerFixture, exclude_cols: list = None
):  # pylint: disable=inconsistent-return-statements
    """Using run_and_assert_task

    Run the task and if it is TBaseOperator then get the result and compare it with the example
    Also if the task is PgReplacePartitions then the target table will be cleared first,
    and then after the launch, compare the contents of the target table with the example
    :param dag: TDag
    :param task_id: Id of the running task
    :param test_datasets: Dictionary with examples
    :param exclude_cols: Columns excluded from comparison
    :param mocker: MockerFixture fixture
    """
    task: TBaseOperator = dag.task_dict[task_id]
    run_and_assert_task(task, dataset=test_datasets, mocker=mocker, exclude_cols=exclude_cols)
