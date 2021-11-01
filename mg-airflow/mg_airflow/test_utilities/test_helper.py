"""Помощник в создании тестов для Dag
"""
import logging
from typing import Any, Union

import petl
from airflow.models import BaseOperator, TaskInstance
from airflow.utils import timezone
from pandas import DataFrame
from pytest_mock.plugin import MockerFixture

from mg_airflow.dag_generator import TDag
from mg_airflow.operators.tbaseoperator import TBaseOperator
from mg_airflow.test_utilities.assertions import assert_frame_equal


def run_task(task: Union[TBaseOperator, BaseOperator], context: dict = None):
    """Запустить таск"""
    # Убираем ограничения на запуск по статусу предыдущих операторов
    task.trigger_rule = 'dummy'
    task.render_template_fields(task.generate_context())
    task.pre_execute(context)
    task.execute(context)
    task.post_execute(context)


def mock_task_inputs(task, dataset, mocker):
    for i, uptask in enumerate(task.upstream_list):
        task.upstream_list[i].result.read = mocker.MagicMock(return_value=dataset[uptask.task_id])


def run_and_read(task: Union[TBaseOperator, BaseOperator], context: dict = None) -> DataFrame:
    """Запустить таск и вернуть DataFrame из экземпляра BaseResult."""
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
    """Запустить таск, получить результат и сравнить с эталоном

    :param task: id таска для запуска
    :param dataset: dictionary с эталонами. Нужны выходной и входные датасеты
    :param exclude_cols: колонки, исключаемые из сравнения
    :param mocker: фикстура MockerFixture
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
    """Использовать run_and_assert_task

    Запустить таск и если это TBaseOperator то получить результат и сравнить с эталоном
    Также если таск PgReplacePartitions то сначала будет очищена целевая таблица,
    а потом после запуска провести сравнение с эталоном содержимое целевой таблицы
    :param dag: TDag
    :param task_id: id таска для запуска
    :param test_datasets: dictionary с эталонами
    :param exclude_cols: колонки, исключаемые из сравнения
    :param mocker: фикстура MockerFixture
    """
    task: TBaseOperator = dag.task_dict[task_id]
    run_and_assert_task(task, dataset=test_datasets, mocker=mocker, exclude_cols=exclude_cols)
