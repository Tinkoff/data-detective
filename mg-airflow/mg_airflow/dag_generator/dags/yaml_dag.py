# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Any

import yaml
from airflow.utils.module_loading import import_string

from mg_airflow import constants
from mg_airflow.dag_generator.dags.tdag import TDag


class YamlDag(TDag):
    """DAG, полностью создаваемый по описанию в yaml файле

    :param dag_dir: директория, содержащая TDag.META_FILE
    :param config: опциональный уже разложенный meta.yaml
    """

    def __init__(self, dag_dir: str, config: dict[str, Any]):
        self.config = config
        if not self.config:
            with open(f'{dag_dir}/{TDag.META_FILE}', encoding='utf-8') as file:
                self.config = yaml.safe_load(file)

        super().__init__(
            dag_dir=dag_dir,
            dag_id=Path(dag_dir).name,
            factory=self.config['factory'],
            start_date=constants.DEFAULT_START_DATE,
            schedule_interval=self.config.get('schedule_interval'),
            description=self.config.get('description', ''),
            default_args=self.config['default_args'],
            template_searchpath=dag_dir,
            tags=self.config.get('tags'),
        )

        self.__fill_dag()

    def __fill_dag(self):
        """Добавить task в DAG по описанию в yaml"""
        tasks = self.config.get('tasks')
        if tasks:
            for task in tasks:
                self.attach_task(task)

    def attach_task(self, task: dict) -> None:
        """Добавить task в DAG

        :param task: словарь с аттрибутами таска
        """
        excluded_params = ('type',)
        task_filtered = {k: v for k, v in task.items() if k not in excluded_params}

        # Обрабатываем callable параметры - нужно преобразовать из строки в функцию
        callables = filter(lambda k: k.endswith('_callable'), task_filtered.copy().keys())
        for param in callables:
            task_filtered[param.replace('_callable', '_lambda_val')] = task_filtered[param]
            task_filtered[param] = self.get_callable_by_def(task_filtered[param])

        # Создаем task
        task_filtered['dag'] = self
        task_type = import_string(task['type'])
        task_type(**task_filtered)
