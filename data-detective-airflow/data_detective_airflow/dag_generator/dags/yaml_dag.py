# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Any

import yaml
from airflow.utils.module_loading import import_string

from data_detective_airflow import constants
from data_detective_airflow.dag_generator.dags.tdag import TDag


class YamlDag(TDag):
    """DAG created based on the description in the yaml file

    :param dag_dir: Directory with TDag.META_FILE
    :param config: Optional and decomposed meta.yaml file
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
        """Add a task to a DAG according to the description in yaml file"""
        tasks = self.config.get('tasks')
        if tasks:
            for task in tasks:
                self.attach_task(task)

    def attach_task(self, task: dict) -> None:
        """Add task to DAG

        :param task: Dictionary with task attributes
        """
        excluded_params = ('type',)
        task_filtered = {k: v for k, v in task.items() if k not in excluded_params}

        # Processing callable parameters by converting from a string to a function
        callables = filter(lambda k: k.endswith('_callable'), task_filtered.copy().keys())
        for param in callables:
            task_filtered[param.replace('_callable', '_lambda_val')] = task_filtered[param]
            task_filtered[param] = self.get_callable_by_def(task_filtered[param])

        # Create a task
        task_filtered['dag'] = self
        task_type = import_string(task['type'])
        task_type(**task_filtered)
