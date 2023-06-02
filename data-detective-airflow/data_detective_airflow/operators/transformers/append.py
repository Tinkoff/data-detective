from typing import Any, Optional

from pandas import concat

from data_detective_airflow.operators.tbaseoperator import TBaseOperator


class Append(TBaseOperator):
    """Merge multiple DataFrames into one

    :param source: Source
    :param append_options: Additional options for pandas.concat() method
    :param kwargs: Additional params for the TBaseOperator
    """

    ui_color = "#8f75d1"

    def __init__(self, source: list[str], append_options: Optional[dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        self.source = source
        self.append_options = append_options or {}
        for src in self.source:
            self.dag.task_dict[src] >> self  # pylint: disable=pointless-statement

    def execute(self, context):
        self.log.info("Start appending...")
        result = [self.dag.task_dict[src].result.read(context) for src in self.source]
        self.log.info("Writing pickle result")
        self.result.write(concat(result, **self.append_options), context)
        self.log.info("Finish")
