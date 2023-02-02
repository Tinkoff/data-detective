from pathlib import Path

import yaml
from pandas import DataFrame

from data_detective_airflow.constants import PG_CONN_ID
from data_detective_airflow.dag_generator import TDag
from data_detective_airflow.operators import PgSCD1, PyTransform


def get_data_from_tuning_messages_yaml(context: dict) -> DataFrame:
    """Get message code from tuning_messages.yaml

    :param context: Execution context
    :return: Dataframe ['code', 'lang', 'text', 'loaded_by']
    """
    with Path(f"{context['dag'].etc_dir}/tuning_messages.yaml").open(encoding="utf-8") as messages_yaml:  # type: ignore
        messages = yaml.safe_load(messages_yaml)

    loaded_by = context['dag'].dag_id

    messages = [
        [code, lang, text, loaded_by]
        for code, message_text in messages.items()
        for lang, text in message_text.items()
    ]

    return DataFrame(messages, columns=['code', 'lang', 'text', 'loaded_by'])


def fill_dag(t_dag: TDag):

    PyTransform(
        task_id='get_data_from_tuning_messages_yaml',
        description='Get message from tuning_messages.yaml',
        transformer_callable=get_data_from_tuning_messages_yaml,
        dag=t_dag,
    )

    PgSCD1(
        task_id='upload_data_to_tuning.messages',
        description='Upload data into tuning.messages table',
        conn_id=PG_CONN_ID,
        process_deletions=True,
        table_name='tuning.messages',
        source=['get_data_from_tuning_messages_yaml'],
        key=['code', 'lang', 'loaded_by'],
        dag=t_dag,
    )
