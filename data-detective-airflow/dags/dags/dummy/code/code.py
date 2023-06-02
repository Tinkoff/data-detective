import yaml
from airflow.utils.context import Context
from pandas import DataFrame, concat


def val_translate(context: Context, in_df: DataFrame, file_name: str) -> DataFrame:
    """Translate values
    :param context: Airflow context
    :param in_df: incoming df
    :param file_name: translate file
    :return:
    """
    task = context.get("task")
    out_df = in_df.copy()
    with open(f"{task.dag.etc_dir}/{file_name}", "r", encoding="utf-8") as cfg:  # type: ignore
        config = yaml.safe_load(cfg)
    out_df["test"] = out_df.apply(lambda row: config[row["test"]], axis=1)
    return out_df


def append_dfs(_context: Context, *sources: DataFrame) -> DataFrame:
    """Append several dfs in one
    :param _context: Airflow context
    :param sources: dfs
    :return:
    """
    return concat([*sources])
