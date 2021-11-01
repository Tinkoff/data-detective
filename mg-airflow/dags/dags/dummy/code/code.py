import yaml
from pandas import DataFrame


def val_translate(context, in_df: DataFrame, file_name: str) -> DataFrame:
    task = context.get('task')
    out_df = in_df.copy()
    with open(f'{task.dag.etc_dir}/{file_name}', 'r', encoding='utf-8') as cfg:
        config = yaml.safe_load(cfg)
    out_df['test'] = out_df.apply(
        lambda row: config[row['test']],
        axis=1
    )
    return out_df
