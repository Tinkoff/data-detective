from pandas import DataFrame


def etl(_context, elt: DataFrame) -> DataFrame:
    out_df = elt.copy()
    out_df['test1'] = out_df['test1'] * 3
    return out_df
