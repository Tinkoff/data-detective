from pathlib import Path

import pandas as pd
import petl as etl


class FileDataset(dict):
    def __init__(self, folder: str):
        self.folder = folder
        self.path: Path = Path(self.folder)
        super().__init__(self)

    def __iter__(self):
        return (fname.stem for fname in self.path.iterdir())

    def save_md(self, key, value):
        raise NotImplementedError()


class JSONPandasDataset(FileDataset):
    def __getitem__(self, item):
        return pd.read_json(
            path_or_buf=f'{self.folder}/{item}.json', orient='table', convert_dates=False, encoding='utf8'
        )

    def __setitem__(self, key, frame: pd.DataFrame):
        frame = frame.reindex(sorted(frame.columns), axis=1)
        self.path.mkdir(exist_ok=True)
        with open(f'{self.folder}/{key}.json', mode='w', encoding='utf-8') as file:
            frame.to_json(path_or_buf=file, orient='table', force_ascii=False, index=False, indent=4, date_unit='ns')

    def __iter__(self):
        return (f_name.stem for f_name in self.path.iterdir() if f_name.name.endswith('.json'))  # filter out md files

    def save_md(self, key, value: pd.DataFrame):
        self.path.mkdir(exist_ok=True)
        with open(f'{self.folder}/{key}.md', mode='w', encoding='utf-8') as file:
            value.to_markdown(file, index=False)


class JSONPetlDataset(JSONPandasDataset):
    def __getitem__(self, item):
        frame = super().__getitem__(item)
        table = etl.fromdataframe(frame, include_index=False)
        return etl.wrap(etl.tupleoftuples(table))

    def __setitem__(self, key, table: etl.Table):
        frame = etl.todataframe(table)
        super().__setitem__(key, frame)
        self.path.mkdir(exist_ok=True)

    def save_md(self, key, value: etl.Table):
        frame = etl.todataframe(value)
        super().save_md(key, frame)
