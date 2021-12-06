from typing import Callable, Dict, List, Text, Union, Optional, Mapping
from pydantic import BaseModel  # pylint:disable=no-name-in-module
from petl import Record as EtlRecord

from common.utils import isnotempty


_TableInfoResultType = Dict[str, Union[
    Optional[str],
    List[str],
    List[Optional[str]],
    List[Dict[str, str]],
    List[Dict[str, Optional[str]]]
]]


class TableInfoDescriptionType(BaseModel):
    keys: Mapping[Text, Text]
    header: Text
    display_headers: str  # Python 3.8+ # type: Literal['0', '1']
    orientation: str  # Python 3.8+ # type: Literal['horizontal', 'vertical']
    serializers: Optional[Dict[Text, Callable[..., Text]]]


class TableInfoBuilder:

    __slots__ = ('_table',)

    def __init__(self, table: TableInfoDescriptionType):
        self._table = table

    def _apply_serializers(self, row_data: Dict) -> Dict:
        serializers = self._table.serializers or dict()
        return {value: serializers.get(key, str)(row_data[key])
                for key, value in self._table.keys.items() if isnotempty(row_data.get(key))}

    @staticmethod
    def _get_all_keys(row_data):
        return {k for row in row_data for k in row.keys()} \
            if isinstance(row_data, (list, tuple)) else row_data.keys()

    def _get_kv_table(self, row_data) -> _TableInfoResultType:
        serializers: Dict[Text, Callable[..., Text]] = self._table.serializers or dict()
        return dict(
            columns=['Key', 'Value'],
            data=[{'Key': value, 'Value': serializers.get(key, str)(row_data[key])}
                  for key, value in self._table.keys.items() if isnotempty(row_data.get(key))],
            header=self._table.header,
            display_headers=self._table.display_headers
        )

    def _get_table(self, row_data) -> _TableInfoResultType:
        all_keys = row_data.flds if isinstance(row_data, EtlRecord) else self._get_all_keys(row_data)
        columns = [self._table.keys.get(j) for j in all_keys if self._table.keys.get(j)]
        columns.sort()
        return dict(
            columns=columns,
            data=[self._apply_serializers(row) for row in row_data] if isinstance(row_data, list)
            else [self._apply_serializers(row_data)],
            header=self._table.header,
            display_headers=self._table.display_headers
        )

    def get_table(self, row_data) -> _TableInfoResultType:
        if self._table.orientation == 'vertical':
            return self._get_kv_table(row_data)
        if self._table.orientation == 'horizontal':
            return self._get_table(row_data)
        raise TypeError("Table orientation should be either 'vertical' or 'horizontal'")

    def __call__(self, row_data) -> _TableInfoResultType:
        return self.get_table(row_data)
