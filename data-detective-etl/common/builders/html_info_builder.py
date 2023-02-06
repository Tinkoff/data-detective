from typing import Dict, List, Text, Optional, Mapping

from common.utils import isnotempty

_HtmlInfoResultType = List[Dict[str, Optional[str]]]


class HtmlInfoBuilder:
    """Build html_info by source data
    build block with data field and container contents
    """

    __slots__ = ('_header_mapping', '_type', '_opened')

    def __init__(self, header_mapping: Mapping[Text, Text], opened: bool = None):
        self._header_mapping = header_mapping
        self._opened = opened

    def __call__(self, row_data) -> _HtmlInfoResultType:
        return [
            {'data': row_data[k], 'header': v, 'opened': str(int(self._opened))}
            if isinstance(self._opened, bool)
            else {'data': row_data[k], 'header': v}
            for k, v in self._header_mapping.items()
            if isnotempty(row_data.get(k))
        ]
