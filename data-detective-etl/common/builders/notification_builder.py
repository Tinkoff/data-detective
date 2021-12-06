from typing import Dict, List, Mapping, Text

from bs4 import BeautifulSoup

from mg.parsers.html_parser import get_text

_NotificationResultType = List[Dict[str, str]]


class NotificationBuilder:

    _map = {'warning': 'error', 'note': 'warning', 'tip': 'success', 'technical_warning': 'error'}

    def __init__(self, headers: Mapping[Text, Text] = None):
        self._headers = headers or {}

    def __call__(self, row_data) -> _NotificationResultType:
        result = []
        for key, value in self._map.items():
            if key in row_data:
                el = {'data': row_data.pop(key), 'type': value}
                if not get_text(BeautifulSoup(el['data'])).strip():
                    continue
                if key in self._headers:
                    el['header'] = self._headers[key]
                result.append(el)
        return result
