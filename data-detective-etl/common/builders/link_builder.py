from typing import Dict, List, Mapping, Text, Optional, Any

from mg.utils import isnotempty

_LinkResultType = Optional[List[Dict[str, Any]]]


class LinkBuilder:

    _map = {'link': 'external', 'wiki_link': 'wiki'}

    def __init__(self, link_types: Mapping[Text, Text] = None):
        self._link_types = link_types or self._map

    def __call__(self, row_data) -> _LinkResultType:
        result = [{'link': row_data.pop(k), 'type': v}
                  for k, v in self._link_types.items() if isnotempty(row_data.get(k))]
        return result or None
