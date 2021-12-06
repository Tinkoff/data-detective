from typing import Dict

_JsonSystemResultType = Dict[str, str]


class JsonSystemBuilder:

    __slots__ = ('_system_for_search', '_type_for_search', '_card_type', '_business_type')

    def __init__(
            self,
            system_for_search: str = None,
            type_for_search: str = None,
            card_type: str = None,
            business_type: str = None,
    ):
        self._system_for_search = system_for_search
        self._type_for_search = type_for_search
        self._card_type = card_type
        self._business_type = business_type

    def __call__(
            self,
            system_for_search: str = None,
            type_for_search: str = None,
            card_type: str = None,
            business_type: str = None,
            **kwargs
    ) -> _JsonSystemResultType:
        result = dict(
            system_for_search=system_for_search or self._system_for_search,
            type_for_search=type_for_search or self._type_for_search,
            card_type=card_type or self._card_type,
            business_type=business_type or self._business_type,
        )
        return {key: value for key, value in result.items() if value}
