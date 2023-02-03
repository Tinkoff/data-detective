from typing import Dict, List, Union

_CodeInfoResultType = Dict[str, Union[List[str], str]]


class CodeBuilder:

    __slots__ = (
        '_header',
        '_language',
        '_key',
        '_data',
        '_opened',
    )

    def __init__(
        self,
        header: str = None,
        language: str = None,
        key: List[str] = None,
        data: str = None,
        opened: bool = True,
    ):
        self._header = header
        self._language = language
        self._key = key
        self._data = data
        self._opened = opened

    def __call__(
        self,
        header: str = None,
        language: str = None,
        key: List[str] = None,
        data: str = None,
        opened: bool = None,
        **kwargs,
    ) -> _CodeInfoResultType:
        if opened is None:
            opened = self._opened

        result = dict(
            header=header or self._header,
            language=language or self._language,
            key=key or self._key,
            data=data or self._data,
            opened=str(int(opened)),
        )
        return {key: value for key, value in result.items() if value}
