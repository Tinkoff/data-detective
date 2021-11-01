from collections import Iterable
from typing import Any

from pandas import notnull


def is_not_empty(value: Any) -> bool:
    """Проверяет значение на None, NaN и пустой Iterable объект, возвращает True если это не так

    value: Any - любое значение
    return: bool - возвращает True, если значение не пустое, иначе False
    """
    if isinstance(value, Iterable):
        return bool(value)
    return notnull(value)
