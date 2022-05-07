from collections.abc import Iterable
from typing import Any

from pandas import notnull


def is_not_empty(value: Any) -> bool:
    """Checks the value for None, NaN and an empty Iterable object, returns True if it is not

    value: Any - any value
    return: bool - returns True if the value is not empty, otherwise False
    """
    if isinstance(value, Iterable):
        return bool(value)
    return notnull(value)
