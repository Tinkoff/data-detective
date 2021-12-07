from collections.abc import Iterable
from typing import Any, Optional

from pandas import isnull, notnull


def get_readable_size_bytes(size: float, decimal_places=2) -> Optional[str]:
    if isnull(size):
        return None
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        if size < 1024 or unit == 'YB':
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024
    return '0 B'


def isnotempty(value: Any) -> bool:
    """Check value for None, NaN or empty Iterable object, return True if value is not empty
    value: Any - any value
    return: bool - return True, if value is not empty, else False
    """
    if isinstance(value, Iterable):
        return bool(value)
    return notnull(value)
