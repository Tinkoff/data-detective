import functools
from hashlib import md5

import dill
from pandas import DataFrame, Series


# pylint: disable=too-many-return-statements
def to_bytes(obj: object) -> bytes:
    """Convert any object to bytes
    @param obj:
    @return:
    """
    if isinstance(obj, DataFrame):
        if obj.empty:
            return b'empty'
        return to_bytes(obj.apply(to_bytes, axis=1))
    if isinstance(obj, Series):
        if obj.empty:
            return b'empty'
        return functools.reduce(xor_bytes, map(to_bytes, obj.to_list()))
    if not obj:
        return b'empty'
    if isinstance(obj, bytes):
        return obj
    if isinstance(obj, list):
        bts = map(to_bytes, obj)
        return functools.reduce(xor_bytes, bts)
    if isinstance(obj, dict):
        bts = (xor_bytes(to_bytes(key), to_bytes(value)) for key, value in obj.items())
        return functools.reduce(xor_bytes, bts)
    return md5(dill.dumps(obj)).digest()


def xor_bytes(bts1: bytes, bts2: bytes) -> bytes:
    """Make a xor by bytes
    @param bts1:
    @param bts2:
    @return:
    """
    return bytes([byte1 ^ byte2 for byte1, byte2 in zip(bts1, bts2)])


def assert_frame_equal(frame1, frame2, *_args, **kwargs):
    frame1 = frame1.fillna('null')
    frame2 = frame2.fillna('null')
    result = to_bytes(frame1) == to_bytes(frame2)
    if not result and kwargs.get('debug', False):
        print('frame1 - CSV:')
        print(frame1.to_csv(index=False))
        print('frame2 - CSV:')
        print(frame2.to_csv(index=False))
        print('Got it!')
    assert result
