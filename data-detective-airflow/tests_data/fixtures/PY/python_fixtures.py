from collections import namedtuple
from pandas import DataFrame

import pytest


def __get_func_and_exc_for_negative_py_dump_tests() -> set:
    """Метод для получения функций и сообщений с ошибками для негативных PY-тестов
    :return множество с функциями и соответствующими сообщениями об ошибках
    """
    py_dump_test_param = namedtuple("py_dump_test_param", ["extract_func", "exception"])
    py_dump_params = set()

    def python2df_callable_without_args(context):
        return DataFrame.from_dict(data_dictionary)

    py_dump_params.add(py_dump_test_param(python2df_callable_without_args,
                                          NameError))

    py_dump_params.add(py_dump_test_param(lambda context: df.from_dict(d),
                                          NameError))
    return py_dump_params


@pytest.fixture(params=__get_func_and_exc_for_negative_py_dump_tests())
def invalid_python_dump_data(request):
    """Фикстура для негативных тестов python dump"""
    yield request.param
