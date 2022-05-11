import allure
import pytest
from numpy import nan
from pandas import NA

from data_detective_airflow.utils.notnull import is_not_empty


@allure.feature('Utils')
@allure.story('is_not_empty function for non empty values (None, NA, NaN, "")')
@pytest.mark.parametrize(
    'name, task, result',
    [
        ('Empty string ', '', False),
        ('Python None ', None, False),
        ('Numpy NaN ', nan, False),
        ('Pandas NA ', NA, False),
        ('Empty list ', [], False),
        ('Empty dict', {}, False),
        ('Empty tuple', (), False),
        ('Empty set', set(), False),
        ('Int 0 ', 0, True),
        ('Float 0 ', 0.0, True),
        ('Complex 0 ', 0.0j, True),
        ('Non empty string ', 'str', True),
        ('Non empty list', ['1', 1], True)
    ]
)
def test_utils_notnull_is_not_empty(name, task, result):
    assert bool(is_not_empty(task)) == result
