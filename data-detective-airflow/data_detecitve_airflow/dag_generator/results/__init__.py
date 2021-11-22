from mg_airflow.dag_generator.results.base_result import ResultType
from mg_airflow.dag_generator.results.pg_result import PgResult
from mg_airflow.dag_generator.results.pickle_result import PickleResult

__all__ = (
    'ResultType',
    'PgResult',
    'PickleResult',
)
