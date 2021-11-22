from mg_airflow.dag_generator.dags import TDag
from mg_airflow.dag_generator.generator import dag_generator, generate_dag
from mg_airflow.dag_generator.results import ResultType
from mg_airflow.dag_generator.works import WorkType

__all__ = (
    'dag_generator',
    'generate_dag',
    'TDag',
    'ResultType',
    'WorkType',
)
