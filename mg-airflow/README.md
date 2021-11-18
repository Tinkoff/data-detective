## MG Airflow

[MG Airflow](https://github.com/TinkoffCreditSystems/metadata-governance/tree/master/mg-airflow) is a framework
whose main idea is to add the concepts of work and result to [Apache Airflow](https://airflow.apache.org/).
It supports tasks testing over [Apache Airflow](https://airflow.apache.org/).
MG Airflow allows developers to create workflows in the form of directed acyclic graphs (DAGs) of tasks.
The easy-to-use MG Airflow scheduler makes it possible to run tasks and save results into works. 
Work storage support for s3, ftp, local disk and database is also included.

[More information about MG Airflow](https://tinkoffcreditsystems.github.io/metadata-governance/docs/mg-airflow/intro)

## Installation

#### Install from [PyPi](https://pypi.org/project/mg-airflow/)

```bash
pip install mg-airflow
```

See example DAG-s in [dags/dags](https://github.com/TinkoffCreditSystems/metadata-governance/tree/master/mg-airflow/dags/dags) folder.
