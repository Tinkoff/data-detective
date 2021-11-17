## MG Airflow

[MG Airflow](https://github.com/TinkoffCreditSystems/metadata-governance/tree/master/mg-airflow) is a framework
whose main idea is to add the concepts of work and result to [Apache Airflow](https://airflow.apache.org/).
It supports tasks testing over [Apache Airflow](https://airflow.apache.org/).
MG Airflow allows developers to create workflows in the form of directed acyclic graphs (DAGs) of tasks.
The easy-to-use MG Airflow scheduler makes it possible to run tasks and save results into works. 
Work storage support for s3, ftp, local disk and database is also included.

## More information about MG Airflow

* [Concepts](../tools/doc-site/docs/mg-airflow/concepts.md)
* [DAG Factories](../tools/doc-site/docs/mg-airflow/factories.md)
* [Operators](../tools/doc-site/docs/mg-airflow/operators.md)
* [DAG development](../tools/doc-site/docs/mg-airflow/development.md)
* [DAG testing](../tools/doc-site/docs/mg-airflow/testing.md)
* [Production](../tools/doc-site/docs/mg-airflow/production.md)
* [Comparison with others solutions](../tools/doc-site/docs/mg-airflow/comparison.md)
* [Abstractions for datasets](../tools/doc-site/docs/mg-airflow/datasets.md)

## Installation

#### Install from [PyPi](https://pypi.org/project/mg-airflow/)

```bash
pip install mg-airflow
```

See example DAG-s in `dags/dags` folder.
