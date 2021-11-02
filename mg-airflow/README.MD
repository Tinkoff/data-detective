# MG Airflow

Framework with task data testing over [Apache Airflow](https://airflow.apache.org/)

## About framework

* [Concepts](docs/concepts.md)
* [DAG Factories](docs/factories.md)
* [Operators](./docs/operators.md)
* [About development](./docs/development.md)
* [About testing](./docs/testing.md)
* [Use in production](./docs/production.md)
* [Comparison with others solutions](./docs/comparison.md)
* [Abstractions for datasets](./docs/datasets.md)

## CONTRIBUTING

Read [CONTRIBUTING](./CONTRIBUTING.md) file.

## Usage

#### Install from [PyPi](https://pypi.org/project/mg-airflow/)

```bash
pip install mg-airflow
```

See example DAG-s (directed acyclic graph) in dags/dags folder.

## Release

Action "Publish mg-airflow" is triggered by tag "mg-airflow/*".
Package version is extracted from `pyproject.toml`.
