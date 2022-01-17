## Data Detective Airflow

[Data Detective Airflow](https://github.com/tinkoff/data-detective/tree/master/data-detective-airflow) is a framework
whose main idea is to add the concepts of work and result to [Apache Airflow](https://airflow.apache.org/).
It supports tasks testing over [Apache Airflow](https://airflow.apache.org/).
Data Detective Airflow allows developers to create workflows in the form of directed acyclic graphs (DAGs) of tasks.
The easy-to-use Data Detective Airflow scheduler makes it possible to run tasks and save results into works. 
Work storage support for s3, ftp, local disk and database is also included.

[More information about Data Detective Airflow](https://data-detective.dev/docs/data-detective-airflow/intro)

## Installation

#### Install from [PyPi](https://pypi.org/project/data-detective-airflow/)

```bash
pip install data-detective-airflow
```

See example DAG-s in [dags/dags](https://github.com/tinkoff/data-detective/tree/master/data-detective-airflow/dags/dags) folder.
