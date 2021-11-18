---
sidebar_position: 3
---

# Creating a DAG

DAGs can be created in 3 ways:
1. **Classic**. Completely manual. It is possible to create a simple dag in the form of a `*.py` file and put it in the `dags` directory.
2. **Python Factory**. This method is semi-automatic. The Dag parameters are set as a YAML config, but the operators are set by python code
3. **YAML Factory**. Using this method, the dag is entirely set by the YAML config.

## Python Factory

This method creates a DAG in semi-automatic mode. The DAG itself is created from the YAML config, empty, without operators.
Operators are added to the DAG using python code.

To create a dag named TAG_NAME, put the yaml file `meta.xml` in any subdirectory `dags/dags/`, for example `dags/dags/DAG_NAME` or `dags/dags/DAG_GROUP/DAG_NAME`.
Any subdirectory `dogs/dogs/' that has 'meta.yaml' is considered a dag.
It has the following parameters:
- `description`: DAG description
- `tags` - list of tags. It is usually used in filters on the portal and is optional
- `schedule_interval`: schedule_interval airflow param, for example `10 17 * * *`
- `default_args`: default values
  - `owner`: airflow
  - `retries`: 1
  - `result_type`: type of the result, acceptable values: 'pickle', 'pg'
  - `work_type`: type of the work, acceptable values: s3, file, pg, sftp
  - `work_conn_id`: id of the connection to the work
- `factory`: `Python` (Python factory is being used)

Example:
```yaml
description: DAG загрузки метаданных Postgres
tags:
  - postgres
schedule_interval: '@once'
result_type: Pickle
default_args:
  owner: airflow
  retries: 1
  result_type: pickle
  work_type: s3
  work_conn_id: s3work
factory: Python
```

It is also necessary to create `dags / dags / DAG_NAME / code / code.py`.
In this file, the function `def fill_dag (dag):` must be defined, which adds the necessary statements to the dag.

```python
def fill_dag(tdag: TDag):
    ...
    DbDump(
        task_id='meta_get_schemas_main',
        conn_id=PG,
        sql='/code/meta_get_schemas.sql',
        dag=tdag
    )
    ...
```

## YAML Factory

This method creates a DAG completely automatically from the YAML config.

To create a dag named DAG_NAME, put the yaml file `meta.yaml` in any subdirectory` dags/dags/ `, e.g. `dags/dags/DAG_NAME` or` dags/dags/DAG_GROUP/DAG_NAME`.
Any subdirectory `dags/dags/` that contains `meta.yaml` is considered a dag.
This file should contain the following parameters:
- `description`: DAG description
  - `tags` - tags list. This list is used in filters on the portal
- `schedule_interval`: schedule_interval airflow param, for example `10 17 * * *`
- `default_args`: default values
  - `owner`: airflow
  - `retries`: 1
  - `result_type`: result type, acceptable values: 'pickle', 'pg', 'gp'
  - `work_type`: type of the work, acceptable values: s3, file, gp, pg, sftp
  - `work_conn_id`: id of the connection to the work
- `factory`: `YAML` (YAML factory is being used)
- `tasks`: tasks list
    - `task_id`: task name (unique id)
    - `description`: task description
    - `type`: Task type, class name of one of the helpers library operators, for example `PgDump`
    - `<params>`: Parameters required to create a specific task. For example, `conn_id`,` sql`

It is important that the task parameters in the YAML file contain a complete list of required parameters for the operator constructor.

Example:
```yaml
description: Тестовый DAG
schedule_interval: '*/5 * * * *'
result_type: Pickle
default_args:
  owner: airflow
  retries: 1
  result_type: pickle
  work_type: s3
  work_conn_id: s3work
factory: YAML
tasks:

  - task_id: df_now
    description: Запрос к базе данных
    type: DbDump
    conn_id:  pg
    sql: 'select now() as value;'

  - task_id: append_all
    description: Объединение предыдущего результата с самим собой
    type: Append
    source:
      - df_now
      - df_now
```

All additional functions, for example, callable functions for `PythonOperator` can be specified in the file` dags / dags / DAG_NAME / code / code.py`.
These functions will be automatically loaded when the DAG is generated.

Any such function must receive the context as the first parameter. For example

```python
def transform(context: dict, df: DataFrame) -> DataFrame:
    """Transform DataFrame

    :param context: Execution context
    :param df: Input DataFrame
    :return: df
    """
    # etc_dir
    config = read_config(context['dag'].etc_dir)
    # Airflow Variable etl_env
    env = 'dev' if context['var']['value'].etl_env == 'dev' else 'prod'
    return df
```
This allows access to all dag, task and running properties.
