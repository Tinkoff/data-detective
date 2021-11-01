# Создание Dag

DAGs можно создавать 3 способами:
1. **Классический**. Полностью ручной. Создать обычный dag в виде `*.py` файла и положить его в директорию `dags`
1. **Фабрика Python**. Полуавтоматический. Параметры Dag задаются в виде YAML-конфига, но операторы задаются python-кодом 
1. **Фабрика YAML**. Dag целиком задается YAML-конфигом.

## Фабрика Python

Этот метод создает DAG в полуавтоматическом режиме. 
Сам DAG создается из YAML-конфига, но он создается пустой, без операторов.
Операторы добавляются в DAG уже с помощью python-кода.

Чтобы создать dag с именем DAG_NAME
нужно положить yaml-файл `meta.yaml` в любую поддиректорию `dags/dags/`,
например `dags/dags/DAG_NAME` или `dags/dags/DAG_GROUP/DAG_NAME`.
Любая поддиректория `dags/dags/`, в которой есть `meta.yaml` считается dag.
В нем следующие параметры:
- `description`: описание DAG
- `tags` - список тегов. Используется в фильтрах на портале. Опционально.
- `schedule_interval`: параметр schedule_interval airflow, например `10 17 * * *`
- `default_args`: значения по-умолчанию
  - `owner`: airflow
  - `retries`: 1
  - `result_type`: тип результата, возможные значения: 'pickle', 'pg'
  - `work_type`: тип ворка, возможные значения: s3, file, pg, sftp
  - `work_conn_id`: id подключения к ворку
- `factory`: `Python` (означает что используется фабрика Python)

Пример файла:
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

Так же нужно создать `dags/dags/DAG_NAME/code/code.py`. 
В этом файле должна быть задана функция `def fill_dag(tdag):`. Ее задача - добавить в dag нужные операторы.

Пример такой функции:  

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

## Фабрика YAML

Этот метод создает DAG полностью автоматически из YAML-конфига.

Чтобы создать dag с именем DAG_NAME
нужно положить yaml-файл `meta.yaml` в любую поддиректорию `dags/dags/`,
например `dags/dags/DAG_NAME` или `dags/dags/DAG_GROUP/DAG_NAME`.
Любая поддиректория `dags/dags/`, в которой есть `meta.yaml` считается dag.
В нем должны быть параметры:
- `description`: описание DAG
- `tags` - список тегов. Используется в фильтрах на портале
- `schedule_interval`: параметр schedule_interval airflow, например `10 17 * * *`
- `default_args`: значения по-умолчанию 
  - `owner`: airflow
  - `retries`: 1
  - `result_type`: тип результата, возможные значения: 'pickle', 'pg', 'gp'
  - `work_type`: тип ворка, возможные значения: s3, file, gp, pg, sftp
  - `work_conn_id`: id подключения к ворку
- `factory`: `YAML` (означает что используется фабрика YAML)
- `tasks`: Список тасков
    - `task_id`: имя таска (он же уникальный id)
    - `description`: описание таска
    - `type`: Тип таска, название класса одного из операторов библиотеки helpers, например `PgDump`
    - `<params>`: Параметры, необходимые для создания конкретного таска. Например, `conn_id`, `sql`

Важно чтобы в параметрах таска в YAML-файле задавался полный список обязательных параметров для конструктора оператора.

Пример файла:
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

Все дополнительные функции, например вызываемые функции для `PythonOperator` можно задать в файле `dags/dags/DAG_NAME/code/code.py`.
Эти функции автоматически подтянутся при генерации DAG.

Любая такая функция первым параметром должна получать контекст. Например

```python
def transform(context: dict, df: DataFrame) -> DataFrame:
    """Преобразовать DataFrame

    :param context: Контекст выполнения
    :param df: Входной DataFrame
    :return: df
    """
    # etc_dir
    config = read_config(context['dag'].etc_dir)
    # Airflow Variable etl_env
    env = 'dev' if context['var']['value'].etl_env == 'dev' else 'prod'
    return df
```
Это позволяет получить доступ ко всем свойствам дага, таска и запуска.
