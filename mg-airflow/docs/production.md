# Production use

Входная точка для генерации DAG-ов - это файл в папке airflow.settings.DAGS_FOLDER с содержимым

```python
import sys

import argcomplete
from airflow.cli.cli_parser import get_parser

from mg_airflow.constants import DAG_ID_KEY
from mg_airflow.dag_generator import dag_generator

dag_id = None
if sys.argv[0].endswith('airflow'):
    parser = get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    dag_id = getattr(args, DAG_ID_KEY, None)

whitelist = [dag_id] if dag_id else []
for dag in dag_generator(dag_id_whitelist=whitelist):
    if not dag:
        continue
    globals()['dag_' + dag.dag_id] = dag
```

* Рядом с файлом `dags/dag_generator.py` стоит положить `.airflowignore` с содержимым `dags`.
Это позволит не тратить ресурсы на сканирование py-файлов в папке dags.
* dags/dag_generator.py - единственная точка входа в DAG-и. Поэтому не имеет смысл распараллеливать процесс сканирования DAG-ов.
* По окончании работы TDag (успешном или неуспешном) вызывается очистка всех work. Этот процесс логируется в scheduler
 в файл лога *.py.log парсинга .py файла с дагом
```
cat dag_generator.py.log | grep callback
>2021-05-08 07:23:30,865|INFO|logging_mixin.py:104|>2021-05-08 07:23:30,864|INFO|dag.py:853|Executing dag callback function: <bound method clear_all_works of <DAG: dummy_s3>>
```
* Запуск `airflow worker` не должен происходить от пользователя root, поэтому в образ добавлен пользователь `aifrlow`.
* Модули для python устанавливаются в `${AIRFLOW_USER_HOME}/.local/bin`
* AIRFLOW_HOME можно будет перенести в папку /app
