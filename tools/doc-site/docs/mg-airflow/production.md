---
sidebar_position: 7
---

# Production

The start point for generating DAGs is a file in the airflow.settings.DATA_FOLDER with the following contents:
```python
import sys

import argcomplete
from airflow.cli.cli_parser import get_parser

from mg_af.constants import DAG_ID_KEY
from mg_af.dag_generator import dag_generator

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

* Next to the file `dags/dag_generator.py ` it is worth placing the file `.airflowignore` with the contents of `dags`.
This will prevent scanning of py files in the dags folder.
* dags/dag_generator.py - the only one entry point to the DAGs. Therefore, it does not make sense to parallelize the process of scanning DAGs.
* At the end of the TDag operation (successful or unsuccessful), the cleanup of all work is called. This process is logged in scheduler.
```
cat dag_generator.py.log | grep callback
>2021-05-08 07:23:30,865|INFO|logging_mixin.py:104|>2021-05-08 07:23:30,864|INFO|dag.py:853|Executing dag callback function: <bound method clear_all_works of <DAG: dummy_s3>>
```
* The launch of `airflow worker` should not occur from the root user, for this the `airflow` user is added to the image.
* Python modules are installed in `${AIRFLOW_USER_HOME}/.local/bin`
* AIR FLOW_HOME can be moved to the /app folder
