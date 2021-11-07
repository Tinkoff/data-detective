# Basic framework concepts

`operator` - standard airflow operator based on `airflow.models.BaseOperator`. 
All operators in mg-airflow are inherited from the base class - `mg_airflow.operators.TBaseOperator`.

`work` is a temporary repository of intermediate pipeline results.
Work in the local file system, sftp file system, s3, postgres are supported.
The base class for work is `mg_airflow.dag_generator.works.base_work.BaseWork`.

`result` is the result of executing the operator (or the task).
Result proxies reading and writing to the work. One operator can have only one result.
The base class for result is `mg_airflow.dag_generator.results.base_result.BaseResult`.

`pass-through/elt` is the operator's mode of operation, in which the command is executed on a remote server.
At the same time, the result is not uploaded to the airflow-worker.
In pass-through operators, reading and writing result is not called.
The ETL operator uploads the result from work into the worker's memory, converts it and loads it into postgres, s3 or a file.
ELT operators execute code on a remote server. An example could be PgSQL if work is in pg.

`dag-factory` - automatic creation of DAGs from YAML files.
Two types of factory are available: when the DAG structure is set completely in YAML and when only the basic properties of the DAG are set in YAML, and operators are set separately by Python code.
The code that is responsible for the operation of the factory is `mg_airflow.generator`.