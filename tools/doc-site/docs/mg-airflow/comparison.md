# Comparison with other frameworks

## DAG configurators for Airflow

* [gusty](https://github.com/chriscardillo/gusty) gusty it works with different input formats, supports TaskGroup.
* [dag-factory](https://github.com/ajbosco/dag-factory) dag-factory creates DAGs from YAML. It is also supported by TaskGroup.

## DAG orchestrators

* [dagster](https://github.com/dagster-io/dagster) This framework has a lot in common with Apache Airflow.
The scheduler and UI are divided into different modules. Work with s3 resources and local files is available.
Dagster implements a concept with work, creation and cleaning upon completion of work. There is also a quick scheduler here.
* [Argo Workflows](https://argoproj.github.io/argo-workflows/) This solution works on Go. Containers are launched in Kubernetes.
It is convenient to use because of the isolation of virtual environments. However, there is a difficulty in implementing testing.
It is necessary to run pipelines on Go, in which datasets in python will be compared.
