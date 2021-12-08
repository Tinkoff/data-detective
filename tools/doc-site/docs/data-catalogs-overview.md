---
id: tools-overview
---

# OpenSource Data Catalog projects overview

* [DataHub](https://github.com/linkedin/datahub). DataHub contains a large number of modules and subprojects. LinkedIn supports this project in opensource. 
DataHub has integrations with systems: Kafka, Airflow, MySQL, SQL Server, Postgres, LDAP, Snowflake, Hive, BigQuery, and many others.
The main disadvantage of this solution is its complex and large structure, which can become a problem when deploying for an enterprise.
Each component from a large set needs its own configuration and stability support.

* [Marquez](https://github.com/MarquezProject/marquez). This project works with basic database entities: schema, table and column. It allows developers to set up the lineage between them.
Apache Airflow add-on also exists in the project. Marquez was released and open sourced by WeWork.
The functionality of Marquez is quite basic. 
This is a simple tool that may be suitable for a small number of user scenarios, most likely Marquez is not suitable for large-scale solutions.

* [Amundsen](https://github.com/amundsen-io/amundsen). Amundsen is a data discovery and metadata engine for improving the productivity of data analysts, data scientists and engineers when interacting with data.
This project supports working with apache Airflow ETL Orchestrator.
Amundsen includes metadata service currently uses a Neo4j proxy to interact with Neo4j graph db and serves frontend service's metadata. The metadata is represented as a graph model.
To search for amundsen, you need to enter the whole word, which makes the search more difficult.

* [OpenLineage](https://github.com/OpenLineage/OpenLineage). OpenLineage defines the metadata for running jobs and the corresponding events. A configurable backend allows choosing what protocol to send the events to.
The main goal of the OpenLineage is to have a unified schema for describing metadata and data lineage across tools to make data lineage collection and analysis easier.
The Linux Foundation is supporting this project on OpenSource.

