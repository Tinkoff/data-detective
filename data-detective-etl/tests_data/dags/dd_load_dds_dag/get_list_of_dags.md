| dag_id                        | dag_dir                                                    | factory   | schedule_interval   | description                                                               | default_args                                                                                             | tags                 | tasks                                                                                                                                                                                                                                                                                                            |
|:------------------------------|:-----------------------------------------------------------|:----------|:--------------------|:--------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------|:---------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dd_system_remove_works        | /usr/local/airflow/dags/dags/dd_system_remove_works        | Python    | 5 3 * * *           | Clean of unremoved works                                                  | {'owner': 'airflow', 'retries': 1}                                                                       | ['system']           | ['clean_s3_works', 'clean_pg_works', 'clean_local_works']                                                                                                                                                                                                                                                        |
| dd_load_dds_dag               | /usr/local/airflow/dags/dags/dd_load_dds_dag               | Python    | 18 03 * * *         | Loading meta information from dag to data detective entity                | {'owner': 'airflow', 'result_type': 'pickle', 'retries': 1, 'work_conn_id': 's3work', 'work_type': 's3'} | ['dds']              | ['get_list_of_dags', 'add_code_files_to_dags', 'transform_dag_to_entity', 'link_root_node_to_dag', 'upload_dds_entity', 'upload_dds_relation']                                                                                                                                                                   |
| dd_load_dds_pg                | /usr/local/airflow/dags/dags/dd_load_dds_pg                | Python    | 13 03 * * *         | Loading meta information from postgres database                           | {'owner': 'airflow', 'result_type': 'pickle', 'retries': 1, 'work_conn_id': 's3work', 'work_type': 's3'} | ['dds']              | ['dump_pg_schemas', 'dump_pg_tables', 'dump_pg_columns', 'transform_schema_to_entity', 'transform_table_to_entity', 'transform_column_to_entity', 'link_schema_to_table', 'link_table_to_column', 'link_root_node_to_schema', 'append_entities', 'append_relations', 'upload_dds_entity', 'upload_dds_relation'] |
| dd_load_dds_root              | /usr/local/airflow/dags/dags/dd_load_dds_root              | YAML      | 5 1 * * *           | Loading root entities                                                     | {'owner': 'airflow', 'result_type': 'pickle', 'retries': 1, 'work_type': 'file'}                         | ['tuning']           | ['dump_root_nodes_entities', 'dump_root_nodes_relations', 'upload_dds_entity', 'upload_dds_relation']                                                                                                                                                                                                            |
| dd_load_tuning_breadcrumb     | /usr/local/airflow/dags/dags/dd_load_tuning_breadcrumb     | YAML      | 0 */2 * * *         | Loading the breadcrumb of relations from root to entity                   | {'owner': 'airflow', 'result_type': 'pickle', 'retries': 1, 'work_type': 'file'}                         | ['tuning']           | ['dump_relation_contains', 'transform_breadcrumb', 'upload_tuning_breadcrumb']                                                                                                                                                                                                                                   |
| dd_load_tuning_relations_type | /usr/local/airflow/dags/dags/dd_load_tuning_relations_type | YAML      | 5 2 * * *           | Loading entity relationship types                                         | {'owner': 'airflow', 'result_type': 'pickle', 'retries': 1, 'work_conn_id': 's3work', 'work_type': 's3'} | ['SLA3', 'prod_dev'] | ['dump_relations_types', 'upload_tuning_relations_type']                                                                                                                                                                                                                                                         |
| dd_load_tuning_search_help    | /usr/local/airflow/dags/dags/dd_load_tuning_search_help    | Python    | 12 03 * * *         | Loading information about systems and types into tuning.search_help table | {'owner': 'airflow', 'result_type': 'pickle', 'retries': 1, 'work_conn_id': 's3work', 'work_type': 's3'} | ['tuning']           | ['get_data_from_search_enums', 'upload_data_to_tuning_search_help']                                                                                                                                                                                                                                              |