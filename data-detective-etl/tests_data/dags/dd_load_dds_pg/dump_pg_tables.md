| schema_name        | table_name     | table_owner   |   estimated_rows |   table_size |   full_table_size |
|:-------------------|:---------------|:--------------|-----------------:|-------------:|------------------:|
| dds                | sample         | airflow       |                0 |         8192 |             16384 |
| tuning             | breadcrumb     | airflow       |                0 |         8192 |             16384 |
| tuning             | relations_type | airflow       |                0 |         8192 |              8192 |
| tuning             | search_help    | airflow       |                0 |         8192 |              8192 |
| mart               | entity         | airflow       |                0 |         8192 |              8192 |
| dds                | entity         | airflow       |               86 |       122880 |            606208 |
| dds                | relation       | airflow       |               85 |        73728 |            229376 |
| wrk_dd_load_dds_pg | entity         | airflow       |               61 |        16384 |             16384 |
| wrk_dd_load_dds_pg | relation       | airflow       |               59 |        24576 |             24576 |