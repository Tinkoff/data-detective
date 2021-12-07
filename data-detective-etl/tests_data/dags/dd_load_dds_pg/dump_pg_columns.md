| schema_name   | table_name     | column_name          | column_type                 |   ordinal_position |
|:--------------|:---------------|:---------------------|:----------------------------|-------------------:|
| dds           | sample         | urn                  | text                        |                  1 |
| dds           | sample         | column_def           | jsonb                       |                  2 |
| dds           | sample         | cnt_rows             | integer                     |                  3 |
| dds           | sample         | sample_data          | jsonb                       |                  4 |
| dds           | sample         | processed_dttm       | timestamp without time zone |                  5 |
| tuning        | breadcrumb     | urn                  | text                        |                  1 |
| tuning        | breadcrumb     | breadcrumb_urn       | jsonb                       |                  2 |
| tuning        | breadcrumb     | breadcrumb_entity    | jsonb                       |                  3 |
| tuning        | breadcrumb     | loaded_by            | text                        |                  4 |
| tuning        | breadcrumb     | processed_dttm       | timestamp without time zone |                  5 |
| tuning        | relations_type | source_type          | text                        |                  1 |
| tuning        | relations_type | target_type          | text                        |                  2 |
| tuning        | relations_type | attribute_type       | text                        |                  3 |
| tuning        | relations_type | relation_type        | text                        |                  4 |
| tuning        | relations_type | source_group_name    | text                        |                  5 |
| tuning        | relations_type | target_group_name    | text                        |                  6 |
| tuning        | relations_type | attribute_group_name | text                        |                  7 |
| tuning        | relations_type | loaded_by            | text                        |                  8 |
| tuning        | relations_type | processed_dttm       | timestamp without time zone |                  9 |
| tuning        | search_help    | type                 | text                        |                  1 |
| tuning        | search_help    | name                 | text                        |                  2 |
| tuning        | search_help    | description          | text                        |                  3 |
| tuning        | search_help    | loaded_by            | text                        |                  4 |
| tuning        | search_help    | processed_dttm       | timestamp without time zone |                  5 |
| mart          | entity         | load_dt              | date                        |                  1 |
| mart          | entity         | urn                  | text                        |                  2 |
| mart          | entity         | loaded_by            | text                        |                  3 |
| mart          | entity         | entity_name          | text                        |                  4 |
| mart          | entity         | entity_type          | text                        |                  5 |
| mart          | entity         | entity_name_short    | text                        |                  6 |
| mart          | entity         | info                 | text                        |                  7 |
| mart          | entity         | grid                 | jsonb                       |                  8 |
| mart          | entity         | json_data            | jsonb                       |                  9 |
| mart          | entity         | json_system          | jsonb                       |                 10 |
| mart          | entity         | json_data_ui         | jsonb                       |                 11 |
| mart          | entity         | codes                | jsonb                       |                 12 |
| mart          | entity         | links                | jsonb                       |                 13 |
| mart          | entity         | htmls                | jsonb                       |                 14 |
| mart          | entity         | notifications        | jsonb                       |                 15 |
| mart          | entity         | tables               | jsonb                       |                 16 |
| mart          | entity         | search_data          | text                        |                 17 |
| mart          | entity         | tags                 | jsonb                       |                 18 |
| mart          | entity         | processed_dttm       | timestamp without time zone |                 19 |
| dds           | entity         | urn                  | text                        |                  1 |
| dds           | entity         | loaded_by            | text                        |                  2 |
| dds           | entity         | entity_name          | text                        |                  3 |
| dds           | entity         | entity_type          | text                        |                  4 |
| dds           | entity         | entity_name_short    | text                        |                  5 |
| dds           | entity         | info                 | text                        |                  6 |
| dds           | entity         | search_data          | text                        |                  7 |
| dds           | entity         | codes                | jsonb                       |                  8 |
| dds           | entity         | grid                 | jsonb                       |                  9 |
| dds           | entity         | json_data            | jsonb                       |                 10 |
| dds           | entity         | json_system          | jsonb                       |                 11 |
| dds           | entity         | json_data_ui         | jsonb                       |                 12 |
| dds           | entity         | htmls                | jsonb                       |                 13 |
| dds           | entity         | links                | jsonb                       |                 14 |
| dds           | entity         | notifications        | jsonb                       |                 15 |
| dds           | entity         | tables               | jsonb                       |                 16 |
| dds           | entity         | tags                 | jsonb                       |                 17 |
| dds           | entity         | processed_dttm       | timestamp without time zone |                 18 |
| dds           | relation       | source               | text                        |                  1 |
| dds           | relation       | destination          | text                        |                  2 |
| dds           | relation       | type                 | text                        |                  3 |
| dds           | relation       | loaded_by            | text                        |                  4 |
| dds           | relation       | attribute            | text                        |                  5 |
| dds           | relation       | processed_dttm       | timestamp without time zone |                  6 |