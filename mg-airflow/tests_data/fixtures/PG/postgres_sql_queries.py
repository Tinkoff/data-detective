sql_test_table = \
    "CREATE SCHEMA IF NOT EXISTS {0};" \
    "DROP TABLE IF EXISTS {0}.{1} CASCADE; " \
    "CREATE TABLE {0}.{1} (id int, data text); " \
    "INSERT INTO {0}.{1} (id, data) " \
    "VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'); " \
    "COMMIT;"

sql_with_non_existent_schema = 'SELECT * FROM test_test.{0} '

sql_with_non_existent_field = 'SELECT * FROM {0}.{1} WHERE group_id < 5'

sql_with_non_existent_source = 'SELECT * FROM {0}.test_tab'

sql_drop_schema = "DROP SCHEMA IF EXISTS {0} CASCADE;"
