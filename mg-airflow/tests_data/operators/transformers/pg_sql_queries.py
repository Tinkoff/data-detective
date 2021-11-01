sql_test_table = """DROP TABLE IF EXISTS {0}.{1} CASCADE; 
                 CREATE TABLE {0}.{1} (id int, data text);
                 INSERT INTO {0}.{1} (id, data)
                 VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');
                 COMMIT;"""


