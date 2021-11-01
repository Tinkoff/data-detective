SELECT a.attrelid
    , a.attname
    , a.attnum as attnum
    , ROW_NUMBER() OVER (ORDER BY a.attnum desc) rn
    FROM cl_distr d, test_dwh_rep.query_submit
        INNER JOIN pg_catalog.pg_attribute a
            ON d.attrelid = a.attrelid AND d.dk_order = a.attnum
        LEFT JOIN dev_dwh_rep.query q
            ON q.id = a.id
        INNER JOIN dev_dwh_rep.query_launch ql
            ON ql.id = a.id;