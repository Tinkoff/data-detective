with ns as (
    select oid, nspname
    from pg_namespace
    where true
      and nspname !~ '^pg_'
      and nspname <> 'information_schema'
)
select lower(ns.nspname)                        as schema_name,
       lower(tbl.relname)                       as table_name,
       pg_catalog.pg_get_userbyid(tbl.relowner) as table_owner,
       coalesce(tbl.reltuples::bigint, 0)       as estimated_rows,
       pg_table_size(tbl.oid)                   as table_size,
       pg_total_relation_size(tbl.oid)          as full_table_size
from pg_class as tbl
         inner join ns as ns on ns.oid = tbl.relnamespace
where tbl.relkind in ('r', 'p')
;
