with ns as (
    select oid, nspname
    from pg_namespace
    where true
      and nspname !~ '^pg_'
      and nspname <> 'information_schema'
)
select lower(ns.nspname)                               as schema_name,
       lower(cl.relname)                               as table_name,
       lower(f.attname)                                as column_name,
       pg_catalog.format_type(f.atttypid, f.atttypmod) as column_type,
       f.attnum                                        as ordinal_position
from pg_attribute f
         inner join pg_class cl
                    on f.attrelid = cl.oid and cl.relkind in ('r', 'p')
         inner join ns as ns
                    on cl.relnamespace = ns.oid
where true
  and attnum > 0
  and f.attisdropped is false
;
