with ns as (
    select oid, nspname
    from pg_namespace
    where true
      and nspname !~ '^pg_'
      and nspname <> 'information_schema'
),
     ind as (
         select schemaname as schema_name,
                tablename  as table_name,
                jsonb_agg(
                        jsonb_build_object(
                                'name', indexname,
                                'ddl', indexdef
                            )
                    )      as index_json
         from pg_indexes
         where True
           and schemaname !~ '^pg_'
         group by schemaname, tablename
     ),
     table_grantee as (select grantee,
                              table_schema,
                              table_name,
                              array_agg(privilege_type) as rights
                       from information_schema.role_table_grants
                       where true
                         and table_schema !~ '^pg_'
                         and table_schema <> 'information_schema'
                       group by table_schema, table_name, grantee
     ),
     table_rights as (
         select table_schema,
                table_name,
                jsonb_agg(jsonb_build_object(
                        'grantee', grantee,
                        'rights', rights)) as table_rights
         from table_grantee
         group by table_schema, table_name
     )
select lower(ns.nspname)                        as schema_name,
       lower(tbl.relname)                       as table_name,
       pg_catalog.pg_get_userbyid(tbl.relowner) as table_owner,
       coalesce(tbl.reltuples::bigint, 0)       as estimated_rows,
       pg_table_size(tbl.oid)                   as table_size,
       pg_total_relation_size(tbl.oid)          as full_table_size,
       ind.index_json                           as index_json,
       tr.table_rights                          as table_rights
from pg_class as tbl
         inner join ns as ns on ns.oid = tbl.relnamespace
         left join ind as ind on ind.schema_name = ns.nspname and ind.table_name = tbl.relname
         left join table_rights as tr on tr.table_schema = ns.nspname and tr.table_name = tbl.relname
where tbl.relkind in ('r', 'p')
;
