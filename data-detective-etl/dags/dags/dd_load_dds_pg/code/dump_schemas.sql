select lower(ns.nspname)                                  as schema_name,
       pg_catalog.pg_get_userbyid(ns.nspowner)            as schema_owner,
       pg_catalog.array_to_json(ns.nspacl)                as schema_acl,
       pg_catalog.obj_description(ns.oid, 'pg_namespace') as schema_description
from pg_namespace as ns
where true
  and ns.nspname !~ '^pg_'
  and ns.nspname !~ '^wrk_dd_'
  and ns.nspname <> 'information_schema'
;
