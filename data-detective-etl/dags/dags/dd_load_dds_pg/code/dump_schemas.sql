select lower(ns.nspname) as schema_name,
       owner.rolname     as schema_owner,
       ns.nspacl         as schema_acl
from pg_namespace as ns
         left join pg_authid as owner on owner.oid = ns.nspowner
where true
  and ns.nspname not in (
                         'pg_catalog',
                         'pg_toast',
                         'information_schema'
    )
;
