SELECT r.destination                                as destination,
       r.source                                     as source,
       coalesce(e.entity_name_short, e.entity_name) as entity_name
FROM dds.relation r
         LEFT JOIN dds.entity e ON r.source = e.urn
WHERE r.type = 'Contains';
