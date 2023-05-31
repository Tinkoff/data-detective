select  urn
        , urn as id
        , lower(entity_name) as entity_name
        , json_system->'type_for_search' as type_for_search
        , json_system->'system_for_search' as system_for_search
        , entity_type
        , search_data
        , info
        , tags

from dds.entity
