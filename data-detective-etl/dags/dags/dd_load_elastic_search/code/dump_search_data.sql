select  urn
        , lower(entity_name) as id
        , json_system->'type_for_search' as type_for_search
        , json_system->'system_for_search' as system_for_search
        , entity_type
        , search_data
        , info
        , tags

from dds.entity
