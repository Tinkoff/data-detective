---
id: tuning.relations_type
---

# tuning.relations_type

## Info

> User types for relation panel on the web.

## Structure

| Key | Name                | Info                                               | Additional Info               |
| ---:| ------------------- | -------------------------------------------------- | :---------------------------- |
| PK  |source_type          | Entity source type                                 | dds.entity.entity_type        |
| PK  |target_type          | Entity target type                                 | dds.entity.entity_type        |
| PK  |attribute_type       | Entity attribute type                              | dds.entity.entity_type        |
| PK  |relation_type        | Relation type                                      | dds.relation.type             |
|     |source_group_name    | Source group name for the right panel              | Manually                      |
|     |target_group_name    | Target group name for the right panel              | Manually                      |
|     |attribute_group_name | Attribute group name for the right panel           | Manually                      |
|     |loaded_by            | A URN of loaded process                            | Usually contains name of DAG  |
|     |processed_dttm       | Datetime of ETL record processing                  |                               |

## Attributes

|Attribute| Value               |
| :-----: | ------------------- |
|Created  | 2021.09.02          |
|Owner    | Date Detective Team |
|Location | PG                  |

## Additional info

Your ad could be here.