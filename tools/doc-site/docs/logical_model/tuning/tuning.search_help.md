---
id: tuning.search_help
---

# tuning.search_help

## Info

> Entity breadcrumbs based on relation 'Contains'

## Structure

| Key | Name             | Info                                                                  | Additional Info                      |
| ---:| ---------------- | --------------------------------------------------------------------- | :----------------------------------- |
| PK  |urn               | Uniform Resource Name of entity                                       | dds.entity.urn                       |
|     |breadcrumb_urn    | List of urn from root to entity                                       | JSON                                 |
|     |breadcrumb_entity | List of coalesce(entity_name_short, entity_name) from root to entity  | JSON                                 |
|     |loaded_by         | A URN of loaded process                                               | Usually contains DAG name            |
|     |processed_dttm    | Datetime of ETL record processing                                     |                                      | 

## Attributes

|Attribute| Value               |
| :-----: | ------------------- |
|Created  | 2021.09.02          |
|Owner    | Date Detective Team |
|Location | PG                  |

## Additional info

Breadcrumb is a sequence of entities from root to the current one.