---
id: dds.relation
---

# dds.relation

## Info

> Table with relations between entities

## Structure

| Key | Name             | Info                              | Additional Info                      |
| ---:| ---------------- | --------------------------------- | :----------------------------------- |
| PK  |source            | Source URN                        | dds.entity.urn                       |
| PK  |destination       | Target URN                        | dds.entity.urn                       |
|     |loaded_by         | A URN of loaded process           | Usually contains DAG name            |
|     |type              | Relation type                     | Examples: Contains, Loads, Describes |
| PK  |attribute         | URN of job or pipeline            | dds.entity.urn                       |
|     |processed_dttm    | Datetime of ETL record processing |                                      |


## Attributes

|Attribute| Value               |
| :-----: | ------------------- |
|Created  | 2021.08.02          |
|Owner    | Date Detective Team |
|Location | PG                  |

## Additional info

Your ad could be here.