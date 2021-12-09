---
id: dds.sample
---

# dds.sample

## Info

> Data samples

## Structure

| Key | Name             | Info                                        | Additional Info                      |
| ---:| ---------------- | ------------------------------------------- | :----------------------------------- |
|  PK |urn               | Uniform Resource Name of entity             | dds.entity.urn                       |
|     |column_def        | Column names with type                      | JSON                                 |
|     |cnt_rows          | Rows in saved sample                        |                                      |
|     |sample_data       | Sample                                      | JSON                                 |
|     |loaded_by         | A URN of loaded process                     | Usually contains DAG name            |
|     |processed_dttm    | Datetime of ETL record processing           |                                      |

## Attributes

|Attribute| Value               |
| :-----: | ------------------- |
|Created  | 2021.10.02          |
|Owner    | Date Detective Team |
|Location | PG                  |

## Additional info

### cnt_rows

List of column names with data type. Example:
```
["column1::bigint", "column2::character varying", "column3::integer"]
```

### sample_data

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "columns": {
      "type": "array",
      "items": [
        {
          "type": "string"
        }
      ]
    },
    "sample_data": {
      "type": "array",
      "items": [
        {
          "type": "object",
          "properties": {
            "columnName1": {
              "type": "string"
            },
            "columnName2": {
              "type": "string"
            }
          },
          "required": [
            "columnNames..."
          ]
        }
      ]
    }
  },
  "required": [
    "columns",
    "sample_data"
  ]
}
```

Example

```json
{
  "columns": [
    "column1",
    "column2",
    "column3"
  ],
  "sample_data": [
    {
      "column1": 101,
      "column2": "value1",
      "column3": 1001
    },
    {
      "column1": 102,
      "column2": "value2",
      "column3": 1003
    }
  ]
}
```
