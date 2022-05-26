---
id: dds.entity
---

# dds.entity

## Info

> Table with entities

## Structure

| Key | Name             | Info                                                | Additional Info                             |
| ---:| ---------------- | --------------------------------------------------- | :------------------------------------------ |
| PK  |urn               | Uniform Resource Name of entity                     | urn:...                                     |
|     |loaded_by         | A URN of loaded process                             | Usually contains DAG name                   |
|     |entity_name       | Entity full name                                    | For DB table this is 'schema.table'         |
|     |entity_name_short | Short entity name                                   | For DB table only table name without schema |
|     |entity_type       | Entity type                                         | Examples: JOB/SCHEMA/TABLE/COLUMN           |
|     |info              | Short info about entity                             |                                             |
|     |search_data       | Text for the search index                           |                                             |
|     |json_data         | Entity raw attributes                               | JSON type                                   |
|     |json_system       | JSON with attributes for search and display         | JSON type                                   |
|     |json_data_ui      | List of keys from json_data to display on the web   | JSON type                                   |
|     |codes             | List of code blocks                                 | JSON type                                   |
|     |grid              | Uses for card view                                  | JSON type                                   |
|     |htmls             | List of html blocks                                 | JSON type                                   |
|     |links             | List of links to external pages                     | JSON type                                   |
|     |notifications     | List of notification blocks                         | JSON type                                   |
|     |tables            | List of table blocks                                | JSON type                                   |
|     |tags              | Simple list with text tags                          | JSON type                                   |
|     |filters           | Filter for search and display                       | JSON type                                   |
|     |processed_dttm    | Datetime of ETL record processing                   | Timestamp                                   |


## Attributes

|Attribute| Value               |
| :-----: | ------------------- |
|Created  | 2021.08.02          |
|Owner    | Date Detective Team |
|Location | PG                  |

## Additional info

We didn't put JSON Schema in the main table because of difficulties with the docusaurus build.

### codes

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": [
    {
      "type": "object",
      "properties": {
        "header": {
          "description": "Block header",
          "minLength": 1,
          "type": "string"
        },
        "language": {
          "description": "Code language",
          "default": "sql",
          "enum": [
            "markdown",
            "python",
            "sql"
          ],
          "type": "string"
        },
        "opened": {
          "default": "1",
          "description": "Opened/closed block",
          "enum": [
            "0",
            "1"
          ],
          "type": "string"
        },
        "data": {
          "description": "Code text",
          "type": "string"
        }
      },
      "required": [
        "header",
        "data"
      ]
    }
  ]
}
```

Example
```json
[
    {
        "header": "Code header",
        "language": "sql",
        "opened": "1",
        "data": "select * from table;"
    }
]
```

### json_data_ui

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "keys": {
      "description": "Key names from json_data, that would be displayed on the web",
      "type": "array",
      "items": [
        {
          "type": "string"
        }
      ]
    },
    "opened": {
      "default": "1",
      "description": "Opened/closed block",
      "enum": [
        "0",
        "1"
      ],
      "type": "string"
    }
  },
  "required": [
    "keys"
  ]
}
```

Example

```json
{
  "keys": [
    "key1",
    "key2"
  ],
  "opened": "1"
}
```

### json_system

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "card_type": {
      "description": "Displayed entity",
      "type": "string"
    },
    "type_for_search": {
      "description": "Entity type for search",
      "type": "string"
    },
    "system_for_search": {
      "description": "System for search",
      "type": "string"
    }
  },
  "required": [
    "type_for_search"
  ]
}
```

Example
```json
[
    {
        "card_type": "Table",
        "type_for_search": "Table",
        "system_for_search": "Greenplum",
    }
]
```

### htmls

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": [
    {
      "type": "object",
      "properties": {
        "data": {
          "description": "Text with HTML",
          "type": "string"
        },
        "header": {
          "description": "Block header",
          "minLength": 1,
          "type": "string"
        },
        "opened": {
          "default": "1",
          "description": "Opened/closed block",
          "enum": [
            "0",
            "1"
          ],
          "type": "string"
        }
      },
      "required": [
        "data",
        "header"
      ]
    }
  ]
}
```

Example

```json
[
    {
        "data": "Text with <b>HTML</b>",
        "header": "Block header",
        "opened": "1"
    }
]
```

### links

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": [
    {
      "type": "object",
      "properties": {
        "link": {
          "description": "URL",
          "type": "string"
        },
        "type": {
          "type": "string",
          "enum": ["external", "wiki"]
        }
      },
      "required": [
        "link",
        "type"
      ]
    }
  ]
}
```

Example

```json
[
  {
    "link": "https://github.com/tinkoff/data-detective",
    "type": "external"
  }
]
```

### notifications

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": [
    {
      "type": "object",
      "properties": {
        "type": {
          "type": "string",
          "description": "Info - green, success - blue, warning - orange, error - red",
          "enum": ["success", "warning", "error", "info"]
        },
        "header": {
          "description": "Block header",
          "minLength": 1,
          "type": "string"
        },
        "data": {
          "type": "string",
          "description": "Block text"
        }
      },
      "required": [
        "type",
        "data"
      ]
    }
  ]
}
```

Example

```json
[
    {
        "type": "info",
        "header": "Header for the green panel",
        "data": "Green panel"
    }
]
```


### tables

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
    "data": {
      "type": "array",
      "items": [
        {
          "type": "object",
          "properties": {
            "columnName": {
              "type": "string"
            }
          },
          "required": [
            "columnNames..."
          ]
        }
      ]
    },
    "display_headers": {
      "default": "1",
      "description": "Display table headers and block name",
      "enum": [
        "0",
        "1"
      ],
      "type": "string"
    },
    "header": {
      "description": "Table header",
      "minLength": 1,
      "type": "string"
    }
  },
  "required": [
    "columns",
    "data",
    "display_headers",
    "header"
  ]
}
```

Example

```json
[
  {
    "data": [
      {
        "column1": 101,
        "column2": "value1"
      },
      {
        "column1": 102,
        "column2": "value2"
      }
    ],
    "columns": [
      "column1",
      "column2"
    ],
    "header": "Table header",
    "display_headers": "1"
  }
]
```


### filters

JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "FilterCategoryName": {
      "description": "Filters for objects category in widescreen tree",
      "default": "Label for filter category name",
      "type": "string"
    }
  },
  "required": [
    "FilterCategoryName"
  ]
}
```

Example

```JSON
 {
  "filters": {
    "Type": "Object type",
    "User": "Object users",
    "Support Team": "Team for support object",
    "Customer Team": "Customer of object",
    "Author": "Author of object"
  }
}
```
