source_type|target_type|attribute_type|relation_type|source_group_name|target_group_name|attribute_group_name|description
-------------- | ------------- | ---------- | --------- | ----------- | ------------- | ---- | -------
SCHEMA         |TABLE          |            |Contains   |             |               |      |Физическая схема содержит физические таблицы
TABLE          |COLUMN         |            |Contains   |             |               |      |Физическая таблица содержит физические колонки
LOGICAL_SCHEMA |LOGICAL_TABLE  |            |Contains   |             |               |      |Логическая схема содержит логические таблицы
LOGICAL_TABLE  |LOGICAL_COLUMN |            |Contains   |             |               |      |Логическая таблица содержит логические колонки
COLUMN         |COLUMN         |JOB         |Loads      |SOURCE COLUMNS|TARGET COLUMNS|JOBS  |Джоб грузит из одной колонки в другую.
LOGICAL_COLUMN |COLUMN         |            |Describes  |IS DESCRIBED |DESCRIBES      |      |Логическая колонка описывает физическую колонку
LOGICAL_SCHEMA |SCHEMA         |            |Describes  |IS DESCRIBED |DESCRIBES      |      |Логическая схема описывает физическую схему
LOGICAL_TABLE  |TABLE          |            |Describes  |IS DESCRIBED |DESCRIBES      |      |Логическая таблица описывает физическую таблицу
TABLE          |TABLE          |JOB         |Loads      |SOURCE TABLES|TARGET TABLES  |JOBS  |Джоб грузит из одной таблицы в другую. Таблица является источником для вью
