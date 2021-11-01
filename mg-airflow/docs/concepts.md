# Основные концепции фреймворка

`operator` - стандартный airflow оператор на основе `airflow.models.BaseOperator`.
Все операторы в mg-airflow наследуются от базового класса - `mg_airflow.operators.TBaseOperator`

`work` - временное хранилище промежуточных результатов пайплайна.
Поддерживаются work в локальной file system, sftp file system, s3, postgres.
Базовый класс для work - `mg_airflow.dag_generator.works.base_work.BaseWork`.

`result` - результат выполнения оператора (таска). 
Result проксирует чтение и запись в work. У одного оператора может быть только один result.
Базовый класс для result - `mg_airflow.dag_generator.results.base_result.BaseResult`. 
    
`pass-through/elt` - режим работы оператора, когда команда выполняется на удаленном сервере.
Не происходит выкачивание result на airflow-worker.
В pass-through операторах не вызывается чтение и записи result.
ETL-оператор выкачивает из work результат в память worker'а, преобразовывает и заливает в work в postgres, s3 или файл.
ELT операторы выполняют код на удаленном сервере. Пример PgSQL, если work находится в pg.

`dag-factory` - автоматическое создание DAG из YAML-файлов
Доступны две factory - когда в YAML задается полностью структура DAG и 
когда в YAML задаются только основные свойства DAG, а операторы задаются Python-кодом отдельно.
Код, который отвечает за factory - `mg_airflow.generator`  
