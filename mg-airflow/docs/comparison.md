# Сравнение с другими решениями

## Конфигураторы DAG-ов для Airflow

* [gusty](https://github.com/chriscardillo/gusty) - работает с разными входными форматами, поддерживает TaskGroup.
* [dag-factory](https://github.com/ajbosco/dag-factory) - создание DAG-ов из YAML, поддерживает TaskGroup.

## DAG orchestrators

* [dagster](https://github.com/dagster-io/dagster) - имеет много общего с Apache Airflow. 
Разделен scheduler и ui на разные модули. Работает с ресурсами в виде s3 и локальными файлами.
В нем реализована концепция с work, созданием и очисткой по завершении работы.
Шустрый планировщик.
* [Argo Workflows](https://argoproj.github.io/argo-workflows/) - решение на go. Запускает контейнеры в kubernetes.
Удобно из-за изоляции виртуальных окружений. Видится сложность реализации тестирования.
Нужно запускать пайплайны на go, в которых будут сравниваться датасеты на python.
