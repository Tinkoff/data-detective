# Data Detective ETL

На основе фреймворка data-detective-airflow.

Наполнение репозитория Data Detective demo-данными.

## Разработка

Среда разработки поднимается в контейнерах docker на машине разработчика и подключается через ssh remote интерпретатор в pycharm.

Перед стартом docker compose сформировать файл `.env` аналогично `.env.example`
```shell
cd data-detective-etl
cp .env.example .env
randstr=`shuf -zer -n32  {A..Z} {a..z} {0..9} | tr -d '\0'`
echo "AIRFLOW__WEBSERVER__SECRET_KEY=${randstr}" >> .env
```
* Запуск песочницы с docker-compose `docker-compose up -d`
* Запусков тестов: `make tests`
* Запуск линтеров:`make lint`

## Root hierarchy

`Таблица и дерево генерируется скриптом common/root_tree_generator.py`

Здесь представлены служебные объекты иерархии 
С ними связаны реальные объекты метаданных
```
root
├── Documentation
└── Data Detective
	├── Logical Model
	├── Physical Model
	└── DAGs
```