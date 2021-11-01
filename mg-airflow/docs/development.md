## Запуск dev среды через docker-compose

Разворачивание dev среды
```bash
cd mg-airflow
cp .env.example .env
randstr=`shuf -zer -n32  {A..Z} {a..z} {0..9} | tr -d '\0'`
echo "SECRET_KEY=${randstr}" >> .env
docker-compose up -d
```

Проверить mg-airflow c [Celery Executor](https://airflow.apache.org/docs/stable/executor/celery.html)
можно следующий образом.
```bash
docker-compose -f docker-compose.CeleryExecutor.yml up -d
```

Будут запущены:
* redis - хранеие очереди
* webserver - портал
* scheduler - планировщик, запускает таски
* flower - просмотр текущих очередей
* worker - сервис, в котором запускаются таски, масштабируемый, 2 экземпляра 

Для запуска автотестов запустить команду
```bash
docker-compose -f docker-compose.tests.yml up --build --exit-code-from=tests
```

Для всех Executor в качестве хранилища метаданных (metadb) используется postgres.
В продуктовом окружении желательно использовать вариант с Celery Executor.

### Настройка в PyCharm Professional

 Настроить в pycharm ssh интерпретатор python
1. Preferences (CMD + ,) > Project Settings > Project Interpreter
2. Click on the gear icon next to the "Project Interpreter" dropdown > Add
3. Select "SSH Interpreter" > Host: localhost
  
   Port: 9922, 
  
   Username: airflow. 
  
   Password: see `./Dockerfile:15`
   
   Interpreter: /usr/local/bin/python3, 
  
   Sync folders: Project Root -> /usr/local/airflow
 
   Disable "Automatically upload..."
6. Confirm the changes and wait for PyCharm to update the indexes
7. Для запуска тестов из PyCharm необходимо указать в Edit Configurations -> Environment variables: 
```
AIRFLOW_HOME=/usr/local/airflow;PYTHON_PATH=/usr/local/bin/python/:/usr/local/airflow:/usr/local/airflow/dags;AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@metadb:5432/airflow
```
1. Чтобы каждый раз не указывать Environment variables их нужно добавить в Templates -> Python tests -> pytest (в Edit Configurations)
1. Настроить pytest. Для этого в настройках Pycharm выберите 
Tools->Python Integrated Tools->Testing->Default test runner = pytest. 
После этого сможете запускать тесты правой кнопкой мыши кликнув по директории `tests` или нужному тесту.

### Работа в dev среде

* Airflow UI после запуска dev среды доступен по адресу http://localhost:8080/. 
Логин/пароль для входа - airflow/airflow.

* Если нужно подключиться внутрь dev контейнера с airflow, нужно в pycharm выбрать 
`Tools > Start SSH Session > Remote Python ...`
или `docker-compose exec app bash` (app - название сервиса в файле docker-compose.yml, bash - команда для выполнения)

## Заметки

* На момент публикации Docker Compose в командной строке можно вызывать команду без дефиса `docker compose ...`.
В документации будут примеры с дефисом `docker-compose ...`.
