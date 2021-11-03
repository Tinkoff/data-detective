## Running the dev environment using docker-compose

Deploying the development environment
```bash
cd mg-airflow
cp .env.example .env
randstr=`shuf -zer -n32  {A..Z} {a..z} {0..9} | tr -d '\0'`
echo "SECRET_KEY=${randstr}" >> .env
docker-compose up -d
```

It is possible to check mg-airflow with [Celery Executor](https://airflow.apache.org/docs/stable/executor/celery.html)
as follows:
```bash
docker-compose -f docker-compose.CeleryExecutor.yml up -d
```
This services will be launched:
* redis - queue storage
* webserver - portal
* scheduler - scheduler that runs tasks
* flower - view current queues
* worker - a service that runs tasks, scalable, 2 instances

To run autotests, run the command
```bash
docker-compose -f docker-compose.tests.yml up --build --exit-code-from=tests
```

For all Executor, postgres is used as a metadata repository (metadb).
In a production environment, it is advisable to use the Celery Executor option.

### Setup in PyCharm Professional

Configuring SSH Python interpreter in PyCharm
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
7. To run tests go to PyCharm -> Edit Configurations -> Environment variables: 
```
AIRFLOW_HOME=/usr/local/airflow;PYTHON_PATH=/usr/local/bin/python/:/usr/local/airflow:/usr/local/airflow/dags;AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@metadb:5432/airflow
```
1. In order not to specify Environment variables every time, they should be added to Templates -> Python tests -> pytest (в Edit Configurations)
1. Setting up py test. To do this, in the Pycharm settings, select 
Tools->Python Integrated Tools->Testing->Default test runner = pytest. 
After that, it is possible to run tests by right-clicking on the directory `tests` or the desired test file.

### Working in a dev environment

* Airflow UI is available at http://localhost:8080/.
Login/password to log in - airflow /airflow.

* To connect to a dev container with airflow, select this options below in PyCharm: `Tools > Start SSH Session > Remote Python ...` or `docker-compose exec app bash` (app - service name in docker-compose.yml, bash - command for executing)

## Notes

* At the time of publication, Docker Compose in the command line can be called with a command without a hyphen `docker compose ...`.
The documentation will contain examples with `docker-compose ...`.
