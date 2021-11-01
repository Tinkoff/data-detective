#!/usr/bin/env bash

function init_airflow {
    export AIRFLOW__CORE__DAGS_FOLDER=/dev/null # не загружаем DAGS пока не создали connections
    airflow db init # Create/upgrade airflow DB
    $AIRFLOW_HOME/init-connections.sh # create connections
    airflow users create -r Admin -u airflow -p airflow -e airflow@apache.ru -f airflow -l airflow
    unset AIRFLOW__CORE__DAGS_FOLDER
}

case "$1" in
  scheduler)
    init_airflow
    airflow scheduler
    ;;
  scheduler-ssh)
    init_airflow
    echo "AIRFLOW__CORE__SQL_ALCHEMY_CONN=$AIRFLOW__CORE__SQL_ALCHEMY_CONN" >> $AIRFLOW_HOME/.ssh/environment
    sudo /usr/sbin/sshd -D &
    airflow scheduler
    ;;
  all-in-one)
    init_airflow
    sudo /usr/sbin/sshd -D &
    airflow scheduler & airflow webserver -p 8080
    ;;
  webserver)
    airflow webserver -p 8080
    ;;
  flower)
    airflow celery flower
    ;;
  worker)
    airflow celery worker
    ;;
  version)
    airflow "$@"
    ;;
  tests)
    init_airflow
    airflow scheduler &
    airflow webserver -p 8080 &

    RESULT_CODE=0 ;

    echo RUN pytest ;
    pytest tests -o cache_dir=/dev/null \
           -p no:cacheprovider \
           --cov=$AIRFLOW_HOME/dags \
           --cov=$AIRFLOW_HOME/mg_airflow \
           || RESULT_CODE=1 ;

    echo RESULT_CODE=$RESULT_CODE ;
    exit $RESULT_CODE
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
