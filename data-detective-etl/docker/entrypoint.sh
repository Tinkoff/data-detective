#!/usr/bin/env bash

function init_airflow {
    export AIRFLOW__CORE__DAGS_FOLDER=/dev/null # Don't load DAGs until connections are created
    airflow db init # Create/upgrade airflow DB
    $AIRFLOW_HOME/init-connections.sh # create connections
    airflow users create -r Admin -u airflow -p airflow -e airflow@tinkoff.ru -f airflow -l airflow
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
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
