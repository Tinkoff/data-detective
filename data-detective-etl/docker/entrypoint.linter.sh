#!/usr/bin/env bash

RESULT_CODE=0 ;
echo RUN pylint ;
pylint \
       ${AIRFLOW_HOME}/dags \
       ${AIRFLOW_HOME}/common \
       ${AIRFLOW_HOME}/tests \
       || RESULT_CODE=1 ;
echo RUN pycodestyle ;
pycodestyle \
       ${AIRFLOW_HOME}/dags \
       ${AIRFLOW_HOME}/tests \
       ${AIRFLOW_HOME}/common \
       || RESULT_CODE=1 ;
echo RUN mypy ;
mypy \
       ${AIRFLOW_HOME}/dags \
       ${AIRFLOW_HOME}/common \
       ${AIRFLOW_HOME}/tests \
       --cache-dir=/dev/null \
       --config-file=${AIRFLOW_HOME}/tox.ini \
       || RESULT_CODE=1 ;
echo LINTERS_RESULT_CODE=$RESULT_CODE ;
exit $RESULT_CODE