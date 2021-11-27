#!/usr/bin/env bash

airflow db init;
${AIRFLOW_HOME}/init-connections.sh ;

RESULT_CODE=0 ;
echo RUN pytest ;
pytest tests -o cache_dir=/dev/null \
       --cov=${AIRFLOW_HOME}/dags \
       --cov=${AIRFLOW_HOME}/common \
       --cov-config=${AIRFLOW_HOME}/.coveragerc \
       || RESULT_CODE=1 ;

# ${AIRFLOW_HOME}/docker/entrypoint.linter.sh || RESULT_CODE=1 ;

echo RESULT_CODE=$RESULT_CODE ;
exit $RESULT_CODE