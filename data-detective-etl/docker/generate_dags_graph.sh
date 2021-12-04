#!/bin/bash

# Parse first command line argument or get all DAGS
if [ -n "$1" ]
then
  DAGS=$1
else
  DAGS=$(ls ${AIRFLOW_HOME}dags/dags)
fi

# Parse second line argument as format (see https://graphviz.org/docs/outputs/)
if [ -n "$2" ]
then
  FORMAT=$2
else
  FORMAT="png"
fi


for DAG in $DAGS
do
  echo "generate graph for $DAG in $FORMAT format"
  airflow dags show --save ${AIRFLOW_HOME}docs/dags/graph/${DAG}_graph.${FORMAT} $DAG
done