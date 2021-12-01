# -*- coding: utf-8 -*-
"""Automatic DAGs generator from yaml files from DAGS_FOLDER"""

import logging
import sys

import argcomplete
from airflow import settings
from airflow.cli.cli_parser import get_parser

from data_detective_airflow.constants import DAG_ID_KEY
from data_detective_airflow.dag_generator import dag_generator

dag_id = None
if sys.argv[0].endswith('airflow'):
    parser = get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    dag_id = getattr(args, DAG_ID_KEY, None)

logging.debug(f'Start factory dag loading from {settings.DAGS_FOLDER}')
whitelist = [dag_id] if dag_id else []
for dag in dag_generator(dag_id_whitelist=whitelist):
    if not dag:
        continue
    globals()['dag_' + dag.dag_id] = dag
    logging.debug(f'Successful build for {dag.dag_id} :: {dag.dag_dir}')
