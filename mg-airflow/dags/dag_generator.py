# -*- coding: utf-8 -*-
"""Автоматический генератор DAGs из yaml-файлов в DAGS_FOLDER"""

import logging
import sys

import argcomplete
from airflow.cli.cli_parser import get_parser

from mg_airflow.constants import DAG_ID_KEY
from mg_airflow.dag_generator import dag_generator

# Ускорение запуска на worker. Подготавливается только dag для запуска.
# Этот файл - единственная точка входа для DAG-ов.
dag_id = None
if sys.argv[0].endswith('airflow'):
    parser = get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    dag_id = getattr(args, DAG_ID_KEY, None)

whitelist = [dag_id] if dag_id else []
for dag in dag_generator(dag_id_whitelist=whitelist):
    if not dag:
        continue
    globals()['dag_' + dag.dag_id] = dag
    logging.debug(f'Successful build for {dag.dag_id} :: {dag.dag_dir}')
