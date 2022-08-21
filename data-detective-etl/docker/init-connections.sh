#!/usr/bin/env bash

airflow connections add --conn-type postgres --conn-host pg --conn-port 5432 --conn-login airflow --conn-password airflow pg
airflow connections add --conn-type postgres --conn-host metadb --conn-port 5432 --conn-login airflow --conn-password airflow airflow_meta_db
airflow connections add --conn-type http --conn-host web_service --conn-port 5000 --conn-login airflow --conn-password airflow web_service
airflow connections add --conn-type aws --conn-extra "{\"aws_access_key_id\": \"accessKey1\", \"aws_secret_access_key\":\"verySecretKey1\", \"host\":\"http://s3work:4566\"}" s3work
airflow connections add --conn-type elasticsearch --conn-host dd_search --conn-port 9200 --conn-login elastic --conn-password simplepassword dd_search
