#!/usr/bin/env bash

# Here it is necessary to set all connections to test/dev servers
airflow connections add --conn-uri 'postgres://airflow:airflow@pg:5432' pg
airflow connections add --conn-uri 'ftp://airflow:airflow@ssh_service:22' ssh_service
airflow connections add --conn-type s3 --conn-extra "{\"aws_access_key_id\": \"accessKey1\", \"aws_secret_access_key\":\"verySecretKey1\", \"host\":\"http://s3:4566\"}" s3
