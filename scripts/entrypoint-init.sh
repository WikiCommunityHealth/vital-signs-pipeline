#!/bin/bash
airflow db init
airflow users create \
  --username admin \
  --password admin \
  --firstname Andrea \
  --lastname Denina \
  --role Admin \
  --email andredeninaf@gmail.com
