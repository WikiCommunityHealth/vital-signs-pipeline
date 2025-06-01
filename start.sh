#!/bin/bash

set -e


mkdir -p ./databases
sudo chown -R 50000:0 ./databases
sudo chmod -R 777 ./databases


mkdir -p ./logs
sudo chown -R 50000:0 ./logs
sudo chmod -R 777 ./logs


docker build -t custom-airflow .


docker compose up --build