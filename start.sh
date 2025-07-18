#!/bin/bash

docker build -t custom-airflow .

docker compose up --build -d