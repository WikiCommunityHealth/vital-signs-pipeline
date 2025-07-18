#!/bin/bash

docker build -t custom-airflow:v1 .

docker compose up --build -d