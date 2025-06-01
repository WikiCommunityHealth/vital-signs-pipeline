# 🌳 Project Structure
````
.
├── dags
│   └── vital_signs_dag.py
├── dashboards
│   ├── app.py
│   ├── apps
│   │   ├── activity.py
│   │   ├── admin.py
│   │   ├── balance.py
│   │   ├── globall.py
│   │   ├── main_app.py
│   │   ├── retention.py
│   │   ├── special.py
│   │   └── stability.py
│   ├── assets
│   │   ├── logo.png
│   │   └── wikimedia-logo.png
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── monitoring
│   ├── grafana
│   │   ├── dashboards.yaml
│   │   └── vital_signs.json
│   ├── prometheus.yml
│   └── statsd.yaml
├── README.md
├── requirements.txt
├── scripts
│   ├── config.py
│   ├── create_db.py
│   ├── fill_editors_db.py
│   ├── fill_web_db.py
│   ├── primary_language.py
│   └── utils.py
└── vital_signs.png

8 directories, 30 files
````

# Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/WikiCommunityHealth/vital-signs-pipeline
cd vital-signs-pipeline
```
### 2. Build and Start All Services
``` bash
chmod +x start.sh
./start.sh
```
start.sh
```bash
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
```
This will:

Build the custom Airflow image

Start Airflow webserver and scheduler

Start PostgreSQL as Airflow backend

Start statsd exporter to get all the airflow metrics

Start Prometheus to scrape metrics

Start Grafana for dashboard visualization


## 🔍 Services Overview

| Service   | URL   | Notes |
|---------    |-----  |-------|
|   | Backend | |
| Airflow    | http://localhost:8080 |	Username: admin, Password: admin |
| Prometheus     |	http://localhost:9090   | |
| Grafana	| http://localhost:3000 | Username: admin, Password: admin |
| Metrics	| http://localhost:9102/metrics	| |
|   | Frontend | |
| Dashboards | http://localhost:8050 | |
## Airflow DAG
<img alt="DAG" src="./vital_signs.png" />

## 📝 TODO

* testare l'integrazione con le vecchie dashboard
